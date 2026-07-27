// Attribution sweep: how much of Vortex's wasm memory is the term dictionary?
//
// Run (after `npm run build`):
//   npx tsx bench/dict-memory.bench.ts
//   DICT_MEM_N=200000 npx tsx bench/dict-memory.bench.ts     # smaller/faster
//
// This is NOT a timing benchmark and is never uploaded anywhere. It exists to
// answer one question with data rather than arithmetic: at a given row count,
// how does wasm memory scale with the number of distinct *terms*, and how much
// of that is the resident dictionary versus the transients around it?
//
// Method — two nested differentials, because nothing can be read directly.
//
// Inner (per config, in the worker): several stores are held live at once and
// memory is read after each, so the slope against store count gives the RETAINED
// cost of one store. A single store tells you nothing — the ingest transient
// sets the high-water mark and the store is allocated inside the space it
// freed. See the worker's header for the measurement that established this.
//
// Outer (here): hold rows fixed, sweep term cardinality, and fit
//
//   d(retained per store)/d(terms)  the dictionary's per-term cost
//   intercept as terms -> 0         quad columns + per-store overhead. For the
//                                   Dictionary layout this should land near
//                                   16 bytes/row (four u32 columns) — that
//                                   agreement is what makes the slope credible.
//
// Alongside: what the first Dictionary read costs (it used to copy the whole
// dictionary twice), what a full scan with every term materialized costs (the
// on-demand dictionary's worst case, one boundary crossing per distinct term),
// and whether repeated mutate-then-query grows the high-water mark.
//
// Every config runs in its own process because wasm memory never shrinks; see
// dict-memory.worker.ts.

import { spawnSync } from 'node:child_process';
import { tmpdir } from 'node:os';
import { writeFileSync, readFileSync, rmSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve, join } from 'node:path';

const here = dirname(fileURLToPath(import.meta.url));
const workerPath = resolve(here, 'dict-memory.worker.ts');
const tsxBin = resolve(here, '..', 'node_modules', '.bin', 'tsx');
const OUT = resolve(here, process.env.DICT_MEM_OUT ?? 'dict-memory.json');

/** Rows held fixed across the sweep — only term cardinality varies. */
const N = Number(process.env.DICT_MEM_N ?? 500_000);

/** Which build variants to sweep. Dictionary layout is the subject; Default is
 *  the control — it has no term dictionary, so its slope isolates everything
 *  that is *not* the dictionary. */
const SLUGS = (process.env.DICT_MEM_SLUGS ?? 'vortex_sorted_dict,vortex_sorted_default').split(',');

/** Subject/object ratios per point. Object ratio tracks subject ratio at half,
 *  so `terms` sweeps roughly two orders of magnitude. */
const RATIOS = (process.env.DICT_MEM_RATIOS ?? '0.001,0.01,0.1,0.5,1.0')
    .split(',')
    .map(Number);

interface Point {
    slug: string; n: number; terms: number; stores: number;
    wasmPerStore: (number | null)[];
    wasmAfterInit: number | null; wasmAfterGen: number | null;
    wasmAfterFirstQuery: number | null; wasmAfterFullScan: number | null;
    wasmAfterRebuild: number | null; wasmAfterFree: number | null;
    jsAfterInit: number; jsAfterBuild: number; jsAfterFirstQuery: number;
    jsAfterFullScan: number; jsAfterRebuild: number;
    rssAfterBuild: number | null; peakRssMb: number | null;
    scanMs: number; decodeMs: number; mutateQueryMs: number;
    fullRows: number; firstQueryRows: number; decodedChars: number;
    [k: string]: unknown;
}

/** Retained MB per store: the slope of linear memory against live store count. */
function retainedPerStore(p: Point): number | null {
    const ys = p.wasmPerStore.filter((v): v is number => v !== null);
    if (ys.length < 2) return null;
    const xs = ys.map((_, i) => i + 1);
    return fit(xs, ys).slope;
}

function runPoint(slug: string, subjRatio: number, objRatio: number): Point | null {
    const outFile = join(tmpdir(), `vortex-dictmem-${slug}-${subjRatio}-${process.pid}.json`);
    const res = spawnSync(
        tsxBin,
        ['--expose-gc', '--max-old-space-size=8192', workerPath,
            slug, String(N), String(subjRatio), String(objRatio), outFile],
        { stdio: 'inherit', env: process.env },
    );
    try {
        if (res.status !== 0) {
            console.error(`\n[${slug} @ ratio ${subjRatio}] worker exited ${res.status} — skipping.`);
            return null;
        }
        return JSON.parse(readFileSync(outFile, 'utf8')) as Point;
    } catch (e) {
        console.error(`[${slug} @ ratio ${subjRatio}] failed to read output:`, e);
        return null;
    } finally {
        rmSync(outFile, { force: true });
    }
}

/** Least-squares fit of `y = slope*x + intercept`. */
function fit(xs: number[], ys: number[]): { slope: number; intercept: number } {
    const k = xs.length;
    const mx = xs.reduce((a, b) => a + b, 0) / k;
    const my = ys.reduce((a, b) => a + b, 0) / k;
    let num = 0, den = 0;
    for (let i = 0; i < k; i++) {
        num += (xs[i] - mx) * (ys[i] - my);
        den += (xs[i] - mx) ** 2;
    }
    const slope = den === 0 ? 0 : num / den;
    return { slope, intercept: my - slope * mx };
}

function main(): void {
    console.log(`Dictionary memory sweep: n=${N.toLocaleString()} rows, ratios ${RATIOS.join(', ')}\n`);
    const points: Point[] = [];

    for (const slug of SLUGS) {
        for (const r of RATIOS) {
            console.log(`\n── ${slug} @ subjectRatio=${r} ─────────────────────────`);
            const p = runPoint(slug, r, r / 2);
            if (!p) continue;
            points.push(p);
            const per = retainedPerStore(p);
            console.log(
                `   terms=${p.terms.toLocaleString()}  retained/store=${per === null ? '?' : per.toFixed(1)} MB  ` +
                `wasm: [${p.wasmPerStore.join(',')}] query=${p.wasmAfterFirstQuery} ` +
                `scan=${p.wasmAfterFullScan} rebuild=${p.wasmAfterRebuild} MB  ` +
                `scan=${p.scanMs.toFixed(0)}ms decode=${p.decodeMs.toFixed(0)}ms ` +
                `mutQuery=${p.mutateQueryMs.toFixed(0)}ms`,
            );
        }
    }

    const analysis: Record<string, unknown> = {};
    for (const slug of SLUGS) {
        const ps = points.filter((p) => p.slug === slug && retainedPerStore(p) !== null);
        if (ps.length < 2) continue;
        const f = fit(ps.map((p) => p.terms), ps.map((p) => retainedPerStore(p) as number));
        analysis[slug] = {
            bytesPerTerm: Math.round(f.slope * 1048576),
            interceptMb: Number(f.intercept.toFixed(1)),
            // The control: four u32 columns are 16 bytes/row regardless of terms.
            expectedQuadColumnsMb: Number(((N * 16) / 1048576).toFixed(1)),
            retainedPerStoreMb: ps.map((p) => ({
                terms: p.terms, mb: Number((retainedPerStore(p) as number).toFixed(1)),
            })),
            firstQueryDeltaMb: ps.map((p) => ({
                terms: p.terms,
                wasm: (p.wasmAfterFirstQuery ?? 0) - (p.wasmPerStore.at(-1) ?? 0)!,
                js: p.jsAfterFirstQuery - p.jsAfterBuild,
            })),
            rebuildGrowthMb: ps.map((p) => ({
                terms: p.terms,
                growth: (p.wasmAfterRebuild ?? 0) - (p.wasmAfterFullScan ?? 0),
                mutateQueryMs: Math.round(p.mutateQueryMs),
            })),
            fullScan: ps.map((p) => ({
                terms: p.terms, rows: p.fullRows,
                scanMs: Math.round(p.scanMs), decodeMs: Math.round(p.decodeMs),
                jsHeapDeltaMb: p.jsAfterFullScan - p.jsAfterFirstQuery,
            })),
        };
    }

    writeFileSync(OUT, JSON.stringify({ n: N, ratios: RATIOS, points, analysis }, null, 2));

    console.log('\n─── analysis ───────────────────────────────────────────────');
    for (const [slug, a] of Object.entries(analysis)) {
        const v = a as Record<string, unknown>;
        console.log(`${slug}:`);
        console.log(`  ${v.bytesPerTerm} bytes/term`);
        console.log(`  intercept ${v.interceptMb} MB (quad columns alone would be ${v.expectedQuadColumnsMb} MB)`);
    }
    console.log(`\nWrote ${points.length} points → ${OUT}`);
}

main();
