// Runs ONE adapter's full lifecycle in its own process, spawned by compare.bench.ts.
// Process-level isolation, not just in-process dispose+gc: a crash in one adapter
// can't take down the whole run, and each adapter gets a clean, uncontaminated
// peak-memory reading (see shared.ts's `peakRssMb`) instead of one polluted by
// whatever every earlier adapter left resident.
import { Bench } from 'tinybench';
import { withCodSpeed } from '@codspeed/tinybench-plugin';
import { writeFileSync } from 'node:fs';

import {
    ADAPTERS, MUT_ADAPTERS, N_TRIPLES, N_QUADS, MUT_BATCH,
    genDataset, genFresh, genDatasetPrefix, datasetProbes, moduli,
    FULL_SCAN_PATTERN, QUERY_OPTS, COLD_QUERY_OPTS, HEAVY_OPTS, FULL_SCAN_OPTS,
    reclaim, collect, peakRssMb, rssMb, jsHeapMb, wasmHeapMb,
    type Row, type Pat, type StoreAdapter,
} from './shared.js';

const [, , slug, role, outFile] = process.argv;
if (!slug || !role || !outFile) {
    console.error('usage: compare.worker.ts <slug> <query|querycold|fullscan|mutate> <out-file>');
    process.exit(1);
}

async function runBench(
    rows: Row[], opts: typeof QUERY_OPTS, add: (b: Bench) => void,
    regime?: 'cold' | 'warm',
): Promise<void> {
    const bench = withCodSpeed(new Bench(opts));
    add(bench);
    await bench.run();
    collect(bench, rows, regime);
}

/** Phases that could not be measured, and why. */
const failures: { phase: string; error: string }[] = [];

/**
 * Run one benchmark group, keeping a failure local to it.
 *
 * A store that cannot survive one phase can still be measured on every other,
 * and that partial result is worth far more than nothing: without this, a single
 * trap loses the adapter's entire query worker — which is how oxigraph
 * disappeared from the comparison altogether. The failure is recorded rather
 * than swallowed, so a missing row is visibly attributed instead of looking like
 * a benchmark that was never run.
 */
async function bench(
    phase: string, rows: Row[], opts: typeof QUERY_OPTS, add: (b: Bench) => void,
    regime?: 'cold' | 'warm',
): Promise<void> {
    try {
        await runBench(rows, opts, add, regime);
    } catch (e) {
        const error = e instanceof Error ? e.message : String(e);
        console.error(`  !! phase '${phase}' failed: ${error}`);
        failures.push({ phase, error });
    }
}

/** Rows each probe actually matches. Recorded alongside the timings because
 *  selectivity is now a property of the dataset's term cardinality rather than a
 *  fixed D², so a timing is only interpretable next to its match count. */
const matched: Record<string, number> = {};

/** What building the store cost, over and above the input `Quad[]`. */
const storeFootprint: Record<string, number | null> = {};

/** `wasm` is Vortex's linear memory specifically — oxigraph has its own module
 *  this cannot see, and rdf-stores has none — so it is only read for Vortex
 *  adapters and reported as null elsewhere rather than as a misleading 0. */
async function memSnapshot(isVortex: boolean): Promise<{ rss: number | null; js: number; wasm: number | null }> {
    // jsHeapMb forces a collection first, so the RSS read after it is post-GC too.
    const js = jsHeapMb();
    return { rss: rssMb(), js, wasm: isVortex ? await wasmHeapMb() : null };
}

const delta = (a: number | null, b: number | null) => (a === null || b === null ? null : a - b);

async function countAll(a: StoreAdapter, h: unknown, pats: Pat[]): Promise<void> {
    for (const p of pats) matched[p.name] = await a.countMatch(h, p);
}

async function runQuery(a: StoreAdapter): Promise<Row[]> {
    const rows: Row[] = [];
    const tProbes = datasetProbes(N_TRIPLES);
    const qProbes = datasetProbes(N_QUADS, { graphs: 8 });

    console.log(`[${a.label}] query (triples)…`);
    const triples = genDataset(N_TRIPLES);
    // Measure across the build only. The input array is ~a gigabyte of JS objects
    // and is identical for every adapter, so a delta isolates the store's own
    // footprint far better than any absolute reading — and unlike dropping the
    // array it does not force us to regenerate it for the ingest phase.
    const isVortex = a.slug.startsWith('vortex');
    const before = await memSnapshot(isVortex);
    let th: unknown = await a.build(triples);
    const after = await memSnapshot(isVortex);
    Object.assign(storeFootprint, {
        rssMb: delta(after.rss, before.rss),
        jsHeapMb: after.js - before.js,
        wasmHeapMb: delta(after.wasm, before.wasm),
    });

    await countAll(a, th, tProbes.triples);
    await bench('query_triples', rows, QUERY_OPTS, (b) => {
        for (const p of tProbes.triples) b.add(`${a.slug}::${p.name}`, async () => { await a.countMatch(th, p); });
    }, 'warm');
    reclaim(a, th);
    th = null;

    console.log(`[${a.label}] query (quads)…`);
    const quads = genDataset(N_QUADS, { graphs: 8 });
    let qh: unknown = await a.build(quads);
    await countAll(a, qh, qProbes.quads);
    await bench('query_quads', rows, QUERY_OPTS, (b) => {
        for (const p of qProbes.quads) b.add(`${a.slug}::${p.name}`, async () => { await a.countMatch(qh, p); });
    }, 'warm');
    reclaim(a, qh);
    qh = null;

    console.log(`[${a.label}] ingest…`);
    await bench('ingest', rows, HEAVY_OPTS, (b) => {
        // Dispose in `afterEach` (untimed) rather than inside the timed function, so
        // freeing the previous store never pollutes the measured ingest cost.
        let h: unknown;
        b.add(`ingest::${a.slug}`, async () => { h = await a.build(triples); }, {
            afterEach: () => { reclaim(a, h); h = undefined; },
        });
    });

    return rows;
}

/**
 * The full scan, in its own process — the same isolation argument this file
 * already makes for adapters, one level down.
 *
 * Materializing every row of a multi-million-quad dataset with realistic term
 * cardinality is by far the heaviest phase, and a store's own retention makes it
 * order-dependent: oxigraph holds ~860 MB per full scan and never reclaims it
 * within a store's lifetime, while wasm linear memory never shrinks back to the
 * engine. Sharing a process with the query and ingest phases therefore left its
 * module too grown to scan at all — it trapped with `unreachable` even on a
 * freshly built store, which would have reported "oxigraph cannot full-scan this
 * dataset" when in fact a clean process does it comfortably (3 scans, peaking
 * ~5.9 GB). Measuring it here instead reports the scan rather than the residue
 * of everything that ran before it.
 */
/**
 * The cold query regime, plus the open it deliberately excludes — in a process
 * of its own.
 *
 * Each iteration answers the FIRST query on a freshly adopted store. The adopt
 * happens in tinybench's `beforeEach`, which is untimed, so the column isolates
 * what a query costs against empty caches rather than reporting that plus the
 * cost of building a store. Opening is measured separately as `open::<slug>`,
 * mirroring the Python tab, so the two are attributable on their own.
 *
 * The process isolation is not stylistic. The first query on an adopted store
 * retains its whole buffer past `free()` (measured at D=128: +32 MB per
 * iteration, and exactly flat when the query is skipped), and wasm linear
 * memory never returns to the OS. Sharing a process with the warm phase piled a
 * live 2M-quad store plus every adoption into one address space until the wasm
 * allocator trapped ("unreachable"). Here the built store is freed the moment
 * its snapshot exists, so the whole budget belongs to the measurements.
 */
async function runQueryCold(a: StoreAdapter): Promise<Row[]> {
    const rows: Row[] = [];
    if (!a.snapshot || !a.open) return rows; // no persistent form, no cold rows

    // One dataset's cold pass: adopt per iteration (untimed), query (timed).
    const coldPass = async (
        phase: string, snap: unknown, probes: Pat[],
    ): Promise<void> => {
        await bench(phase, rows, COLD_QUERY_OPTS, (b) => {
            for (const p of probes) {
                let h: unknown;
                b.add(`${a.slug}::${p.name}`, async () => { await a.countMatch(h, p); }, {
                    beforeEach: async () => { h = await a.open!(snap); },
                    afterEach: () => { a.dispose?.(h); h = undefined; },
                });
            }
        }, 'cold');
    };

    console.log(`[${a.label}] query cold (triples)…`);
    const triples = genDataset(N_TRIPLES);
    const tProbes = datasetProbes(N_TRIPLES);
    let th: unknown = await a.build(triples);
    const tSnap = await a.snapshot(th);
    reclaim(a, th);
    th = null;

    // Open, on its own: adopting the snapshot into a queryable store, with no
    // query behind it. The Python tab has measured this as `open::<slug>` all
    // along; without it the cold column's context — how much of a cold start is
    // the open — is not on the page.
    await bench('open', rows, HEAVY_OPTS, (b) => {
        let h: unknown;
        b.add(`open::${a.slug}`, async () => { h = await a.open!(tSnap); }, {
            afterEach: () => { a.dispose?.(h); h = undefined; },
        });
    });

    await coldPass('query_triples_cold', tSnap, tProbes.triples);

    console.log(`[${a.label}] query cold (quads)…`);
    const quads = genDataset(N_QUADS, { graphs: 8 });
    const qProbes = datasetProbes(N_QUADS, { graphs: 8 });
    let qh: unknown = await a.build(quads);
    const qSnap = await a.snapshot(qh);
    reclaim(a, qh);
    qh = null;
    await coldPass('query_quads_cold', qSnap, qProbes.quads);

    return rows;
}

async function runFullScan(a: StoreAdapter): Promise<Row[]> {
    const rows: Row[] = [];
    console.log(`[${a.label}] full scan…`);
    const triples = genDataset(N_TRIPLES);
    let h: unknown = await a.build(triples);
    await bench('full', rows, FULL_SCAN_OPTS, (b) => {
        b.add(`${a.slug}::full`, async () => { await a.countMatch(h, FULL_SCAN_PATTERN); });
    }, 'warm');
    if (a.snapshot && a.open) {
        const snap = await a.snapshot(h);
        await bench('full_cold', rows, FULL_SCAN_OPTS, (b) => {
            let fresh: unknown;
            b.add(`${a.slug}::full`, async () => { await a.countMatch(fresh, FULL_SCAN_PATTERN); }, {
                beforeEach: async () => { fresh = await a.open!(snap); },
                afterEach: () => { a.dispose?.(fresh); fresh = undefined; },
            });
        }, 'cold');
    }
    reclaim(a, h);
    h = null;
    return rows;
}

async function runMutate(a: StoreAdapter): Promise<Row[]> {
    const rows: Row[] = [];
    const fresh = genFresh(MUT_BATCH);
    const delSlice = genDatasetPrefix(N_TRIPLES, MUT_BATCH);

    console.log(`\n[${a.label}] add (${MUT_BATCH})…`);
    await bench('add', rows, HEAVY_OPTS, (b) => {
        let h: unknown;
        b.add(`add::${a.slug}`, async () => {
            h = await a.newEmpty();
            await a.addAll(h, fresh);
        }, { afterEach: () => { reclaim(a, h); h = undefined; } });
        if (a.addBatch) {
            let hb: unknown;
            b.add(`add_batch::${a.slug}`, async () => {
                hb = await a.newEmpty();
                await a.addBatch!(hb, fresh);
            }, { afterEach: () => { reclaim(a, hb); hb = undefined; } });
        }
    });

    console.log(`[${a.label}] delete (${MUT_BATCH})…`);
    let dh: unknown = null;
    await bench('delete', rows, HEAVY_OPTS, (b) => {
        b.add(`delete::${a.slug}`, async () => { await a.deleteAll(dh, delSlice); }, {
            beforeEach: async () => { dh = await a.build(delSlice); },
            afterEach: () => { reclaim(a, dh); dh = null; },
        });
    });

    return rows;
}

async function main(): Promise<void> {
    const list = role === 'mutate' ? MUT_ADAPTERS : ADAPTERS;
    const a = list.find((x) => x.slug === slug);
    if (!a) {
        console.error(`unknown adapter slug '${slug}' for role '${role}'`);
        process.exit(1);
    }

    const rows = role === 'query' ? await runQuery(a)
        : role === 'querycold' ? await runQueryCold(a)
            : role === 'fullscan' ? await runFullScan(a)
                : await runMutate(a);
    writeFileSync(outFile, JSON.stringify({
        rows,
        peakRssMb: peakRssMb(),
        storeFootprint,
        matched,
        failures,
        cardinality: role === 'query'
            ? { triples: moduli(N_TRIPLES), quads: moduli(N_QUADS, { graphs: 8 }) }
            : undefined,
    }));
}

main().catch((e) => {
    console.error(e);
    process.exit(1);
});
