// CodSpeed benchmark for the JS/WASM bindings — library-only (no rdf-stores /
// oxigraph comparison). This is the JavaScript counterpart of the Rust suite
// (core/benches/benchmark.rs): same "star" (one-factor-at-a-time) design over
// the store's real axes — builder × layout × secondary index — swept across the
// query routing patterns, plus the build, read-back and mutation paths.
//
// Unlike js/bench/compare.bench.ts (wall-clock, comparative, feeds the Pages
// dashboard, NEVER uploaded), THIS file IS uploaded to CodSpeed: it runs under
// CodSpeedHQ/action in instrumentation mode (see .github/workflows/codspeed.yml),
// so every task gets deterministic instruction counts and a flamegraph published
// next to the Rust benchmarks. `withCodSpeed` is a no-op when not run under the
// action, so `npm run bench:codspeed` locally just produces wall-clock numbers.
//
// It supersedes the former core/benches/js_read_path.rs, which could only
// *approximate* this read path by timing its Rust-side stages in isolation;
// here the whole JS→WASM boundary (promise machinery, quad packing, the lazy
// read model) is measured for real, and the flamegraph attributes the cost.
//
// Single process by design: CodSpeed measures each task deterministically, and
// there are no competing libraries to isolate — so none of compare.bench.ts's
// process-per-adapter machinery is needed here.
//
// Run locally (after `npm run build`):
//   npm run bench:codspeed
//   CODSPEED_BENCH_DIM=24 npm run bench:codspeed   # bigger dataset

import { Bench, type BenchOptions } from 'tinybench';
import { withCodSpeed } from '@codspeed/tinybench-plugin';
import { DataFactory } from 'rdf-data-factory';
import type { Quad, Term } from '@rdfjs/types';

import { VortexRdfStore, type BuildOptions } from '../entry/node.js';
// Only ./util.js is importable here — it is the one bench module with no store
// library behind it (see its purity contract); shared.ts loads oxigraph and
// rdf-stores at module scope, which this instrumented process must never do.
import { decodeAll, fmtNs, freeWasm } from './util.js';

const df = new DataFactory();
const EX = 'http://example.org/#';
const nn = (n: number | string) => df.namedNode(EX + n);

// ─── Dataset shape (env-tunable) ─────────────────────────────────────────────
// Small by default: CodSpeed instrumentation runs under Valgrind (~50× slower)
// and counts instructions deterministically, so a representative size catches
// regressions on every path — a larger one only multiplies Valgrind cost.
const DIM = Number(process.env.CODSPEED_BENCH_DIM ?? 16); // triples: DIM³ rows
const DIM_QUADS = Number(process.env.CODSPEED_BENCH_DIM_QUADS ?? 8); // quads: DIM_QUADS⁴ rows
const MUT_N = Number(process.env.CODSPEED_MUT_N ?? 500); // add/delete batch size

/** DIM³ triples in the default graph: quad(ex#s, ex#p, ex#o). */
function genTriples(d: number): Quad[] {
    const out: Quad[] = [];
    for (let s = 0; s < d; s++)
        for (let p = 0; p < d; p++)
            for (let o = 0; o < d; o++) out.push(df.quad(nn(s), nn(p), nn(o)));
    return out;
}

/** DIM_QUADS⁴ quads across named graphs: quad(ex#s, ex#p, ex#o, ex#g). */
function genQuads(d: number): Quad[] {
    const out: Quad[] = [];
    for (let s = 0; s < d; s++)
        for (let p = 0; p < d; p++)
            for (let o = 0; o < d; o++)
                for (let g = 0; g < d; g++) out.push(df.quad(nn(s), nn(p), nn(o), nn(g)));
    return out;
}

/** DIM³ triples whose objects are literals — plain, language-tagged, typed,
 * and escape-bearing (quotes, backslashes, newlines, and `"@`/`^^` lookalikes
 * inside the value). Every other dataset in this file is named-node-only, so
 * the tasks over this one are the only coverage of literal escaping on ingest
 * and the unescape path on decode/export. */
function genLiteralTriples(d: number): Quad[] {
    const dt = df.namedNode('http://www.w3.org/2001/XMLSchema#integer');
    const out: Quad[] = [];
    let i = 0;
    for (let s = 0; s < d; s++)
        for (let p = 0; p < d; p++)
            for (let o = 0; o < d; o++) {
                const object =
                    i % 4 === 0 ? df.literal(`plain value ${o}`)
                    : i % 4 === 1 ? df.literal(`tagged value ${o}`, 'en')
                    : i % 4 === 2 ? df.literal(String(o), dt)
                    : df.literal(`say "hi"@home ^^ line\nbreak back\\slash ${o}`);
                out.push(df.quad(nn(s), nn(p), object));
                i++;
            }
    return out;
}

/** A batch of fresh quads (disjoint namespace) for the add phase. */
function genFresh(n: number): Quad[] {
    const out: Quad[] = [];
    for (let i = 0; i < n; i++) out.push(df.quad(nn('add-s' + i), nn('add-p'), nn('add-o' + i)));
    return out;
}

/** The first `n` triples genTriples(DIM) would emit — the delete phase only
 * needs a batch-sized slice of quads that actually exist in the store. */
function genTriplesPrefix(d: number, n: number): Quad[] {
    const out: Quad[] = [];
    for (let s = 0; s < d && out.length < n; s++)
        for (let p = 0; p < d && out.length < n; p++)
            for (let o = 0; o < d && out.length < n; o++) out.push(df.quad(nn(s), nn(p), nn(o)));
    return out;
}

// ─── Query patterns (probe terms fixed at index 0, so they always hit rows) ──
type Pat = { name: string; s: Term | null; p: Term | null; o: Term | null; g: Term | null };
const t0 = nn(0);
const g0 = nn(0);
// The six routing classes the resolver branches on, split by which dataset can
// exercise them: S/P/O/PO/SPO on the triples store, G/SPOG on the quads store.
const TRIPLE_PATTERNS: Pat[] = [
    { name: 'S', s: t0, p: null, o: null, g: null },
    { name: 'P', s: null, p: t0, o: null, g: null },
    { name: 'O', s: null, p: null, o: t0, g: null },
    { name: 'PO', s: null, p: t0, o: t0, g: null },
    { name: 'SPO', s: t0, p: t0, o: t0, g: null },
];
const FULL_SCAN_PATTERN: Pat = { name: 'full', s: null, p: null, o: null, g: null };
const QUAD_PATTERNS: Pat[] = [
    { name: 'G', s: null, p: null, o: null, g: g0 },
    { name: 'SPOG', s: t0, p: t0, o: t0, g: g0 },
];

// ─── Store variants (mirror the Rust star-design axes) ───────────────────────
type Variant = { slug: string; options: BuildOptions };

// Build (write) path: sweep builder × layout × index one factor at a time
// around an Unsorted/Dictionary baseline (the JS default), matching the Rust
// serialize group's axes.
const BUILD_VARIANTS: Variant[] = [
    { slug: 'unsorted_dict', options: { builder: 'Unsorted', layout: 'Dictionary' } }, // baseline / JS default
    { slug: 'unsorted_default', options: { builder: 'Unsorted', layout: 'Default' } },
    { slug: 'sorted_dict', options: { builder: 'Sorted', layout: 'Dictionary' } },
    { slug: 'sorted_default', options: { builder: 'Sorted', layout: 'Default' } },
    { slug: 'sorted_typedobject', options: { builder: 'Sorted', layout: 'TypedObject' } },
    { slug: 'sorted_dict_byref', options: { builder: 'Sorted', layout: 'Dictionary', indexes: ['SecondaryByReference'] } },
    { slug: 'sorted_dict_bycopy', options: { builder: 'Sorted', layout: 'Dictionary', indexes: ['SecondaryByCopy'] } },
];

// Query (read) path: two representative configs — the JS default (no sorted
// access path, the optimization target js_read_path.rs was chasing) and the
// fully-indexed fast path (where a bound predicate/object binary-searches).
const QUERY_VARIANTS: Variant[] = [
    { slug: 'unsorted_dict', options: { builder: 'Unsorted', layout: 'Dictionary' } },
    { slug: 'sorted_dict_bycopy', options: { builder: 'Sorted', layout: 'Dictionary', indexes: ['SecondaryByCopy'] } },
];

async function drain(stream: unknown): Promise<number> {
    let n = 0;
    for await (const _ of stream as AsyncIterable<Quad>) n++;
    return n;
}

// tinybench options shape only the LOCAL wall-clock run. Under CodSpeed
// instrumentation the plugin ignores them: each task runs seven untimed warmup
// invocations (core.optimizeFunction), then global.gc(), then exactly ONE
// measured invocation. That single-shot model is why the runner passes
// --no-liftoff (codspeed.yml, package.json): with background wasm tier-up
// enabled, the measured invocation nondeterministically executed Liftoff or
// TurboFan code — a ~2x instruction-count swing on the build tasks at
// identical code. Reads get a time budget; the costly build/mutation phases
// get warmup plus a fixed iteration count so local numbers are stable too.
const READ_OPTS: BenchOptions = { time: 200, iterations: 10, warmup: true, warmupIterations: 3, throws: true };
const HEAVY_OPTS: BenchOptions = { time: 0, iterations: 7, warmup: true, warmupIterations: 2, throws: true };

async function runGroup(opts: BenchOptions, add: (b: Bench) => void): Promise<void> {
    const bench = withCodSpeed(new Bench(opts));
    add(bench);
    await bench.run();
    // Local wall-clock runs: print a compact mean per task. Under the CodSpeed
    // runner tinybench's result shape differs (the plugin logs its own
    // `[CodSpeed] Measured` lines), so only print when a latency is present.
    for (const task of bench.tasks) {
        const r = task.result;
        if (!r || !('latency' in r) || !r.latency) continue;
        console.log(`  ${task.name.padEnd(34)} ${fmtNs(r.latency.mean * 1e6)}`);
    }
}

// ─── Groups ──────────────────────────────────────────────────────────────────

/** build::<config> — VortexRdfStore.fromQuads over each star variant, plus a
 * fromString (parse + build) task to cover the RDF-text entry point and a
 * literal-bearing build covering term escaping on ingest. */
async function benchBuild(triples: Quad[], literals: Quad[]): Promise<void> {
    // All generated terms are named nodes, so N-Triples serialization is trivial.
    const nquads = triples
        .map((q) => `<${q.subject.value}> <${q.predicate.value}> <${q.object.value}> .`)
        .join('\n');
    await runGroup(HEAVY_OPTS, (b) => {
        for (const v of BUILD_VARIANTS) {
            let h: VortexRdfStore | undefined;
            b.add(`build::${v.slug}`, async () => { h = await VortexRdfStore.fromQuads(triples, v.options); }, {
                afterEach: () => { if (h) freeWasm(h); h = undefined; },
            });
        }
        let hs: VortexRdfStore | undefined;
        b.add('build::fromString_nquads', async () => {
            hs = await VortexRdfStore.fromString(nquads, 'nquads', { builder: 'Sorted', layout: 'Dictionary' });
        }, { afterEach: () => { if (hs) freeWasm(hs); hs = undefined; } });
        let hl: VortexRdfStore | undefined;
        b.add('build::sorted_dict_literals', async () => {
            hl = await VortexRdfStore.fromQuads(literals, { builder: 'Sorted', layout: 'Dictionary' });
        }, { afterEach: () => { if (hl) freeWasm(hl); hl = undefined; } });
    });
}

/** Literal-bearing read paths on a Sorted/Dictionary store over
 * genLiteralTriples' dataset: toRdf re-escapes every literal on export, and
 * the decoded read parses every literal's serialized form on the JS side. */
async function benchLiterals(literals: Quad[]): Promise<void> {
    const store = await VortexRdfStore.fromQuads(literals, { builder: 'Sorted', layout: 'Dictionary' });
    await runGroup(READ_OPTS, (b) => {
        b.add('readback::toRdf_nquads_literals', async () => { await store.toRdf('nquads'); });
        b.add('readpath::full_decoded_literals', async () => {
            decodeAll(await store.getQuads(null, null, null, null));
        });
    });
    freeWasm(store);
}

/** query_<config>::<pattern> — getQuads across every routing class, on each
 * query variant. Store built once per variant (untimed). */
async function benchQuery(triples: Quad[], quads: Quad[]): Promise<void> {
    for (const v of QUERY_VARIANTS) {
        const th = await VortexRdfStore.fromQuads(triples, v.options);
        await runGroup(READ_OPTS, (b) => {
            for (const p of TRIPLE_PATTERNS)
                b.add(`query_${v.slug}::${p.name}`, async () => { await th.getQuads(p.s, p.p, p.o, p.g); });
            b.add(`query_${v.slug}::full`, async () => {
                await th.getQuads(FULL_SCAN_PATTERN.s, FULL_SCAN_PATTERN.p, FULL_SCAN_PATTERN.o, FULL_SCAN_PATTERN.g);
            });
        });
        freeWasm(th);

        const qh = await VortexRdfStore.fromQuads(quads, v.options);
        await runGroup(READ_OPTS, (b) => {
            for (const p of QUAD_PATTERNS)
                b.add(`query_${v.slug}::${p.name}`, async () => { await qh.getQuads(p.s, p.p, p.o, p.g); });
        });
        freeWasm(qh);
    }
}

/** readpath::<variant> — the read entry points on the JS-default store for one
 * selective pattern (S), isolating the boundary cost each carries: getQuads
 * (materialized array), match (lazy stream drain), matchCodes (zero-copy u32
 * columns, no term strings). Directly supports read-path tuning.
 *
 * The `_decoded` variants additionally read every term's `.value`. They are the
 * only benchmarks in this file that exercise term decoding at all — the others
 * stop at the lazy quads — so they are what guards the term dictionary's
 * per-lookup cost against regressions. `full_decoded` is the worst case: every
 * row, every term, i.e. every distinct code resolved once. */
async function benchReadPath(triples: Quad[]): Promise<void> {
    const store = await VortexRdfStore.fromQuads(triples, { builder: 'Unsorted', layout: 'Dictionary' });
    const p = TRIPLE_PATTERNS[0]; // S
    const f = FULL_SCAN_PATTERN;
    await runGroup(READ_OPTS, (b) => {
        b.add('readpath::getQuads', async () => { await store.getQuads(p.s, p.p, p.o, p.g); });
        b.add('readpath::match_stream', async () => { await drain(store.match(p.s, p.p, p.o, p.g)); });
        b.add('readpath::matchCodes', async () => { await store.matchCodes(p.s, p.p, p.o, p.g); });
        b.add('readpath::getQuads_decoded', async () => {
            decodeAll(await store.getQuads(p.s, p.p, p.o, p.g));
        });
        b.add('readpath::full_decoded', async () => {
            decodeAll(await store.getQuads(f.s, f.p, f.o, f.g));
        });
    });
    freeWasm(store);
}

/** readback::<op> — serialize/deserialize the store across the boundary:
 * toBytes (IPC out), fromBytes (IPC in), toRdf (N-Quads out). */
async function benchReadback(triples: Quad[]): Promise<void> {
    const store = await VortexRdfStore.fromQuads(triples, { builder: 'Sorted', layout: 'Dictionary' });
    const bytes = await store.toBytes();
    await runGroup(READ_OPTS, (b) => {
        b.add('readback::toBytes', async () => { await store.toBytes(); });
        b.add('readback::toRdf_nquads', async () => { await store.toRdf('nquads'); });
        let h: VortexRdfStore | undefined;
        b.add('readback::fromBytes', async () => { h = await VortexRdfStore.fromBytes(bytes); }, {
            afterEach: () => { if (h) freeWasm(h); h = undefined; },
        });
    });
    freeWasm(store);
}

/** mutate::<op> — the mutation paths on the JS-default store: per-quad addQuad
 * loop, batched addQuads, per-quad deleteQuad loop. */
async function benchMutate(): Promise<void> {
    const fresh = genFresh(MUT_N);
    const delSlice = genTriplesPrefix(DIM, MUT_N);
    const opts: BuildOptions = { layout: 'Dictionary' };

    await runGroup(HEAVY_OPTS, (b) => {
        let h: VortexRdfStore | undefined;
        b.add('mutate::addQuad_loop', async () => {
            h = VortexRdfStore.empty();
            for (const q of fresh) await h.addQuad(q);
        }, { afterEach: () => { if (h) freeWasm(h); h = undefined; } });

        let hb: VortexRdfStore | undefined;
        b.add('mutate::addQuads_batch', async () => {
            hb = VortexRdfStore.empty();
            await hb.addQuads(fresh);
        }, { afterEach: () => { if (hb) freeWasm(hb); hb = undefined; } });

        let hd: VortexRdfStore | undefined;
        b.add('mutate::deleteQuad_loop', async () => {
            for (const q of delSlice) await hd!.deleteQuad(q);
        }, {
            beforeEach: async () => { hd = await VortexRdfStore.fromQuads(delSlice, opts); },
            afterEach: () => { if (hd) freeWasm(hd); hd = undefined; },
        });
    });
}

async function main(): Promise<void> {
    console.log(
        `CodSpeed JS bench · triples DIM=${DIM} (${(DIM ** 3).toLocaleString()} rows), ` +
        `quads DIM_QUADS=${DIM_QUADS} (${(DIM_QUADS ** 4).toLocaleString()} rows), MUT_N=${MUT_N}`,
    );
    const triples = genTriples(DIM);
    const quads = genQuads(DIM_QUADS);
    const literals = genLiteralTriples(DIM);

    await benchBuild(triples, literals);
    await benchQuery(triples, quads);
    await benchReadPath(triples);
    await benchReadback(triples);
    await benchLiterals(literals);
    await benchMutate();
}

main().catch((e) => { console.error(e); process.exit(1); });
