// Shared infrastructure for every multi-process bench instrument here: the
// comparative orchestrator (compare.bench.ts), its per-adapter worker
// (compare.worker.ts), and the dictionary-memory worker (dict-memory.worker.ts,
// which takes the dataset generator, the Vortex adapters and the memory probes).
//
// PURITY CONTRACT: each of those runs in its own child process and imports this
// file independently, so it must be pure/deterministic given the same env vars —
// no shared in-memory state crosses the process boundary, and every knob is an
// env var read here rather than a value passed between processes.
//
// codspeed.bench.ts deliberately does NOT import this file: the store libraries
// loaded below would put a foreign multi-MB wasm module into its Valgrind-
// instrumented process. The pure halves live elsewhere and are re-exported
// here so the workers keep their single import: generic helpers in ./util.ts,
// the dataset-generation layer in ./datasets.ts (both importable by
// codspeed.bench.ts).

import type { BenchOptions, Bench } from 'tinybench';
import type { Quad } from '@rdfjs/types';
import { existsSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';

import { VortexRdfStore, type BuildOptions } from '../entry/node.js';
import { fmtNs, freeWasm } from './util.js';
import { df, type Pat } from './datasets.js';
import {
    RdfStore,
    RdfStoreIndexNestedMapQuoted,
    TermDictionaryQuotedIndexed,
    TermDictionaryNumberRecordFullTerms,
} from 'rdf-stores';
import { Store as OxiStore } from 'oxigraph';

export * from './datasets.js';

// ─── Config (env-tunable) ────────────────────────────────────────────────────
//
// Scale, as a row count: BENCH_SIZE — the same env name the Rust suites read, so
// one knob sets every environment. BENCH_DIM remains as cube shorthand for quick
// pilots (BENCH_DIM=16 → 4,096 rows) and loses to an explicit row count. The
// default is the
// dashboard's indicative-overview scale: 2^20 rows — the
// benchmarks exist to show where vortex-rdf stands, not to be a stress test,
// and this size keeps a full refresh under an hour while still exercising
// term handling at ~630k distinct terms.
const dim = Number(process.env.BENCH_DIM ?? 0);
/** One dataset for the whole run: every store whose model has graphs builds
 *  from it and answers every pattern on it, and one whose model does not (hdt)
 *  reads the same rows with the graph dropped. */
export const N_TRIPLES = Number(process.env.BENCH_SIZE ?? 0) || (dim > 0 ? dim ** 3 : 1_048_576);
export const N_QUADS = N_TRIPLES;
/** Named graphs in that dataset, before the generator's coprimality nudge.
 *  Matches the Rust suite's `WANT_GRAPHS` and the Python suite's
 *  BENCH_GRAPHS_QUADS, so all three tabs describe the same shape. */
export const GRAPHS = Number(process.env.BENCH_GRAPHS_QUADS ?? 8);
export const MUT_BATCH = Number(process.env.MUT_BATCH ?? 10_000); // add/delete batch, independent of scale

// The dataset-generation layer (genDataset and friends, the probe patterns,
// and the dense-cube generators) lives in ./datasets.ts — pure, so the
// instrumented CodSpeed suite can import it too — and is re-exported above.

// ─── Adapter interface ───────────────────────────────────────────────────────
export interface StoreAdapter<H = unknown> {
    slug: string; // dashboard id segment
    label: string; // display name
    build(quads: Quad[]): Promise<H> | H; // ingest (idiomatic bulk API per lib)
    newEmpty(): Promise<H> | H; // empty store (add phase)
    addAll(h: H, quads: Quad[]): Promise<void> | void; // per-quad add loop
    addBatch?(h: H, quads: Quad[]): Promise<void> | void; // optional batch add (Vortex)
    deleteAll(h: H, quads: Quad[]): Promise<void> | void; // per-quad delete loop
    countMatch(h: H, p: Pat): Promise<number> | number; // count by consuming — every result term is read (consumeQuads)
    /** Resolve the pattern and return only the match count — no term value is
     *  read. Each store's cheapest correct count path (a count API where one
     *  exists, the result set's length otherwise): the COUNT/ASK shape. */
    countOnly(h: H, p: Pat): Promise<number> | number;
    // Cold-regime pair, optional: `snapshot` serializes a built store once
    // (untimed) and `open` adopts that snapshot into a fresh handle, which the
    // cold query phase does per iteration. Only the stores with a persistent
    // form implement them — an in-memory library has no artifact to reopen, so
    // its "cold" would be a full re-parse of the source and would compare
    // seconds against microseconds. Adapters without the pair are simply
    // absent from the cold rows.
    snapshot?(h: H): Promise<unknown>;
    open?(snapshot: unknown): Promise<H>;
    // Operations a store cannot perform in this environment, as opposed to slow
    // ones. When set, the worker emits explained `unsupported` cells instead of
    // timing anything: `ingestUnsupported` covers build-from-quads (hdt's wasm
    // surface is read-only — `build` opens a pre-built artifact instead),
    // `quadsUnsupported` covers the named-graph dataset (the HDT model has no
    // graphs), and `mutationUnsupported` covers add/delete. Same vocabulary as
    // the Rust and Python tabs' unsupported rows.
    ingestUnsupported?: string;
    quadsUnsupported?: string;
    mutationUnsupported?: string;
    // Both Vortex and oxigraph are wasm-bindgen: `.free()` deterministically drops
    // the WASM-side allocation. The generated FinalizationRegistry is a fallback
    // only — V8 has no visibility into WASM linear-memory pressure, so it can defer
    // running finalizers arbitrarily long, letting dozens of multi-million-quad
    // stores queue up unfreed. Every store this bench builds must be disposed as
    // soon as its measurement is done, not left to GC.
    dispose?(h: H): void;
}

// Disposal alone isn't enough for the pure-JS adapters (rdf-stores): a 2M-quad store
// there is a large, long-lived Map-of-Maps graph, and merely dropping the reference
// only makes it *eligible* for GC — V8's generational collector has no obligation to
// actually reclaim it before the next multi-hundred-MB allocation piles on top. Run
// with `--expose-gc` (wired into every worker spawn) and force a collection at every
// isolation point, for every adapter, wasm-backed or not.
// Disposal is best-effort: a store that trapped mid-query can leave its wasm
// value borrowed, so `free()` itself throws ("attempted to take ownership of a
// Rust value while it was borrowed"). That is cleanup of an already-failed
// measurement — letting it propagate would discard every row the adapter had
// successfully produced, which is exactly backwards. The collection below still
// runs, and the process exits shortly after regardless.
export function reclaim(a: StoreAdapter, h: unknown): void {
    try {
        a.dispose?.(h);
    } catch (e) {
        console.error(`  !! ${a.label}: dispose failed (${e instanceof Error ? e.message : e})`);
    }
    global.gc?.();
}

// Vortex: one adapter per curated build variant (mirrors the Rust star design axes).
export const VORTEX_VARIANTS: { slug: string; label: string; options: BuildOptions }[] = [
    { slug: 'vortex_dict', label: 'Vortex Dict', options: { layout: 'dictionary' } },
    { slug: 'vortex_dict_byref', label: 'Vortex Dict+ByRef', options: { layout: 'dictionary', indexes: ['secondary-by-reference'] } },
    { slug: 'vortex_dict_bycopy', label: 'Vortex Dict+ByCopy', options: { layout: 'dictionary', indexes: ['secondary-by-copy'] } },
    { slug: 'vortex_default', label: 'Vortex Default', options: { layout: 'default' } },
    { slug: 'vortex_default_byref', label: 'Vortex Default+ByRef', options: { layout: 'default', indexes: ['secondary-by-reference'] } },
    { slug: 'vortex_default_bycopy', label: 'Vortex Default+ByCopy', options: { layout: 'default', indexes: ['secondary-by-copy'] } },
];

/** Count a result set by consuming it: read every term of every quad.
 *
 *  The stores return different amounts of *done* work: the pure-JS stores hold
 *  eager term objects, while Vortex quads are lazy wasm-backed proxies whose
 *  term values have not crossed the boundary yet. Counting `.length` alone lets
 *  Vortex skip that crossing entirely — the JS tab then reads structurally
 *  faster than the same query on the same data on the Rust and Python tabs,
 *  which both materialize (quads_vec / N-Triples strings). Reading all four
 *  term values makes materialization part of the measured work everywhere, and
 *  puts the wasm boundary cost — the thing worth optimizing — on the page. */
let consumeSink = 0;

/** Thrown when a consume loop exceeds [`CONSUME_BUDGET_MS`]. The message is what
 *  lands in the run's `failures` list, so it says how far the loop got. */
export class ConsumeBudgetExceeded extends Error {
    constructor(done: number, total: number, ms: number) {
        super(
            `consume budget exceeded after ${(ms / 1000).toFixed(0)}s ` +
            `(${done.toLocaleString()} of ${total.toLocaleString()} quads; ` +
            `raise CONSUME_BUDGET_MS to wait longer)`,
        );
        this.name = 'ConsumeBudgetExceeded';
    }
}

/** Wall-clock budget for one consume loop, ms. `0` disables the check.
 *
 *  The budget exists because a store that cannot materialize a result set does
 *  not fail fast — it death-marches, ballooning the heap towards the V8 cap
 *  (GC falls out of concurrent mode into back-to-back full collections) until
 *  its wasm module traps. The loop below is where that time is spent, so this
 *  is where it can be cut short: the throw surfaces through the worker's
 *  per-phase catch as an ordinary `failures` entry and renders as the same
 *  "failed" cell, delivered in minutes rather than tens of minutes.
 *
 *  The default is orders of magnitude above any *successful* consume. */
const CONSUME_BUDGET_MS = Number(process.env.CONSUME_BUDGET_MS ?? 120_000);

export function consumeQuads(
    quads: ArrayLike<{ subject: { value: string }; predicate: { value: string };
                       object: { value: string }; graph?: { value: string } }>,
): number {
    let acc = 0;
    // Check the budget every 64Ki quads, not every iteration: a per-iteration
    // `performance.now()` would add ~2M timer calls to a measured full scan.
    // Result sets below 64Ki rows are never checked at all, which is fine —
    // a death march needs a large result set to march over.
    const t0 = CONSUME_BUDGET_MS > 0 ? performance.now() : 0;
    for (let i = 0; i < quads.length; i++) {
        if (CONSUME_BUDGET_MS > 0 && (i & 0xffff) === 0xffff) {
            const ms = performance.now() - t0;
            if (ms > CONSUME_BUDGET_MS) throw new ConsumeBudgetExceeded(i, quads.length, ms);
        }
        const q = quads[i];
        acc += q.subject.value.length + q.predicate.value.length
             + q.object.value.length + (q.graph?.value.length ?? 0);
    }
    consumeSink += acc; // the reads must escape, or the JIT may drop them
    return quads.length;
}
/** [`consumeQuads`]'s contract for a store whose results are term strings
 *  rather than quad objects (hdt): read every string, escape the reads. */
export function consumeStrings(strings: string[]): void {
    let acc = 0;
    for (const s of strings) acc += s.length;
    consumeSink += acc;
}

export function vortexAdapter(variant: { slug: string; label: string; options: BuildOptions }): StoreAdapter<VortexRdfStore> {
    return {
        slug: variant.slug,
        label: variant.label,
        build: (quads) => VortexRdfStore.fromQuads(quads, variant.options),
        newEmpty: () => VortexRdfStore.empty(),
        addAll: async (h, quads) => { for (const q of quads) await h.addQuad(q); },
        addBatch: (h, quads) => h.addQuads(quads),
        deleteAll: async (h, quads) => { for (const q of quads) await h.deleteQuad(q); },
        countMatch: async (h, p) => consumeQuads(await h.getQuads(p.s, p.p, p.o, p.g)),
        countOnly: (h, p) => h.countQuads(p.s, p.p, p.o, p.g),
        snapshot: (h) => h.toBytes(),
        open: (bytes) => VortexRdfStore.fromBytes(bytes as Uint8Array),
        dispose: freeWasm,
    };
}

function newRdfStore(kind: 'default' | 'single'): RdfStore {
    if (kind === 'default') return RdfStore.createDefault();
    // Single GSPO index — the same constructors createDefault uses, one combination.
    // `createDefault` is rdf-stores's only factory and always wires in quoted-triple
    // (RDF-star) support (RdfStoreIndexNestedMapQuoted / TermDictionaryQuotedIndexed) —
    // there's no leaner non-quoted construction path in the library, so mirroring that
    // exact choice here (rather than substituting a plain, non-quoted index) is what
    // keeps this an apples-to-apples comparison with what real rdf-stores callers get.
    // Pin <number> (as createDefault does) so Q resolves to Quad, not BaseQuad.
    return new RdfStore<number>({
        indexCombinations: [['graph', 'subject', 'predicate', 'object']],
        indexConstructor: (o) => new RdfStoreIndexNestedMapQuoted(o),
        dictionary: new TermDictionaryQuotedIndexed(new TermDictionaryNumberRecordFullTerms()),
        dataFactory: df,
    });
}

export function rdfStoresAdapter(kind: 'default' | 'single', label: string): StoreAdapter<RdfStore> {
    return {
        slug: kind === 'default' ? 'rdfstores_default' : 'rdfstores_single',
        label,
        build: (quads) => { const s = newRdfStore(kind); for (const q of quads) s.addQuad(q); return s; },
        newEmpty: () => newRdfStore(kind),
        addAll: (h, quads) => { for (const q of quads) h.addQuad(q); },
        deleteAll: (h, quads) => { for (const q of quads) h.removeQuad(q); },
        countMatch: (h, p) => consumeQuads(h.getQuads(p.s, p.p, p.o, p.g)), // synchronous array
        countOnly: (h, p) => h.countQuads(p.s, p.p, p.o, p.g),
    };
}

export function oxigraphAdapter(): StoreAdapter<OxiStore> {
    return {
        slug: 'oxigraph',
        label: 'oxigraph',
        build: (quads) => new OxiStore(quads as unknown as Iterable<never>), // bulk via constructor
        newEmpty: () => new OxiStore(),
        addAll: (h, quads) => { for (const q of quads) h.add(q as never); },
        deleteAll: (h, quads) => { for (const q of quads) h.delete(q as never); },
        countMatch: (h, p) => consumeQuads(h.match(p.s as never, p.p as never, p.o as never, p.g as never)),
        // No count API: match() materializes its result array either way, so
        // walking its length is this store's floor for a count.
        countOnly: (h, p) => h.match(p.s as never, p.p as never, p.o as never, p.g as never).length,
        dispose: freeWasm,
    };
}

// ─── hdt (wasm, read-only) ───────────────────────────────────────────────────
//
// The hdt crate's wasm surface is read-only: HDT *construction* (`read_nt`)
// spawns OS threads and uses rayon, which trap on wasm32, so the artifact is
// built natively and only opened here. The Rust comparative bench writes one
// from the same shared dataset as part of its run (`cargo bench --bench
// compare`, at BENCH_SIZE = this tab's D³) — point `HDT_FILE` elsewhere to
// override. The wasm module itself is the bench's own build of the crate's
// bindings: `npm run build:hdt-wasm` → bench/hdt-pkg (see bench/hdt-wasm).

const here = dirname(fileURLToPath(import.meta.url));
export const HDT_FILE = process.env.HDT_FILE
    ?? resolve(here, '../../target/bench_compare/hdt/data.hdt');

type HdtModule = typeof import('./hdt-pkg/hdt_wasm_bench.js');
type HdtStore = import('./hdt-pkg/hdt_wasm_bench.js').HdtStore;
let hdtModule: HdtModule | null = null;
async function hdtMod(): Promise<HdtModule> {
    if (!hdtModule) {
        // Same init path as entry/node.js: hand `init` the wasm bytes directly,
        // because Node's fetch does not speak file: URLs.
        const mod = await import('./hdt-pkg/hdt_wasm_bench.js');
        const wasmPath = resolve(here, 'hdt-pkg/hdt_wasm_bench_bg.wasm');
        await mod.default({ module_or_path: readFileSync(wasmPath) });
        hdtModule = mod;
    }
    return hdtModule;
}

/** A probe term in HDT dictionary spelling: IRIs bare, literals with their
 *  quotes — the same conversion the Rust tab's hdt adapter applies. */
function hdtTerm(t: { termType: string; value: string } | null): string | undefined {
    if (!t) return undefined;
    return t.termType === 'Literal' ? `"${t.value}"` : t.value;
}

/** Window for `ids_to_strings`: translating several million ids in one call is
 *  that surface's documented OOM risk, so the full scan feeds it chunks. */
const HDT_TRANSLATE_IDS = 300_000 * 3;

export function hdtAdapter(): StoreAdapter<HdtStore> {
    return {
        slug: 'hdt',
        // Not "(file)": the wasm bindings parse the artifact's bytes into wasm
        // linear memory, so this store answers from memory like the others.
        label: 'hdt',
        ingestUnsupported:
            'the wasm bindings are read-only — the artifact is built natively '
            + '(the Rust comparative bench writes it) and only opened here',
        quadsUnsupported: 'the HDT format has no named graphs — triples only',
        mutationUnsupported: 'an HDT file is immutable once built',
        // `build` opens the pre-built artifact; the quads argument is only the
        // consistency check that the artifact and this run's generated dataset
        // are the same rows — the probe-count cross-check then verifies the
        // contents, not just the cardinality.
        build: async (quads) => {
            if (!existsSync(HDT_FILE)) {
                throw new Error(
                    `no HDT artifact at ${HDT_FILE} — the Rust comparative bench builds it `
                    + '(cargo bench --bench compare), or point HDT_FILE at one');
            }
            const mod = await hdtMod();
            const h = new mod.HdtStore(readFileSync(HDT_FILE));
            const n = h.num_triples();
            if (n !== quads.length) {
                h.free();
                throw new Error(
                    `HDT artifact at ${HDT_FILE} holds ${n} triples but this run's dataset has `
                    + `${quads.length} — rebuild it: BENCH_SIZE=${quads.length} cargo bench --bench compare`);
            }
            return h;
        },
        newEmpty: () => { throw new Error('hdt is read-only'); },
        addAll: () => { throw new Error('hdt is read-only'); },
        deleteAll: () => { throw new Error('hdt is read-only'); },
        countMatch: (h, p) => {
            const ids = h.triple_ids_with_pattern(hdtTerm(p.s), hdtTerm(p.p), hdtTerm(p.o));
            // Materialize every term string, in windows — the counterpart of
            // consumeQuads reading every term value on the quad-shaped stores.
            for (let off = 0; off < ids.length; off += HDT_TRANSLATE_IDS) {
                consumeStrings(h.ids_to_strings(ids.subarray(off, Math.min(off + HDT_TRANSLATE_IDS, ids.length))));
            }
            return ids.length / 3;
        },
        // The id-level read: pattern resolution over the dictionary-encoded
        // triples, no string translated.
        countOnly: (h, p) => h.triple_ids_with_pattern(hdtTerm(p.s), hdtTerm(p.p), hdtTerm(p.o)).length / 3,
        // Its persistent form IS the artifact: snapshot hands over the file's
        // bytes, open parses them — the same reopen the other tabs measure.
        snapshot: async () => readFileSync(HDT_FILE),
        open: async (bytes) => {
            const mod = await hdtMod();
            return new mod.HdtStore(bytes as Uint8Array);
        },
        dispose: freeWasm,
    };
}

// Full matrix for ingest + query.
export const ADAPTERS: StoreAdapter[] = [
    ...VORTEX_VARIANTS.map(vortexAdapter),
    rdfStoresAdapter('default', 'rdf-stores (default)'),
    rdfStoresAdapter('single', 'rdf-stores (1 index)'),
    oxigraphAdapter(),
    hdtAdapter(),
];

// Representative subset for mutations (per-quad add/delete is the fair cross-lib path).
export const MUT_ADAPTERS: StoreAdapter[] = [
    vortexAdapter({ slug: 'vortex', label: 'Vortex', options: { layout: 'dictionary' } }),
    rdfStoresAdapter('default', 'rdf-stores (default)'),
    oxigraphAdapter(),
];

// ─── Result normalization (dashboard shape) ──────────────────────────────────
export interface Row {
    group: string; variant: string | null; id: string;
    fastest: string; slowest: string; median: string; mean: string;
    fastest_ns: number | null; slowest_ns: number | null;
    median_ns: number | null; mean_ns: number | null;
    samples: string;
    /** Cache regime this row was measured in — 'warm' is a repeat query on an
     *  open store, 'cold' opens one per iteration and answers its first. Absent
     *  on rows where the distinction does not apply (ingest, mutation). */
    regime?: 'cold' | 'warm';
    /** An operation this store cannot perform, as opposed to a slow one. */
    unsupported?: boolean;
    /** Why, shown as the cell's tooltip. */
    reason?: string;
}

/** A row the dashboard renders as 'unsupported' rather than as a missing cell.
 *
 *  Mirrors `unsupported_row` in python/bench/worker.py, down to the null
 *  `median_ns` that keeps the row out of its column's best/ratio arithmetic:
 *  an operation a store cannot perform is not a slow result, and a blank cell
 *  reads as a benchmark nobody ran. */
export function unsupportedRow(id: string, reason: string): Row {
    const [group, variant] = id.split('::');
    return {
        group, variant: variant ?? null, id,
        unsupported: true, reason,
        fastest: 'unsupported', slowest: 'unsupported',
        median: 'unsupported', mean: 'unsupported',
        fastest_ns: null, slowest_ns: null, median_ns: null, mean_ns: null,
        samples: '0',
    };
}

export function collect(bench: Bench, results: Row[], regime?: 'cold' | 'warm'): void {
    for (const task of bench.tasks) {
        const r = task.result;
        if (!r || !('latency' in r) || !r.latency) continue;
        const lat = r.latency; // tinybench reports milliseconds
        const mean_ns = lat.mean * 1e6;
        const median_ns = lat.p50 * 1e6;
        const fastest_ns = lat.min * 1e6;
        const slowest_ns = lat.max * 1e6;
        const idx = task.name.indexOf('::');
        const group = idx >= 0 ? task.name.slice(0, idx) : task.name;
        const variant = idx >= 0 ? task.name.slice(idx + 2) : null;
        results.push({
            group, variant, id: task.name,
            fastest: fmtNs(fastest_ns), slowest: fmtNs(slowest_ns),
            median: fmtNs(median_ns), mean: fmtNs(mean_ns),
            fastest_ns, slowest_ns, median_ns, mean_ns,
            samples: String(lat.samplesCount),
            ...(regime ? { regime } : {}),
        });
    }
}

// tinybench options per phase, in the repetition counts every comparative suite
// shares: 10 measured runs for a query (`QUERY_ITERS` in
// `python/bench/worker.py`, `QUERY_SAMPLES` in `core/benches/support/mod.rs`),
// 3 for the phases that cost seconds each.
//
// `time: 0` is what makes the query count mean 10. tinybench treats `time` as a
// minimum duration and keeps iterating until BOTH budgets are satisfied, so any
// nonzero floor would run a microsecond-scale query tens of thousands of times
// and report a mean over those, against the other tabs' median of 10.
export const QUERY_OPTS: BenchOptions = { time: 0, iterations: 10, warmup: true, warmupIterations: 5, throws: true };
export const HEAVY_OPTS: BenchOptions = { time: 0, iterations: 3, warmup: false, warmupIterations: 0, throws: true };
// The cold arm keeps the query repetition count but drops warmup, for two
// reasons. Warming up a cold measurement is self-contradictory — every
// iteration adopts a fresh store by construction, so there is nothing for a
// preceding run to warm. And each adoption is expensive in a way that does not
// come back: the first query on a `fromBytes` store retains its whole buffer
// past `free()`, and wasm linear memory never shrinks, so warmup runs would
// spend a third of the budget for nothing and trip the wasm allocator.
export const COLD_QUERY_OPTS: BenchOptions = { time: 0, iterations: 10, warmup: false, warmupIterations: 0, throws: true };
// See the comment on FULL_SCAN_PATTERN (datasets.ts): far fewer repetitions of a
// full-table dump.
export const FULL_SCAN_OPTS: BenchOptions = { time: 0, iterations: 3, warmup: false, warmupIterations: 0, throws: true };

// ─── Memory instrumentation ──────────────────────────────────────────────────
//
// `peakRssMb` is the cross-library metric: it is the only figure that means the
// same thing for a wasm-backed store (Vortex, oxigraph) and a pure-JS one
// (rdf-stores). `wasmHeapMb` is for attribution *within* Vortex only — it is
// meaningless for rdf-stores and would understate oxigraph, which has its own
// module. Never rank adapters by it.

/** Linux-only: the kernel-tracked high-water mark, the true peak RSS for this
 * process's whole lifetime — unlike `process.memoryUsage().rss`, which is only a
 * point-in-time reading and can miss a spike between samples. */
export function peakRssMb(): number | null {
    try {
        const status = readFileSync('/proc/self/status', 'utf8');
        const m = status.match(/^VmHWM:\s+(\d+)\s+kB/m);
        return m ? Math.round(Number(m[1]) / 1024) : null;
    } catch {
        return null;
    }
}

/** Current (not peak) RSS. Paired with dropping the input quads and forcing a
 * collection, this isolates what a store actually holds: the generated `Quad[]`
 * is well over a gigabyte of JS objects at the default scale, is identical for
 * every adapter, and otherwise swamps the differences between them in the peak. */
export function rssMb(): number | null {
    try {
        const status = readFileSync('/proc/self/status', 'utf8');
        const m = status.match(/^VmRSS:\s+(\d+)\s+kB/m);
        return m ? Math.round(Number(m[1]) / 1024) : null;
    } catch {
        return null;
    }
}

/** JS heap in use, after forcing a collection. Requires `--expose-gc` (every
 * worker spawn wires it in); without it this is a point-in-time reading that may
 * include garbage. */
export function jsHeapMb(): number {
    global.gc?.();
    return Math.round(process.memoryUsage().heapUsed / 1048576);
}

/** Vortex's wasm linear memory, in MB.
 *
 * `__wbg_init` short-circuits and returns the exports when the module is already
 * initialized (`entry/node.js` does that at import time), so re-invoking it is
 * just how one gets at the namespace. Linear memory only ever grows, so this is
 * simultaneously the current and the peak figure. */
export async function wasmHeapMb(): Promise<number | null> {
    try {
        const mod = await import('../pkg/web/vortex_rdf.js');
        const wasm = await mod.default();
        return Math.round((wasm as { memory: WebAssembly.Memory }).memory.buffer.byteLength / 1048576);
    } catch {
        return null;
    }
}
