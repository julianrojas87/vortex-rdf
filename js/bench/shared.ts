// Shared definitions between the orchestrator (compare.bench.ts) and the per-adapter
// worker (compare.worker.ts). Each adapter's full lifecycle runs in its own child
// process (see compare.bench.ts) — this file is imported independently by both, so
// it must be pure/deterministic given the same env vars (no shared in-memory state
// crosses the process boundary).

import type { BenchOptions, Bench } from 'tinybench';
import { DataFactory } from 'rdf-data-factory';
import type { Quad, Term } from '@rdfjs/types';
import { readFileSync } from 'node:fs';

import { VortexRdfStore, type BuildOptions } from '../entry/node.js';
import {
    RdfStore,
    RdfStoreIndexNestedMapQuoted,
    TermDictionaryQuotedIndexed,
    TermDictionaryNumberRecordFullTerms,
} from 'rdf-stores';
import { Store as OxiStore } from 'oxigraph';

export const df = new DataFactory();

// ─── Config (env-tunable) ────────────────────────────────────────────────────
export const D = Number(process.env.BENCH_DIM ?? 128); // triples dataset: D³ triples
export const DQ = Number(process.env.BENCH_DIM_QUADS ?? 32); // quads dataset: DQ⁴ quads
export const MUT_BATCH = Number(process.env.MUT_BATCH ?? 10_000); // add/delete batch, independent of D
/** Row counts, kept as D³/DQ⁴ so the existing scale knobs mean what they always did. */
export const N_TRIPLES = D ** 3;
export const N_QUADS = DQ ** 4;

// ─── Dataset generator ───────────────────────────────────────────────────────
//
// Term cardinality is an explicit knob, independent of row count. The previous
// generator drew D³ rows from a namespace of only D IRIs (128 distinct terms for
// 2,097,152 triples), which made every store's term handling — dictionaries,
// interning, string storage — invisible to the benchmark, and is nothing like
// real RDF, where distinct terms scale with the data.
//
// Uniqueness: term indices are `i % k` per role, so quad i maps to the residue
// tuple (i mod nSubj, i mod nPred, i mod nObj, i mod nGraph). By the Chinese
// Remainder Theorem that map is injective over `i < lcm(...)`, so making the
// four moduli pairwise coprime (their lcm is then their product) and checking
// the product covers `n` guarantees every generated quad is distinct — with no
// dedupe set, which at these row counts would itself dominate memory.
//
// Deliberate consequence: index 0 exists for every role, so row 0 satisfies all
// four single-term probes and every pattern in `datasetProbes` matches at least
// one row.

const BASE = 'http://data.example.org';

export interface DatasetOpts {
    /** distinct subjects / n (default 1.0 — real RDF subjects are near-unique). */
    subjectRatio?: number;
    /** distinct predicates: a small closed vocabulary, as in real data (default 32). */
    predicates?: number;
    /** distinct objects / n (default 0.5). */
    objectRatio?: number;
    /** fraction of distinct objects that are literals rather than IRIs (default 0.4). */
    literalFrac?: number;
    /** distinct named graphs; 1 means the default graph only (default 1). */
    graphs?: number;
}

/** Env-overridable defaults, so a sweep can vary cardinality without new code. */
export function datasetOpts(o: DatasetOpts = {}): Required<DatasetOpts> {
    return {
        subjectRatio: o.subjectRatio ?? Number(process.env.BENCH_SUBJ_RATIO ?? 1),
        predicates: o.predicates ?? Number(process.env.BENCH_PREDICATES ?? 32),
        objectRatio: o.objectRatio ?? Number(process.env.BENCH_OBJ_RATIO ?? 0.5),
        literalFrac: o.literalFrac ?? Number(process.env.BENCH_LITERAL_FRAC ?? 0.4),
        graphs: o.graphs ?? Number(process.env.BENCH_GRAPHS ?? 1),
    };
}

function gcd(a: number, b: number): number {
    while (b) [a, b] = [b, a % b];
    return a;
}

/** Per-role term counts: the requested values nudged up until pairwise coprime. */
export function moduli(n: number, opts: DatasetOpts = {}): {
    nSubj: number; nPred: number; nObj: number; nGraph: number; terms: number;
} {
    const o = datasetOpts(opts);
    const want = [
        Math.max(1, Math.round(n * o.subjectRatio)),
        Math.max(1, Math.round(o.predicates)),
        Math.max(1, Math.round(n * o.objectRatio)),
        Math.max(1, Math.round(o.graphs)),
    ];
    const got: number[] = [];
    for (const w of want) {
        let k = w;
        while (got.some((g) => gcd(g, k) !== 1)) k++;
        got.push(k);
    }
    const [nSubj, nPred, nObj, nGraph] = got;
    // Product == lcm because they are pairwise coprime; see the CRT note above.
    // Use logs: the product overflows the float mantissa long before it matters.
    const logProduct = got.reduce((acc, k) => acc + Math.log(k), 0);
    if (logProduct < Math.log(n)) {
        throw new Error(
            `dataset cardinality too low for ${n} distinct quads: ` +
            `${nSubj}x${nPred}x${nObj}x${nGraph} cannot cover it — raise a ratio`,
        );
    }
    // The default graph is a term too, and it is not one of the named graphs.
    const terms = nSubj + nPred + nObj + (nGraph === 1 ? 1 : nGraph);
    return { nSubj, nPred, nObj, nGraph, terms };
}

const subjectTerm = (i: number) =>
    df.namedNode(`${BASE}/resource/2026/subject/${String(i).padStart(9, '0')}`);
const predicateTerm = (i: number) =>
    df.namedNode(`${BASE}/ontology/2026/property/${String(i).padStart(4, '0')}`);
const graphTerm = (i: number) =>
    df.namedNode(`${BASE}/graph/2026/named/${String(i).padStart(6, '0')}`);

/** Objects alternate IRI/literal deterministically, in `literalFrac` proportion. */
function objectTerm(i: number, literalFrac: number): Term {
    return i % 10 < Math.round(literalFrac * 10)
        ? df.literal(`descriptive object value number ${String(i).padStart(9, '0')}`)
        : df.namedNode(`${BASE}/resource/2026/object/${String(i).padStart(9, '0')}`);
}

/** Quad `i` of the `n`-row dataset. Pure in `i`, so probes and prefixes can be
 *  derived without materializing the array. */
function quadAt(i: number, m: ReturnType<typeof moduli>, literalFrac: number): Quad {
    return df.quad(
        subjectTerm(i % m.nSubj),
        predicateTerm(i % m.nPred),
        objectTerm(i % m.nObj, literalFrac) as never,
        m.nGraph === 1 ? df.defaultGraph() : graphTerm(i % m.nGraph),
    );
}

/** `n` distinct quads whose term cardinality follows `opts`. */
export function genDataset(n: number, opts: DatasetOpts = {}): Quad[] {
    const o = datasetOpts(opts);
    const m = moduli(n, opts);
    const out: Quad[] = new Array(n);
    for (let i = 0; i < n; i++) out[i] = quadAt(i, m, o.literalFrac);
    return out;
}

/** The first `take` quads `genDataset(n, opts)` would produce, without building
 *  the full array — the mutation worker needs only a MUT_BATCH-sized slice for
 *  the delete phase, and `n` can be a couple of million. */
export function genDatasetPrefix(n: number, take: number, opts: DatasetOpts = {}): Quad[] {
    const o = datasetOpts(opts);
    const m = moduli(n, opts);
    const k = Math.min(take, n);
    const out: Quad[] = new Array(k);
    for (let i = 0; i < k; i++) out[i] = quadAt(i, m, o.literalFrac);
    return out;
}

/** A batch of fresh quads in a disjoint namespace, for the add phase. */
export function genFresh(n: number): Quad[] {
    const out: Quad[] = new Array(n);
    for (let i = 0; i < n; i++) {
        out[i] = df.quad(
            df.namedNode(`${BASE}/fresh/2026/subject/${String(i).padStart(9, '0')}`),
            df.namedNode(`${BASE}/fresh/2026/property/0000`),
            df.namedNode(`${BASE}/fresh/2026/object/${String(i).padStart(9, '0')}`),
        );
    }
    return out;
}

// ─── Query patterns (probe terms fixed at index 0, so they always hit rows) ──
export type Pat = { name: string; s: Term | null; p: Term | null; o: Term | null; g: Term | null };
// 'full' (every variable unbound) is measured separately from the selective patterns,
// under FULL_SCAN_OPTS's much lower repetition count: repeating a full-table
// materialization (all D³ rows, ~2M at the default D=128) ~15x (QUERY_OPTS's 5 warmup
// + 10 timed) reproducibly trips an internal `unreachable` trap in oxigraph's wasm
// build around the 5th repetition on the same store — confirmed in isolation with
// nothing else in the process, so it's not a cross-adapter memory issue. No other
// adapter under test showed any problem with that many repetitions, but a full-table
// dump repeated many times is a heavy op for any store (closer in spirit to `ingest`
// than to a selective query), so the lower budget applies to every adapter uniformly
// rather than singling out the one library that happens to crash on it.
export const FULL_SCAN_PATTERN: Pat = { name: 'full', s: null, p: null, o: null, g: null };

/**
 * Probes for an `n`-row dataset built with `opts`, all bound to term index 0 so
 * every one matches row 0 at minimum — no pattern can silently measure a
 * zero-row query.
 *
 * Selectivity now follows the data rather than the old shared namespace: at the
 * defaults `S` matches ~1 row (subjects are near-unique), `P` ~n/32, `O` ~2, and
 * the conjunctions narrow to a handful. That spread is what real queries look
 * like, but it is *not* comparable to the pre-cardinality numbers — the workers
 * record each pattern's matched-row count alongside its timing so the figures
 * stay interpretable.
 */
export function datasetProbes(n: number, opts: DatasetOpts = {}): {
    triples: Pat[]; quads: Pat[]; full: Pat;
} {
    const o = datasetOpts(opts);
    const m = moduli(n, opts);
    const s0 = subjectTerm(0);
    const p0 = predicateTerm(0);
    const o0 = objectTerm(0, o.literalFrac);
    const g0 = m.nGraph === 1 ? df.defaultGraph() : graphTerm(0);
    return {
        triples: [
            { name: 'S', s: s0, p: null, o: null, g: null },
            { name: 'P', s: null, p: p0, o: null, g: null },
            { name: 'O', s: null, p: null, o: o0, g: null },
            { name: 'PO', s: null, p: p0, o: o0, g: null },
            { name: 'SPO', s: s0, p: p0, o: o0, g: null },
        ],
        quads: [
            { name: 'G', s: null, p: null, o: null, g: g0 },
            { name: 'SPOG', s: s0, p: p0, o: o0, g: g0 },
        ],
        full: FULL_SCAN_PATTERN,
    };
}

// ─── Adapter interface ───────────────────────────────────────────────────────
export interface StoreAdapter<H = unknown> {
    slug: string; // dashboard id segment
    label: string; // display name
    build(quads: Quad[]): Promise<H> | H; // ingest (idiomatic bulk API per lib)
    newEmpty(): Promise<H> | H; // empty store (add phase)
    addAll(h: H, quads: Quad[]): Promise<void> | void; // per-quad add loop
    addBatch?(h: H, quads: Quad[]): Promise<void> | void; // optional batch add (Vortex)
    deleteAll(h: H, quads: Quad[]): Promise<void> | void; // per-quad delete loop
    countMatch(h: H, p: Pat): Promise<number> | number; // consume + count
    // Both Vortex and oxigraph are wasm-bindgen: `.free()` deterministically drops
    // the WASM-side allocation. The generated FinalizationRegistry is a fallback
    // only — V8 has no visibility into WASM linear-memory pressure, so it can defer
    // running finalizers arbitrarily long, letting dozens of multi-million-quad
    // stores queue up unfreed. Every store this bench builds must be disposed as
    // soon as its measurement is done, not left to GC.
    dispose?(h: H): void;
}

// Both Vortex's and oxigraph's public `.d.ts` are hand-curated (typescript_custom_section
// / equivalent) and deliberately omit the wasm-bindgen `free()` method from the normal
// consumer-facing API — ordinary callers are meant to lean on the FinalizationRegistry.
// This benchmark is not an ordinary caller: it needs deterministic disposal, so it reaches
// past the curated type rather than widening what real consumers see.
function freeWasm(h: unknown): void {
    (h as { free(): void }).free();
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
    { slug: 'vortex_unsorted_dict', label: 'Vortex Unsorted/Dict', options: { builder: 'Unsorted', layout: 'Dictionary' } },
    { slug: 'vortex_unsorted_default', label: 'Vortex Unsorted/Default', options: { builder: 'Unsorted', layout: 'Default' } },
    { slug: 'vortex_sorted_dict', label: 'Vortex Sorted/Dict', options: { builder: 'Sorted', layout: 'Dictionary' } },
    { slug: 'vortex_sorted_dict_byref', label: 'Vortex Sorted/Dict+ByRef', options: { builder: 'Sorted', layout: 'Dictionary', indexes: ['SecondaryByReference'] } },
    { slug: 'vortex_sorted_dict_bycopy', label: 'Vortex Sorted/Dict+ByCopy', options: { builder: 'Sorted', layout: 'Dictionary', indexes: ['SecondaryByCopy'] } },
    { slug: 'vortex_sorted_default', label: 'Vortex Sorted/Default', options: { builder: 'Sorted', layout: 'Default' } },
    { slug: 'vortex_sorted_default_byref', label: 'Vortex Sorted/Default+ByRef', options: { builder: 'Sorted', layout: 'Default', indexes: ['SecondaryByReference'] } },
    { slug: 'vortex_sorted_default_bycopy', label: 'Vortex Sorted/Default+ByCopy', options: { builder: 'Sorted', layout: 'Default', indexes: ['SecondaryByCopy'] } },
];

export function vortexAdapter(variant: { slug: string; label: string; options: BuildOptions }): StoreAdapter<VortexRdfStore> {
    return {
        slug: variant.slug,
        label: variant.label,
        build: (quads) => VortexRdfStore.fromQuads(quads, variant.options),
        newEmpty: () => VortexRdfStore.empty(),
        addAll: async (h, quads) => { for (const q of quads) await h.addQuad(q); },
        addBatch: (h, quads) => h.addQuads(quads),
        deleteAll: async (h, quads) => { for (const q of quads) await h.deleteQuad(q); },
        countMatch: async (h, p) => (await h.getQuads(p.s, p.p, p.o, p.g)).length,
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
        countMatch: (h, p) => h.getQuads(p.s, p.p, p.o, p.g).length, // synchronous array
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
        countMatch: (h, p) => h.match(p.s as never, p.p as never, p.o as never, p.g as never).length,
        dispose: freeWasm,
    };
}

// Full matrix for ingest + query.
export const ADAPTERS: StoreAdapter[] = [
    ...VORTEX_VARIANTS.map(vortexAdapter),
    rdfStoresAdapter('default', 'rdf-stores (default)'),
    rdfStoresAdapter('single', 'rdf-stores (1 index)'),
    oxigraphAdapter(),
];

// Representative subset for mutations (per-quad add/delete is the fair cross-lib path).
export const MUT_ADAPTERS: StoreAdapter[] = [
    vortexAdapter({ slug: 'vortex', label: 'Vortex', options: { layout: 'Dictionary' } }),
    rdfStoresAdapter('default', 'rdf-stores (default)'),
    oxigraphAdapter(),
];

// ─── Result normalization (dashboard shape) ──────────────────────────────────
export interface Row {
    group: string; variant: string | null; id: string;
    fastest: string; slowest: string; median: string; mean: string;
    fastest_ns: number; slowest_ns: number; median_ns: number; mean_ns: number;
    samples: string;
}

export function fmtNs(ns: number): string {
    if (ns < 1e3) return ns.toFixed(0) + ' ns';
    if (ns < 1e6) return (ns / 1e3).toPrecision(3) + ' µs';
    if (ns < 1e9) return (ns / 1e6).toPrecision(3) + ' ms';
    return (ns / 1e9).toPrecision(3) + ' s';
}

export function collect(bench: Bench, results: Row[]): void {
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
        });
    }
}

// tinybench options per phase. Query gets a time budget; the expensive build/mutation
// phases get a low fixed iteration count (each build is costly at D=128).
export const QUERY_OPTS: BenchOptions = { time: 500, iterations: 10, warmup: true, warmupIterations: 5, throws: true };
export const HEAVY_OPTS: BenchOptions = { time: 0, iterations: 3, warmup: false, warmupIterations: 0, throws: true };
// See the comment on FULL_SCAN_PATTERN: far fewer repetitions of a full-table dump.
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
