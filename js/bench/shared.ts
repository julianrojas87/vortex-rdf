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
export const D = Number(process.env.BENCH_DIM ?? 128); // triples dataset: D³ triples (rdf-stores `-d 128`)
export const DQ = Number(process.env.BENCH_DIM_QUADS ?? 32); // quads dataset: DQ⁴ quads (graph patterns)
export const MUT_BATCH = Number(process.env.MUT_BATCH ?? 10_000); // add/delete batch, independent of D
const EX = 'http://example.org/#';

// ─── Dataset generators (rdf-stores.js style) ────────────────────────────────
const nn = (n: number | string) => df.namedNode(EX + n);

/** D³ triples in the default graph: quad(ex#s, ex#p, ex#o). */
export function genTriples(d: number): Quad[] {
    const out: Quad[] = [];
    for (let s = 0; s < d; s++)
        for (let p = 0; p < d; p++)
            for (let o = 0; o < d; o++) out.push(df.quad(nn(s), nn(p), nn(o)));
    return out;
}

/** DQ⁴ quads across named graphs: quad(ex#s, ex#p, ex#o, ex#g). */
export function genQuads(d: number): Quad[] {
    const out: Quad[] = [];
    for (let s = 0; s < d; s++)
        for (let p = 0; p < d; p++)
            for (let o = 0; o < d; o++)
                for (let g = 0; g < d; g++) out.push(df.quad(nn(s), nn(p), nn(o), nn(g)));
    return out;
}

/** A batch of fresh quads (disjoint namespace) for the add phase. */
export function genFresh(n: number): Quad[] {
    const out: Quad[] = [];
    for (let i = 0; i < n; i++) out.push(df.quad(nn('add-s' + i), nn('add-p'), nn('add-o' + i)));
    return out;
}

/** The first `n` triples `genTriples(d)` would produce, without materializing the
 * full D³ array — the mutation worker only needs a MUT_BATCH-sized slice for the
 * delete phase, and D³ can be a couple million quads. */
export function genTriplesPrefix(d: number, n: number): Quad[] {
    const out: Quad[] = [];
    for (let s = 0; s < d && out.length < n; s++)
        for (let p = 0; p < d && out.length < n; p++)
            for (let o = 0; o < d && out.length < n; o++) out.push(df.quad(nn(s), nn(p), nn(o)));
    return out;
}

// ─── Query patterns (probe terms fixed at index 0, so they always hit rows) ──
export type Pat = { name: string; s: Term | null; p: Term | null; o: Term | null; g: Term | null };
const t0 = nn(0); // simultaneously subject/predicate/object 0 in the rdf-stores namespace
const g0 = nn(0);
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
export const TRIPLE_PATTERNS: Pat[] = [
    { name: 'S', s: t0, p: null, o: null, g: null },
    { name: 'P', s: null, p: t0, o: null, g: null },
    { name: 'O', s: null, p: null, o: t0, g: null },
    { name: 'PO', s: null, p: t0, o: t0, g: null },
    { name: 'SPO', s: t0, p: t0, o: t0, g: null },
];
export const FULL_SCAN_PATTERN: Pat = { name: 'full', s: null, p: null, o: null, g: null };
export const QUAD_PATTERNS: Pat[] = [
    { name: 'G', s: null, p: null, o: null, g: g0 },
    { name: 'SPOG', s: t0, p: t0, o: t0, g: g0 },
];

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
export function reclaim(a: StoreAdapter, h: unknown): void {
    a.dispose?.(h);
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
