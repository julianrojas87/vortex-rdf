import { Quad, Term, Stream } from '@rdfjs/types';

/**
 * How quads are ordered while the columnar array is built.
 * - 'Unsorted': natural insertion order. Cheapest to build, but every `match`
 *   falls back to a full column scan.
 * - 'Sorted': global in-memory sort by subject -> predicate -> object -> graph.
 *   Costs a sort at build time, but unlocks binary-search lookups on subject.
 *
 * The core's out-of-core 'SortedStream' builder is not available here: it
 * spills sorted runs to disk, which WebAssembly has no access to.
 */
export type BuilderStrategy = 'Unsorted' | 'Sorted';

/**
 * How quad terms are encoded into columns.
 * - 'Default': all four terms as N-Triples strings natively optimised by Vortex.
 * - 'TypedObject': the object is split into kind/value/datatype/language columns.
 * - 'Dictionary': every term is replaced by a u32 code into a global sorted term
 *   dictionary. More compact than 'Default'. Added quads live in an
 *   in-memory string tail until the store is serialized or compacted.
 */
export type LayoutStrategy = 'Default' | 'TypedObject' | 'Dictionary';

/**
 * Secondary indexes embedded alongside the primary quad columns.
 * 'SecondaryByReference' adds sorted predicate/object columns plus row-id
 * back-references, letting predicate-only and object-only patterns use a
 * binary search instead of a full scan.
 * 'SecondaryByCopy' embeds two complete extra copies of the quad columns —
 * one sorted by (p, o, s, g), one by (o, s, p, g) — so predicate- and
 * object-bound patterns (including predicate+object prefix lookups) get the
 * same sorted access path subjects have, at ~2x the storage.
 * Both are only effective with a 'Sorted' builder.
 */
export type IndexType = 'SecondaryByReference' | 'SecondaryByCopy';

/** RDF syntaxes accepted for parsing and emitted for serialization. */
export type RdfFormatName =
    | 'nt' | 'ntriples'
    | 'nq' | 'nquads'
    | 'ttl' | 'turtle'
    | 'trig'
    | 'n3'
    | 'rdf' | 'rdfxml' | 'xml'
    | 'jsonld';

/** Build-time configuration. Any omitted field keeps its default. */
export interface BuildOptions {
    /** @default 'Unsorted' */
    builder?: BuilderStrategy;
    /** @default 'Dictionary' */
    layout?: LayoutStrategy;
    /** @default [] */
    indexes?: IndexType[];
}

/** A bare BuilderStrategy string is accepted as shorthand for `{ builder }`. */
export type BuildOptionsInput = BuildOptions | BuilderStrategy;

export class VortexRdfStore {
    static empty(): VortexRdfStore;
    static fromBytes(bytes: Uint8Array): Promise<VortexRdfStore>;
    static fromString(input: string, format: RdfFormatName, options?: BuildOptionsInput): Promise<VortexRdfStore>;
    /** `quads` may be an array, or an RDF/JS `Stream<Quad>` (a Node-style event emitter). */
    static fromQuads(quads: Quad[] | Stream<Quad>, options?: BuildOptionsInput): Promise<VortexRdfStore>;

    /** The layout this store's columns are encoded with. */
    layout(): LayoutStrategy;
    size(): Promise<number>;
    has(quad: Quad): Promise<boolean>;
    /** Add one quad in place (a quad already present is ignored, per RDF/JS). */
    addQuad(quad: Quad): Promise<void>;
    /**
     * Add many quads in one call — one tail rebuild for the whole batch,
     * where a loop over addQuad pays one per quad.
     */
    addQuads(quads: Quad[]): Promise<void>;
    deleteQuad(quad: Quad): Promise<void>;
    /**
     * Stream the quads matching a pattern (the RDF/JS `Source.match` contract).
     * Pass `null`/`undefined` for a variable position. Returns **synchronously**
     * an RDF/JS `Stream<Quad>` (`.on('data'|'end'|'error', …)`, `.read()`) of
     * lazy `Quad`s: a term's string is decoded from the columnar data only when
     * its `.value`/`.termType` is read, and never eagerly. The stream also
     * implements `Symbol.asyncIterator`, so it can be consumed with `for await`
     * (cast to `AsyncIterable<Quad>` in typed code).
     */
    match(subject?: Term | null, predicate?: Term | null, object?: Term | null, graph?: Term | null): Stream<Quad>;
    /**
     * Materialize the quads matching a pattern into an array of lazy `Quad`s —
     * the array-returning counterpart of `match`. `async` (returns a `Promise`)
     * because resolving the match crosses the WebAssembly boundary; the returned
     * `Quad`s still decode their term strings lazily on access.
     */
    getQuads(subject?: Term | null, predicate?: Term | null, object?: Term | null, graph?: Term | null): Promise<Quad[]>;
    /**
     * Low-level prototype: an alternative read path to `match`/`getQuads`,
     * which build their own columnar payload rather than going through this.
     * Resolves a pattern to the matched rows' raw u32 term codes — four
     * columnar `Uint32Array`s — without materializing any term strings;
     * resolve codes to terms with `decodeTerm`. Returns `null` unless the
     * store is Dictionary layout with no pending appends (appended quads are
     * encoded against a fresh dictionary, so their codes would not decode).
     */
    matchCodes(subject?: Term | null, predicate?: Term | null, object?: Term | null, graph?: Term | null): Promise<{ s: Uint32Array; p: Uint32Array; o: Uint32Array; g: Uint32Array; length: number } | null>;
    /** Low-level. Decode a Dictionary-layout term code to its N-Triples term string. */
    decodeTerm(code: number): string | undefined;
    /** Low-level. Encode an N-Triples term string to its Dictionary-layout code (inverse of decodeTerm). */
    encodeTerm(term: string): number | undefined;
    /** Serialize to Vortex file bytes; read back with `VortexRdfStore.fromBytes` or write to disk as a `.vortex` file. */
    toBytes(): Promise<Uint8Array>;
    /** Serialize the quads to an RDF syntax. */
    toRdf(format: RdfFormatName): Promise<string>;
}

export function rdf_to_vortex(input: string, format: RdfFormatName, options?: BuildOptionsInput): Promise<Uint8Array>;
export function vortex_to_rdf(vortex_bytes: Uint8Array, format: RdfFormatName): Promise<string>;
