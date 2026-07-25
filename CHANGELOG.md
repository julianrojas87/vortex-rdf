# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased](https://github.com/vortex-rdf/vortex-rdf/compare/v0.1.0...HEAD)

<!-- hand-written: entries here are curated by hand and preserved by
     scripts/update-changelog.sh. Remove this comment to let git-cliff generate
     the [Unreleased] section from Conventional Commits instead. -->

### Changed

- Enable the `+simd128` target feature for WASM builds ([`6e8e8b6`](https://github.com/vortex-rdf/vortex-rdf/commit/6e8e8b6d77be89d05431239444595092fdf9bd72) by [@julianrojas87](https://github.com/julianrojas87)).

### Removed

- Remove the format-specific `nquads_to_vortex` / `vortex_to_nquads` helpers from
  the JS/WASM bindings ([`12c58c7`](https://github.com/vortex-rdf/vortex-rdf/commit/12c58c7602f2afff6fb77657d5ab5d39f13b573e) by [@julianrojas87](https://github.com/julianrojas87)).

## [0.2.0] - 2026-07-24

### Changed

- Major performance improvements to the JS/WASM bindings: ingestion quads are
  packed into a single byte buffer before crossing into WASM, pattern matching
  runs directly over column buffers, quads serialize via unchecked IRI builders,
  and chained filters match over already-filtered row selections — roughly
  halving serialization cost ([`c777712`](https://github.com/vortex-rdf/vortex-rdf/commit/c7777125cd07aa101bf58e10ce9bd62502c76639) by [@julianrojas87](https://github.com/julianrojas87)).

### Removed

- `init_panic_hook` is no longer part of the WASM export surface ([`b1512c0`](https://github.com/vortex-rdf/vortex-rdf/commit/b1512c0533485947725f2318078cce98c895d4e6) by [@julianrojas87](https://github.com/julianrojas87)).

## [0.1.0](https://github.com/vortex-rdf/vortex-rdf/releases/tag/v0.1.0) - 2026-07-21

Initial release ([79 commits](https://github.com/vortex-rdf/vortex-rdf/commits/v0.1.0) by [@julianrojas87](https://github.com/julianrojas87)). The entries below summarise features built across
many commits, so they are not attributed individually.

### Added

- `vortex-rdf-core`: a columnar RDF quad store built on [Vortex](https://docs.vortex.dev),
  with serialization to/from `.vortex` files and Vortex IPC streams.
- Three column layouts — `Default`, `TypedObject`, and `Dictionary` — trading off
  compression strategy and query characteristics.
- Two secondary index types — `SecondaryByReference` and `SecondaryByCopy` — for
  accelerating pattern matching beyond the primary sort order.
- Three ingestion builders — `UnsortedStream`, `SortedInMemory`, and `SortedStream`
  (out-of-core, spill-to-disk) — for building a store from a quad stream.
- `VortexRdfStore` query API: pattern matching, mutation (add/delete), and
  compaction, with row selections composing over both in-memory and file-backed
  stores.
- `vortex-rdf-cli`: a command-line interface for converting between RDF formats
  and Vortex-RDF, and for querying `.vortex` files.
- `vortex-rdf` (npm): WebAssembly bindings exposing a `VortexStore` with an
  RDF-JS-compatible `DatasetCore` interface.
