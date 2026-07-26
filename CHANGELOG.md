# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased](https://github.com/vortex-rdf/vortex-rdf/compare/v0.2.1...HEAD)

### Changed

- Implement StrColReader for faster string column reads ([`3813ce2`](https://github.com/vortex-rdf/vortex-rdf/commit/3813ce2d7087ef64d226f2742aea5491672c44d1) by julianrojas87)
- Reuse payload buffer when serializing against disk ([`ab92cf4`](https://github.com/vortex-rdf/vortex-rdf/commit/ab92cf4aef3754e3061f3f2b395e6a6845da27be) by julianrojas87)
- Decoding vortex into quads uses unchecked path ([`659273c`](https://github.com/vortex-rdf/vortex-rdf/commit/659273c591f7000494a5467e5113e4057b87a42a) by julianrojas87)
- True zero-copy for IPC-based exchange ([`3a5d314`](https://github.com/vortex-rdf/vortex-rdf/commit/3a5d314b2e91993285e17c15af3b307b496c913b) by julianrojas87)
- Serialize to disk only when needed for stream builders ([`a2ab133`](https://github.com/vortex-rdf/vortex-rdf/commit/a2ab1333ad22afa1444f8656ffdd0c49e5e07fec) by julianrojas87)

### Fixed

- WASM compilation time (from 15m to 40s) ([`c7b0225`](https://github.com/vortex-rdf/vortex-rdf/commit/c7b0225d3882fc36ca6672604c28f65bf42ea4d0) by julianrojas87)
- Changelog update process ([`1702cef`](https://github.com/vortex-rdf/vortex-rdf/commit/1702cef296cc90c50fcdaf944b1be9c0fc8a4cc5) by julianrojas87)

## [0.3.0](https://github.com/vortex-rdf/vortex-rdf/compare/v0.2.1...v0.3.0) - 2026-07-25

### Changed

- Implement StrColReader for faster string column reads ([`3813ce2`](https://github.com/vortex-rdf/vortex-rdf/commit/3813ce2d7087ef64d226f2742aea5491672c44d1) by julianrojas87)
- Reuse payload buffer when serializing against disk ([`ab92cf4`](https://github.com/vortex-rdf/vortex-rdf/commit/ab92cf4aef3754e3061f3f2b395e6a6845da27be) by julianrojas87)
- Decoding vortex into quads uses unchecked path ([`659273c`](https://github.com/vortex-rdf/vortex-rdf/commit/659273c591f7000494a5467e5113e4057b87a42a) by julianrojas87)
- True zero-copy for IPC-based exchange ([`3a5d314`](https://github.com/vortex-rdf/vortex-rdf/commit/3a5d314b2e91993285e17c15af3b307b496c913b) by julianrojas87)
- Serialize to disk only when needed for stream builders ([`a2ab133`](https://github.com/vortex-rdf/vortex-rdf/commit/a2ab1333ad22afa1444f8656ffdd0c49e5e07fec) by julianrojas87)

### Fixed

- WASM compilation time (from 15m to 40s) ([`c7b0225`](https://github.com/vortex-rdf/vortex-rdf/commit/c7b0225d3882fc36ca6672604c28f65bf42ea4d0) by julianrojas87)
- Changelog update process ([`1702cef`](https://github.com/vortex-rdf/vortex-rdf/commit/1702cef296cc90c50fcdaf944b1be9c0fc8a4cc5) by julianrojas87)

## [0.3.0](https://github.com/vortex-rdf/vortex-rdf/compare/v0.2.1...v0.3.0) - 2026-07-25

### Changed

- Implement StrColReader for faster string column reads ([`3813ce2`](https://github.com/vortex-rdf/vortex-rdf/commit/3813ce2d7087ef64d226f2742aea5491672c44d1) by [@julianrojas87](https://github.com/julianrojas87))

- Reuse payload buffer when serializing against disk ([`ab92cf4`](https://github.com/vortex-rdf/vortex-rdf/commit/ab92cf4aef3754e3061f3f2b395e6a6845da27be) by [@julianrojas87](https://github.com/julianrojas87))

- Decoding vortex into quads uses unchecked path ([`659273c`](https://github.com/vortex-rdf/vortex-rdf/commit/659273c591f7000494a5467e5113e4057b87a42a) by [@julianrojas87](https://github.com/julianrojas87))

- True zero-copy for IPC-based exchange ([`3a5d314`](https://github.com/vortex-rdf/vortex-rdf/commit/3a5d314b2e91993285e17c15af3b307b496c913b) by [@julianrojas87](https://github.com/julianrojas87))

- Serialize to disk only when needed for stream builders ([`a2ab133`](https://github.com/vortex-rdf/vortex-rdf/commit/a2ab1333ad22afa1444f8656ffdd0c49e5e07fec) by [@julianrojas87](https://github.com/julianrojas87))


### Fixed

- WASM compilation time (from 15m to 40s) ([`c7b0225`](https://github.com/vortex-rdf/vortex-rdf/commit/c7b0225d3882fc36ca6672604c28f65bf42ea4d0) by [@julianrojas87](https://github.com/julianrojas87))

## [0.2.1] - 2026-07-25

### Added

- Add proper codspeed benchmark that uploads (js) ([`320bb5b`](https://github.com/vortex-rdf/vortex-rdf/commit/320bb5b030f80f0c94d5eed94fcb002026df57ad) by [@julianrojas87](https://github.com/julianrojas87)).
- Add CHANGELOG management scripts ([`e7ce0f7`](https://github.com/vortex-rdf/vortex-rdf/commit/e7ce0f7cb3fa94b0433e136b218c5fd13282b390) by [@julianrojas87](https://github.com/julianrojas87)).

### Changed

- Enable the `+simd128` target feature for WASM builds ([`6e8e8b6`](https://github.com/vortex-rdf/vortex-rdf/commit/6e8e8b6d77be89d05431239444595092fdf9bd72) by [@julianrojas87](https://github.com/julianrojas87)).

### Removed

- Remove the format-specific `nquads_to_vortex` / `vortex_to_nquads` helpers from
  the JS/WASM bindings ([`12c58c7`](https://github.com/vortex-rdf/vortex-rdf/commit/12c58c7602f2afff6fb77657d5ab5d39f13b573e) by [@julianrojas87](https://github.com/julianrojas87)).

### Fixed

- Scale of JS benchmark bar charts ([`606a08f`](https://github.com/vortex-rdf/vortex-rdf/commit/606a08f1139ea46bf42686955582b1c232690fc6) by [@julianrojas87](https://github.com/julianrojas87)).

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
