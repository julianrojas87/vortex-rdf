# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.5.0](https://github.com/vortex-rdf/vortex-rdf/compare/v0.4.0...v0.5.0) - 2026-07-29

### Added

- Add PyO3 bindings with an rdflib Store integration (python) ([`c851392`](https://github.com/vortex-rdf/vortex-rdf/commit/c8513929cd96ca1f2f0675812eb1c4faf50e6f49) by @julianrojas87)
- Port the DBBench harness from feat/cottas-bench (bench) ([`946211b`](https://github.com/vortex-rdf/vortex-rdf/commit/946211baebc485befd3a69f0e560d34499963651) by @julianrojas87)
- Push SPARQL BGP evaluation down into code space (python) ([`5c77b33`](https://github.com/vortex-rdf/vortex-rdf/commit/5c77b335d7e4b5b6ea6c29b39e8266c8e0f9a835) by @julianrojas87)
- Split the bindings into a standalone vortex-rdf package (python) ([`9e01f89`](https://github.com/vortex-rdf/vortex-rdf/commit/9e01f894d9293d0d1397cae635fe918ba2583b6f) by @julianrojas87)
- PyPi badges for published Python bindings ([`372b800`](https://github.com/vortex-rdf/vortex-rdf/commit/372b8005c9bddb5e2d31e703dab9073836878dc3) by @julianrojas87)

### Fixed

- Parse language-tagged and typed literals in parse_term (core) ([`618a7a8`](https://github.com/vortex-rdf/vortex-rdf/commit/618a7a83f67352b8924c5dcbcdd189ac8e624a05) by @julianrojas87)

## [0.4.0](https://github.com/vortex-rdf/vortex-rdf/compare/v0.3.0...v0.4.0) - 2026-07-29

### Added

- Store the term dictionary as a scannable column, not a list cell ([`30c0fa0`](https://github.com/vortex-rdf/vortex-rdf/commit/30c0fa042c5756d22a36148cc01ef146a467faff) by @julianrojas87)
- Choose the dictionary placement when writing files (core) ([`210db2e`](https://github.com/vortex-rdf/vortex-rdf/commit/210db2e06b807d2df90e937bbdd1d11a3caf73e6) by @julianrojas87)
- File-backed term dictionary with auto residency (core) ([`fb469c5`](https://github.com/vortex-rdf/vortex-rdf/commit/fb469c57fd618a9d223bff0020b38b2ae6de12d2) by @julianrojas87)
- Avoid action runs on docs-only commits ([`d620bb3`](https://github.com/vortex-rdf/vortex-rdf/commit/d620bb3af328e22541d088b1e0c45e1338fa0427) by @julianrojas87)

### Changed

- Cut wasm dictionary and ingest memory ([`2573626`](https://github.com/vortex-rdf/vortex-rdf/commit/25736260ef0bff113d34030cbe1efb16357ddb7a) by @julianrojas87)
- Hold and serialize the term dictionary FSST-compressed ([`75a9389`](https://github.com/vortex-rdf/vortex-rdf/commit/75a9389dbab2836d55b4a384e138c08917e927d3) by @julianrojas87)
- Share one term resolution across a match's index probes (core) ([`d442fff`](https://github.com/vortex-rdf/vortex-rdf/commit/d442ffffcc124c25ef17acdf30c60eac3432e663) by @julianrojas87)
- Stop pessimizing views a fast path already narrowed (core) ([`9d2ea13`](https://github.com/vortex-rdf/vortex-rdf/commit/9d2ea13ab24d7ee6217891965ba5716872d7fa76) by @julianrojas87)
- Memoize term to code lookups per dictionary (core) ([`8da6c31`](https://github.com/vortex-rdf/vortex-rdf/commit/8da6c312243e03522fc471c89335ab4504d463da) by @julianrojas87)
- Intern terms at ingest instead of buffering owned quads ([`276089d`](https://github.com/vortex-rdf/vortex-rdf/commit/276089de0675b9f02a9b340898177d43db514c28) by @julianrojas87)
- Seam the dictionary behind DictAccess with an async match prelude ([`7f06209`](https://github.com/vortex-rdf/vortex-rdf/commit/7f06209077bd75e03f73eed7d8ea43a17e554cb5) by @julianrojas87)
- Give shared types dedicated source modules (core) ([`5b6b867`](https://github.com/vortex-rdf/vortex-rdf/commit/5b6b867f9003664ee06d29ff1b86787b67471652) by @julianrojas87)
- Split the crate test suite by area (core) ([`7fae164`](https://github.com/vortex-rdf/vortex-rdf/commit/7fae164e1854b0826d5066a01f2ba69cdab6d813) by @julianrojas87)
- Split the wasm binding crate into modules (js) ([`c4dea32`](https://github.com/vortex-rdf/vortex-rdf/commit/c4dea32405405453062c38b3a6cb044a5f294477) by @julianrojas87)
- Share the RDF format-name table with core (js) ([`46a7668`](https://github.com/vortex-rdf/vortex-rdf/commit/46a76680078129b22e30c06853f2a82f3d2e30d6) by @julianrojas87)
- Derive the padded dictionary extent from footer statistics (core) ([`7f9eceb`](https://github.com/vortex-rdf/vortex-rdf/commit/7f9eceb9aadec3d0cb0c4cf74bc6e8591d5b3287) by @julianrojas87)
- Erase the retired _dict_terms format (core) ([`c947d1a`](https://github.com/vortex-rdf/vortex-rdf/commit/c947d1a8fc77d6b95fe6ab23d209b7d1c38d4ad9) by @julianrojas87)
- Fence-guided probes for the file-backed dictionary (core) ([`1720a44`](https://github.com/vortex-rdf/vortex-rdf/commit/1720a440d535b1b1fd216cb79640a2753d516171) by @julianrojas87)
- Compose typed objects without re-validating stored terms (core) ([`eb78570`](https://github.com/vortex-rdf/vortex-rdf/commit/eb78570b2af8fd0c23930da6f13d08b49ed663a5) by @julianrojas87)
- Materialize quads chunk-wise into an exactly-sized vec (core) ([`b1c01f3`](https://github.com/vortex-rdf/vortex-rdf/commit/b1c01f3cd78bf671721ab8b6ca01be6a1b0d8720) by @julianrojas87)

### Fixed

- Polluted CHANGELOG ([`b5951c0`](https://github.com/vortex-rdf/vortex-rdf/commit/b5951c04fe6527630e73cd2178f899fba1422f89) by @julianrojas87)

## [0.3.0](https://github.com/vortex-rdf/vortex-rdf/compare/v0.2.1...v0.3.0) - 2026-07-25

### Changed

- Implement StrColReader for faster string column reads ([`3813ce2`](https://github.com/vortex-rdf/vortex-rdf/commit/3813ce2d7087ef64d226f2742aea5491672c44d1) by @julianrojas87)
- Reuse payload buffer when serializing against disk ([`ab92cf4`](https://github.com/vortex-rdf/vortex-rdf/commit/ab92cf4aef3754e3061f3f2b395e6a6845da27be) by @julianrojas87)
- Decoding vortex into quads uses unchecked path ([`659273c`](https://github.com/vortex-rdf/vortex-rdf/commit/659273c591f7000494a5467e5113e4057b87a42a) by @julianrojas87)
- True zero-copy for IPC-based exchange ([`3a5d314`](https://github.com/vortex-rdf/vortex-rdf/commit/3a5d314b2e91993285e17c15af3b307b496c913b) by @julianrojas87)
- Serialize to disk only when needed for stream builders ([`a2ab133`](https://github.com/vortex-rdf/vortex-rdf/commit/a2ab1333ad22afa1444f8656ffdd0c49e5e07fec) by @julianrojas87)

### Fixed

- WASM compilation time (from 15m to 40s) ([`c7b0225`](https://github.com/vortex-rdf/vortex-rdf/commit/c7b0225d3882fc36ca6672604c28f65bf42ea4d0) by @julianrojas87)
- Changelog update process ([`1702cef`](https://github.com/vortex-rdf/vortex-rdf/commit/1702cef296cc90c50fcdaf944b1be9c0fc8a4cc5) by @julianrojas87)

## [0.2.1] - 2026-07-25

### Added

- Add proper codspeed benchmark that uploads (js) ([`320bb5b`](https://github.com/vortex-rdf/vortex-rdf/commit/320bb5b030f80f0c94d5eed94fcb002026df57ad) by @julianrojas87).
- Add CHANGELOG management scripts ([`e7ce0f7`](https://github.com/vortex-rdf/vortex-rdf/commit/e7ce0f7cb3fa94b0433e136b218c5fd13282b390) by @julianrojas87).

### Changed

- Enable the `+simd128` target feature for WASM builds ([`6e8e8b6`](https://github.com/vortex-rdf/vortex-rdf/commit/6e8e8b6d77be89d05431239444595092fdf9bd72) by @julianrojas87).

### Removed

- Remove the format-specific `nquads_to_vortex` / `vortex_to_nquads` helpers from
  the JS/WASM bindings ([`12c58c7`](https://github.com/vortex-rdf/vortex-rdf/commit/12c58c7602f2afff6fb77657d5ab5d39f13b573e) by @julianrojas87).

### Fixed

- Scale of JS benchmark bar charts ([`606a08f`](https://github.com/vortex-rdf/vortex-rdf/commit/606a08f1139ea46bf42686955582b1c232690fc6) by @julianrojas87).

## [0.2.0] - 2026-07-24

### Changed

- Major performance improvements to the JS/WASM bindings: ingestion quads are
  packed into a single byte buffer before crossing into WASM, pattern matching
  runs directly over column buffers, quads serialize via unchecked IRI builders,
  and chained filters match over already-filtered row selections — roughly
  halving serialization cost ([`c777712`](https://github.com/vortex-rdf/vortex-rdf/commit/c7777125cd07aa101bf58e10ce9bd62502c76639) by @julianrojas87).

### Removed

- `init_panic_hook` is no longer part of the WASM export surface ([`b1512c0`](https://github.com/vortex-rdf/vortex-rdf/commit/b1512c0533485947725f2318078cce98c895d4e6) by @julianrojas87).

## [0.1.0](https://github.com/vortex-rdf/vortex-rdf/releases/tag/v0.1.0) - 2026-07-21

Initial release ([79 commits](https://github.com/vortex-rdf/vortex-rdf/commits/v0.1.0) by @julianrojas87). The entries below summarise features built across
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
