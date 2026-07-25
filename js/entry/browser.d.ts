// Types for the browser/bundler entry point: identical public surface to the
// Node entry (they differ only in how the wasm bytes are loaded at runtime).
export {
    VortexRdfStore,
    rdf_to_vortex,
    vortex_to_rdf,
} from '../pkg/web/vortex_rdf.js';
export type {
    BuildOptions,
    BuildOptionsInput,
    BuilderStrategy,
    LayoutStrategy,
    IndexType,
    RdfFormatName,
} from '../pkg/web/vortex_rdf.js';
