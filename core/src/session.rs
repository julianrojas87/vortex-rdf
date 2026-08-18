//! Crate-wide Vortex session infrastructure. Lives at the crate root,
//! executing *any* Vortex kernel — an in-memory decode as much as a
//! file scan — needs the session's registries.

use std::sync::LazyLock;

use vortex_array::scalar_fn::session::ScalarFnSession;
use vortex_array::session::ArraySession;
use vortex_io::session::RuntimeSession;
use vortex_layout::session::LayoutSession;
use vortex_session::VortexSession;

#[cfg(any(
    all(feature = "file-io", not(target_arch = "wasm32")),
    all(target_arch = "wasm32", target_os = "unknown")
))]
use vortex_io::session::RuntimeSessionExt;

/// The one Vortex session: arrays, layouts, scalar kernels, and a runtime.
///
/// Every target reads and writes Vortex *files* (the wasm bindings exchange
/// file bytes via `open_buffer`/`to_bytes`), so every target needs the same
/// registries — a single session keeps the encoding registry from diverging
/// between targets. The runtime handle is the only per-target piece: tokio
/// natively, the microtask-queue `WasmRuntime` on wasm (required by the file
/// writer's task spawning), and none for native no-file-io builds, whose code
/// paths are all handle-free.
pub(crate) static VORTEX_SESSION: LazyLock<VortexSession> = LazyLock::new(|| {
    let session = VortexSession::empty()
        .with::<ArraySession>()
        .with::<LayoutSession>()
        .with::<ScalarFnSession>()
        .with::<RuntimeSession>();
    #[cfg(all(feature = "file-io", not(target_arch = "wasm32")))]
    let session = session.with_tokio();
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    let session = session.with_handle(vortex_io::runtime::wasm::WasmRuntime::handle());
    vortex_file::register_default_encodings(&session);
    crate::io::container::register(&session);
    enable_store_edition(&session);
    session
});

/// Enroll every registered encoding in one enabled edition. The file writer
/// refuses encodings outside the session's enabled editions (reading is
/// covered by registration alone), and this store's wire contract is
/// "whatever the registered compressor emits" — not a vortex edition
/// boundary — so the edition spans the full registry.
fn enable_store_edition(session: &VortexSession) {
    use vortex_array::session::ArraySessionExt as _;
    use vortex_edition::{Edition, EditionId, EditionInclusion, EditionSessionExt as _};
    use vortex_error::{VortexExpect as _, vortex_err};

    const STORE_EDITION: EditionId = EditionId::new("vortexrdf", 2026, 8, 0);

    let editions = session.editions();
    editions
        .declare_edition(Edition {
            id: STORE_EDITION,
            min_vortex_version: None,
        })
        .map_err(|error| vortex_err!("{error}"))
        .vortex_expect("the store edition is valid");
    let ids = session
        .arrays()
        .registry()
        .read(|map| map.keys().copied().collect::<Vec<_>>());
    for id in ids {
        editions
            .declare_inclusion(EditionInclusion::new(&id, STORE_EDITION))
            .map_err(|error| vortex_err!("{error}"))
            .vortex_expect("every registered encoding joins the store edition once");
    }
    session
        .enable_edition(STORE_EDITION)
        .map_err(|error| vortex_err!("{error}"))
        .vortex_expect("the store edition was just declared");
}
