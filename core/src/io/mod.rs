pub(crate) mod export;
pub(crate) mod native_file;
/// Every item in `ser` is write-side native-container machinery, compiled only
/// where a store can be written: natively behind `file-io`, and on wasm (whose
/// bindings exchange file bytes).
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
pub(crate) mod ser;
pub(crate) mod store_layout;

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
    store_layout::register(&session);
    session
});

pub use export::deserialize;

#[cfg(feature = "file-io")]
pub use ser::{
    quads_stream_to_vortex_file_with_builder, quads_stream_to_vortex_writer_with_builder,
};

/// The value-level [`BuilderStrategy`](crate::store::BuilderStrategy) twins of
/// the `*_with_builder` entry points above.
#[cfg(feature = "file-io")]
pub use ser::{quads_stream_to_vortex_file, quads_stream_to_vortex_writer_with_strategy};
