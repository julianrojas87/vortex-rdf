//! The tokio runtime the bench targets drive their async calls through.
//! Included by `support/mod.rs` and, via `#[path]`, by `compare.rs`.

use std::sync::OnceLock;

use tokio::runtime::Runtime;

/// The one tokio runtime every bench drives its async calls through.
pub fn rt() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| Runtime::new().unwrap())
}
