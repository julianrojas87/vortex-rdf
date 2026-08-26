//! Debug-only timing helpers for `log::debug!` instrumentation: every timed
//! log line in this crate (and in the binding crates instrumenting around
//! it) starts its timer with [`timer`] and reads it with [`elapsed`], so
//! timing costs nothing when debug logging is off.

use std::time::Duration;
use web_time::Instant;

/// Start a timer only when debug logging will read it: on wasm
/// `Instant::now()` is a JS-boundary `performance.now()` call, so hot paths
/// must not pay for it when the log line is discarded anyway.
pub fn timer() -> Option<Instant> {
    log::log_enabled!(log::Level::Debug).then(Instant::now)
}

/// The elapsed time on a [`timer`] — zero when the timer was never started
/// (debug logging off, in which case `log::debug!` discards the value without
/// formatting it).
pub fn elapsed(t: Option<Instant>) -> Duration {
    t.map_or(Duration::ZERO, |started| started.elapsed())
}
