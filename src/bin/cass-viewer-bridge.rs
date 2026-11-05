//! Additive Viewer adapter; the existing `cass` binary is unchanged.
//! Strictly read-only by default; the narrow `cass_owned_queries_v1` write
//! lane requires the explicit local `--enable-viewer-writes` opt-in, and
//! the Task-12 committed-change push channel requires the explicit local
//! `--enable-viewer-events` opt-in (loopback TCP listener plus a
//! `<db>.viewer_events.addr` endpoint sidecar; journal replay is truth).
#![forbid(unsafe_code)]

#[path = "../viewer_bridge/mod.rs"]
mod viewer_bridge;

fn main() -> std::process::ExitCode {
    viewer_bridge::run(std::env::args_os().collect())
}
