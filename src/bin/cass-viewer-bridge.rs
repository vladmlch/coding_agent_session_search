//! Additive, read-only Viewer adapter; the existing `cass` binary is unchanged.
#![forbid(unsafe_code)]

#[path = "../viewer_bridge/mod.rs"]
mod viewer_bridge;

fn main() -> std::process::ExitCode {
    viewer_bridge::run(std::env::args_os().collect())
}
