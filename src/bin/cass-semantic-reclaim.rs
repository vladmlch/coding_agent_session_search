//! Compatibility spelling for the offline recovery command shipped in `cass`.
#![forbid(unsafe_code)]

#[path = "../semantic_reclaim_cli.rs"]
pub mod semantic_reclaim_cli;

fn main() -> std::process::ExitCode {
    coding_agent_search::ensure_runtime_hooks_registered();
    std::process::ExitCode::from(semantic_reclaim_cli::run_standalone(
        std::env::args_os().collect(),
    ))
}
