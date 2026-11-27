//! Execute the shipped Pages database queries against Node's real SQLite/FTS5.
//! This covers query semantics, not the browser DOM or WASM loader.

use std::process::Command;

mod ws_paths_1c;

#[test]
fn pages_database_queries_against_real_sqlite() {
    let script =
        ws_paths_1c::workspace_root().join("tests/js/pages_database_queries.test.mjs");
    let output = Command::new("node")
        .args(["--experimental-vm-modules", "--test"])
        .arg(script)
        .output()
        .expect("Node.js 22+ with node:sqlite is required for the Pages query regression tests");
    assert!(
        output.status.success(),
        "Pages database query regressions failed ({}):\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
