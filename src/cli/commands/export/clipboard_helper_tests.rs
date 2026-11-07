use crate::cli::commands::export::indexed_export_cli_error::copy_to_system_clipboard;

/// On a host with **no** clipboard tool on PATH (PATH=""), the helper
/// must return Err so the caller can fall back to stdout. The
/// previous failure mode was a silent success that dropped the
/// export on the floor.
#[test]
pub(crate) fn returns_err_when_no_tool_is_available() {
    // Save and clear PATH so spawn() can't find pbcopy/wl-copy/xclip
    // /xsel/clip. Restore on exit even if the assertion panics.
    struct PathGuard(Option<std::ffi::OsString>);
    impl Drop for PathGuard {
        fn drop(&mut self) {
            // Tests touch process env: required for an end-to-end
            // check of the spawn fallback chain.
            unsafe {
                match self.0.take() {
                    Some(prev) => std::env::set_var("PATH", prev),
                    None => std::env::remove_var("PATH"),
                }
            }
        }
    }
    let guard = PathGuard(std::env::var_os("PATH"));
    unsafe {
        std::env::set_var("PATH", "");
    }

    let err = copy_to_system_clipboard("hello")
        .expect_err("with PATH cleared, no clipboard tool should be findable");
    assert!(
        err.contains("no clipboard tool") || err.contains("tried"),
        "error should describe what was tried, got: {err}"
    );

    drop(guard);
}
