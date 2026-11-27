#[cfg(target_os = "linux")]
use std::ffi::CString;
#[cfg(target_os = "linux")]
use std::os::unix::ffi::OsStrExt;
#[cfg(target_os = "linux")]
use std::path::Path;

#[cfg(target_os = "linux")]
use anyhow::{Context, Result};

#[cfg(target_os = "linux")]
mod linux_publish_swap {
    use std::ffi::{c_char, c_int, c_uint};

    pub const AT_FDCWD: c_int = -100;
    pub const RENAME_EXCHANGE: c_uint = 0x2;
    /// Linux's `<errno.h>` definition (`EINVAL = 22`). We don't depend on
    /// `libc` directly — `errno.h` has guaranteed this value on every
    /// glibc/musl architecture since the kernel was renumbered, and
    /// pulling in `libc` for one constant inflates compile time.
    /// Cross-checked against `man 2 renameat2` (returns EINVAL when the
    /// underlying filesystem doesn't support `RENAME_EXCHANGE`).
    pub const EINVAL: i32 = 22;

    // SAFETY: a raw libc symbol declaration; the only caller is
    // `atomic_exchange_paths`, which documents its argument invariants.
    #[allow(unsafe_code)]
    unsafe extern "C" {
        pub fn renameat2(
            olddirfd: c_int,
            oldpath: *const c_char,
            newdirfd: c_int,
            newpath: *const c_char,
            flags: c_uint,
        ) -> c_int;
    }
}

#[cfg(target_os = "linux")]
fn path_to_cstring(path: &Path) -> Result<CString> {
    CString::new(path.as_os_str().as_bytes()).with_context(|| {
        format!(
            "encoding filesystem path for Linux atomic lexical publish swap: {}",
            path.display()
        )
    })
}

#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
pub(crate) fn atomic_exchange_paths(left: &Path, right: &Path) -> Result<()> {
    let left_c = path_to_cstring(left)?;
    let right_c = path_to_cstring(right)?;
    // SAFETY: both paths are valid NUL-terminated C strings that outlive the
    // call, AT_FDCWD is a valid dirfd, and renameat2 reads nothing else.
    let result = unsafe {
        linux_publish_swap::renameat2(
            linux_publish_swap::AT_FDCWD,
            left_c.as_ptr(),
            linux_publish_swap::AT_FDCWD,
            right_c.as_ptr(),
            linux_publish_swap::RENAME_EXCHANGE,
        )
    };
    if result == 0 {
        return Ok(());
    }

    Err(std::io::Error::last_os_error()).with_context(|| {
        format!(
            "atomically exchanging lexical publish paths {} <-> {}",
            left.display(),
            right.display()
        )
    })
}

#[cfg(target_os = "linux")]
pub(crate) fn linux_atomic_exchange_is_unsupported(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .and_then(std::io::Error::raw_os_error)
            == Some(linux_publish_swap::EINVAL)
    })
}
