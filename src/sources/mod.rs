//! Source configuration and provenance for cass.
//!
//! This module defines the local source model. In the local-only build,
//! `type = "local"` entries name extra directories indexed in place, and
//! historical `type = "ssh"` entries survive as inert archive records whose
//! previously mirrored conversations stay searchable and filterable. Remote
//! synchronization (SSH/SFTP/rsync/Tailscale) was removed; nothing in this
//! module opens a connection.
//!
//! # Architecture
//!
//! - **config**: Configuration types for sources (`sources.toml`)
//! - **provenance**: Types for tracking conversation origins, including the
//!   byte-stable archive directory naming old mirrors depend on
//!
//! # Configuration
//!
//! Sources are configured in `~/.config/cass/sources.toml`:
//!
//! ```toml
//! [[sources]]
//! name = "archive-box"
//! type = "local"
//! paths = ["/Volumes/old-sessions"]
//! ```
//!
//! # Provenance
//!
//! Each conversation tracks where it came from via [`provenance::Origin`]:
//!
//! ```rust,ignore
//! use coding_agent_search::sources::provenance::{Origin, SourceKind};
//!
//! // Local conversation
//! let local = Origin::local();
//!
//! // Archived remote conversation
//! let remote = Origin::remote("work-laptop");
//! ```

#[cfg(unix)]
mod child_output;

pub mod config;
pub(crate) mod config_validation;
pub mod provenance;

#[cfg(not(unix))]
use std::io::Read as IoRead;
use std::process::{Child, Command, Output};
#[cfg(not(unix))]
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
#[cfg(not(unix))]
use std::thread::JoinHandle;
use std::time::Duration;
#[cfg(not(unix))]
use std::time::Instant;

#[cfg(not(unix))]
use wait_timeout::ChildExt;

#[cfg(not(unix))]
struct ChildPipeReader {
    receiver: Receiver<std::io::Result<Vec<u8>>>,
    handle: JoinHandle<()>,
}

#[cfg(not(unix))]
fn drain_child_pipe<R>(mut pipe: R, max_bytes: Option<usize>) -> ChildPipeReader
where
    R: IoRead + Send + 'static,
{
    let (sender, receiver) = mpsc::channel();
    let handle = std::thread::spawn(move || {
        let mut output = Vec::new();
        let result = (|| {
            if let Some(limit) = max_bytes {
                pipe.by_ref()
                    .take((limit as u64).saturating_add(1))
                    .read_to_end(&mut output)?;
                if output.len() > limit {
                    // Keep draining so a writer cannot deadlock on a full pipe.
                    // The caller's deadline still terminates a non-exiting child.
                    std::io::copy(&mut pipe, &mut std::io::sink())?;
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "child output exceeded the configured byte limit",
                    ));
                }
            } else {
                pipe.read_to_end(&mut output)?;
            }
            Ok(output)
        })();
        let _ = sender.send(result);
    });
    ChildPipeReader { receiver, handle }
}

#[cfg(not(unix))]
fn finish_child_pipe(
    pipe_reader: Option<ChildPipeReader>,
    deadline: Instant,
) -> std::io::Result<Option<Vec<u8>>> {
    match pipe_reader {
        Some(reader) => {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .unwrap_or(Duration::ZERO);
            match reader.receiver.recv_timeout(remaining) {
                Ok(result) => {
                    reader
                        .handle
                        .join()
                        .map_err(|_| std::io::Error::other("child pipe reader panicked"))?;
                    result.map(Some)
                }
                Err(RecvTimeoutError::Timeout) => Ok(None),
                Err(RecvTimeoutError::Disconnected) => {
                    let reader_panicked = reader.handle.join().is_err();
                    let message = if reader_panicked {
                        "child pipe reader panicked before sending output"
                    } else {
                        "child pipe reader disconnected before sending output"
                    };
                    Err(std::io::Error::new(std::io::ErrorKind::BrokenPipe, message))
                }
            }
        }
        None => Ok(Some(Vec::new())),
    }
}

#[cfg(unix)]
pub(crate) fn configure_child_process_group(cmd: &mut Command) {
    use std::os::unix::process::CommandExt;

    cmd.process_group(0);
}

#[cfg(not(unix))]
pub(crate) fn configure_child_process_group(_cmd: &mut Command) {}

#[cfg(unix)]
#[allow(unsafe_code)]
fn kill_child_process_group(pid: u32) {
    let Ok(pid) = i32::try_from(pid) else {
        return;
    };
    if pid <= 1 {
        return;
    }
    // SAFETY: a strictly positive owned child PID is negated to address only
    // its process group, never PID 0 or -1. The Unix collector retains that
    // child unreaped until cleanup; kill takes no pointers. Avoid spawning
    // another process during timeout/error cleanup.
    let _ = unsafe { libc::kill(-pid, libc::SIGKILL) };
}

#[cfg(not(unix))]
fn kill_child_process_group(_pid: u32) {}

/// Wait for a child process while draining stdout/stderr without letting either
/// process execution or pipe collection outlive the same wall-clock deadline.
///
/// Callers should pass children configured with
/// [`configure_child_process_group`] before `spawn()`. Without that, the direct
/// child can be killed but shell grandchildren may keep inherited pipe FDs open
/// until they exit naturally.
///
/// Limit each captured stream while retaining the same process deadline.
/// Unix capture owns nonblocking pipes directly: overflow stops immediately,
/// and cancellation cannot leave background reader threads behind. Process
/// teardown/reaping itself can exceed the budget if the OS stalls.
#[cfg(unix)]
pub(crate) fn wait_for_child_output_with_limit(
    child: Child,
    timeout: Duration,
    max_bytes: Option<usize>,
) -> std::io::Result<Option<Output>> {
    child_output::wait(child, timeout, max_bytes)
}

// Retain the existing non-Unix pipe implementation until native pipe
// cancellation is available there; do not claim Unix ownership guarantees.
#[cfg(not(unix))]
pub(crate) fn wait_for_child_output_with_limit(
    mut child: Child,
    timeout: Duration,
    max_bytes: Option<usize>,
) -> std::io::Result<Option<Output>> {
    let timeout = if timeout.is_zero() {
        Duration::from_secs(1)
    } else {
        timeout
    };
    let start = Instant::now();
    let deadline = start.checked_add(timeout).unwrap_or(start);
    let child_pid = child.id();
    let stdout_reader = child
        .stdout
        .take()
        .map(|pipe| drain_child_pipe(pipe, max_bytes));
    let stderr_reader = child
        .stderr
        .take()
        .map(|pipe| drain_child_pipe(pipe, max_bytes));

    match child.wait_timeout(timeout) {
        Ok(Some(status)) => {
            let result = (|| {
                let Some(stdout) = finish_child_pipe(stdout_reader, deadline)? else {
                    return Ok(None);
                };
                let Some(stderr) = finish_child_pipe(stderr_reader, deadline)? else {
                    return Ok(None);
                };
                Ok(Some(Output {
                    status,
                    stdout,
                    stderr,
                }))
            })();
            if !matches!(&result, Ok(Some(_))) {
                kill_child_process_group(child_pid);
            }
            result
        }
        Ok(None) => {
            kill_child_process_group(child_pid);
            let _ = child.kill();
            let _ = child.wait();
            Ok(None)
        }
        Err(error) => {
            kill_child_process_group(child_pid);
            let _ = child.kill();
            let _ = child.wait();
            Err(error)
        }
    }
}

// Re-export commonly used config types
pub use config::{
    ConfigError, PathMapping, Platform, SourceDefinition, SourcesConfig, SyncSchedule,
};

// Re-export commonly used provenance types
pub use provenance::{LOCAL_SOURCE_ID, Origin, Source, SourceFilter, SourceKind};

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Stdio;

    #[cfg(unix)]
    #[test]
    fn bounded_child_output_preserves_small_output_and_rejects_overflow() {
        for (script, overflow) in [
            ("printf 1234; printf abcd >&2", false),
            ("printf 12345", true),
            ("printf 12345 >&2", true),
        ] {
            let mut command = Command::new("sh");
            command
                .args(["-c", script])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            configure_child_process_group(&mut command);
            let result = wait_for_child_output_with_limit(
                command.spawn().unwrap(),
                Duration::from_secs(5),
                Some(4),
            );
            if overflow {
                assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidData);
            } else {
                let output = result.unwrap().unwrap();
                assert!(output.status.success());
                assert_eq!(output.stdout, b"1234");
                assert_eq!(output.stderr, b"abcd");
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn bounded_child_output_stops_immediately_after_overflow() {
        let mut command = Command::new("sh");
        command
            .args(["-c", "printf 12345; sleep 30"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        configure_child_process_group(&mut command);
        let started = std::time::Instant::now();
        let error = wait_for_child_output_with_limit(
            command.spawn().unwrap(),
            Duration::from_secs(30),
            Some(4),
        )
        .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(started.elapsed() < Duration::from_secs(5));
    }
}
