//! DESIGN.md §1.3 — "No multi-process access to the data directory,
//! ever." A single-writer lock file turns that documented rule into an
//! enforced one: a second process opening the same directory fails
//! instead of silently interleaving appends into one active segment
//! (review M-2).

use std::io::Write;
use std::path::{Path, PathBuf};

use crate::{Result, SalamanderError};

const LOCK_FILE_NAME: &str = "LOCK";

/// RAII guard: created when a `Log` acquires the directory lock, deletes
/// the lock file on drop. Held as a field on `Log`, so the lock lives
/// exactly as long as the open log — including release on the `?`-early-
/// return paths in `Log::open`, since the guard drops when the local
/// binding goes out of scope before `Log` is constructed.
pub(crate) struct DirLock {
    path: PathBuf,
}

impl DirLock {
    /// Create `<dir>/LOCK` exclusively. Fails with `Locked` if it already
    /// exists — the sign of another live process (or a stale lock left by
    /// a crashed one; see the error message).
    pub(crate) fn acquire(dir: &Path) -> Result<Self> {
        let path = dir.join(LOCK_FILE_NAME);
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(mut file) => {
                // PID is a debugging aid only — we deliberately do NOT
                // read it back to attempt stale-lock takeover. A leftover
                // LOCK after a crash is removed by hand (or, in the crash
                // harness, by the parent after it kills the child).
                let _ = writeln!(file, "{}", std::process::id());
                Ok(DirLock { path })
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                Err(SalamanderError::Locked(format!(
                    "{} exists; another process may hold this data dir. If no \
                     process is using it, delete the LOCK file and retry.",
                    path.display()
                )))
            }
            Err(e) => Err(SalamanderError::Io(e)),
        }
    }
}

impl Drop for DirLock {
    fn drop(&mut self) {
        // Best-effort: if removal fails there's nothing useful to do
        // during a drop, and a leftover lock is recoverable by hand.
        let _ = std::fs::remove_file(&self.path);
    }
}
