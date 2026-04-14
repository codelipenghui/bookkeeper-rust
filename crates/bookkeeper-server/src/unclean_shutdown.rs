//! Unclean shutdown detection — port of Java's `UncleanShutdownDetectionImpl`.
//!
//! At startup (after recovery is complete and just before we start
//! accepting connections), every ledger directory gets a `DIRTY` marker
//! file. On graceful shutdown we delete the files. If the process dies
//! abruptly, the files remain, and the next boot knows the previous run
//! exited uncleanly.
//!
//! This matches Java's behavior in
//! `bookkeeper/bookie/UncleanShutdownDetectionImpl.java` with one
//! addition: when unclean is detected, callers can opt in to a full
//! `local_consistency_check` via `integrity_scan_on_unclean` config so
//! torn-write / partial-flush corruption is surfaced before the bookie
//! serves reads.
//!
//! The `CURRENT_DIR` subdirectory layout is owned by the storage layer
//! (`bookkeeper-storage::db_ledger_storage::CURRENT_DIR`). We mirror the
//! constant here instead of re-exporting it to keep the dependency
//! direction one-way (server → storage) rather than introducing a tiny
//! bidirectional coupling.

use std::io;
use std::path::{Path, PathBuf};

/// Java's `UncleanShutdownDetectionImpl.DIRTY_FILENAME`.
pub const DIRTY_FILENAME: &str = "DIRTY";

/// Subdirectory inside each ledger dir where the DIRTY marker lives.
/// Matches `BookieImpl.getCurrentDirectory(dir)` / our own
/// `storage::db_ledger_storage::CURRENT_DIR`.
const CURRENT_DIR: &str = "current";

/// Writes and reads DIRTY markers in each ledger directory.
pub struct UncleanShutdownDetection<'a> {
    ledger_dirs: &'a [String],
}

impl<'a> UncleanShutdownDetection<'a> {
    pub fn new(ledger_dirs: &'a [String]) -> Self {
        Self { ledger_dirs }
    }

    /// True if any DIRTY marker exists. On I/O error we return true
    /// (fail-safe) — matches Java's `lastShutdownWasUnclean()` which
    /// defaults to unclean on any Throwable.
    pub fn last_shutdown_was_unclean(&self) -> bool {
        let mut dirty_paths: Vec<String> = Vec::new();
        let mut io_err = false;
        for dir in self.ledger_dirs {
            let path = Self::dirty_path(dir);
            match path.try_exists() {
                Ok(true) => dirty_paths.push(path.display().to_string()),
                Ok(false) => {}
                Err(e) => {
                    tracing::error!(
                        "Unable to check DIRTY file at {}: {e} — assuming unclean (fail-safe)",
                        path.display()
                    );
                    io_err = true;
                }
            }
        }
        if !dirty_paths.is_empty() {
            tracing::info!(
                "Dirty files exist on boot-up indicating an unclean shutdown. Dirty files: {}",
                dirty_paths.join(",")
            );
        }
        io_err || !dirty_paths.is_empty()
    }

    /// Create a DIRTY marker in every ledger dir. Java aborts startup if
    /// creation fails — we do the same by returning the underlying I/O
    /// error to the caller.
    pub fn register_startup(&self) -> io::Result<()> {
        for dir in self.ledger_dirs {
            let current = Path::new(dir).join(CURRENT_DIR);
            std::fs::create_dir_all(&current)?;
            let path = current.join(DIRTY_FILENAME);
            // Use `create_new` style semantics via OpenOptions so existing
            // DIRTY markers are preserved (matches Java's
            // `dirtyFile.createNewFile()` which returns false when the
            // file already exists but does not fail).
            match std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)
            {
                Ok(_) => {}
                Err(e) => {
                    tracing::error!(
                        "Unable to register start-up: could not create DIRTY file {}: {e}",
                        path.display()
                    );
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    /// Remove DIRTY markers — signals a graceful shutdown. Java logs but
    /// swallows errors here, so we do the same (a stuck DIRTY file just
    /// means the next boot is conservatively treated as unclean).
    pub fn register_clean_shutdown(&self) {
        for dir in self.ledger_dirs {
            let path = Self::dirty_path(dir);
            match path.try_exists() {
                Ok(true) => {
                    if let Err(e) = std::fs::remove_file(&path) {
                        tracing::error!(
                            "Unable to register clean shutdown: could not remove DIRTY file {}: {e}",
                            path.display()
                        );
                    }
                }
                Ok(false) => {
                    tracing::warn!(
                        "register_clean_shutdown: DIRTY file {} is missing",
                        path.display()
                    );
                }
                Err(e) => {
                    tracing::error!(
                        "register_clean_shutdown: cannot stat DIRTY file {}: {e}",
                        path.display()
                    );
                }
            }
        }
    }

    fn dirty_path(dir: &str) -> PathBuf {
        Path::new(dir).join(CURRENT_DIR).join(DIRTY_FILENAME)
    }
}
