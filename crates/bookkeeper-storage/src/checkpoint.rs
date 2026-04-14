use std::fs;
use std::path::{Path, PathBuf};
use crate::StorageError;
use crate::journal::format::LastMark;

/// Matches Java's `LAST_MARK_DEFAULT_NAME = "lastMark"` (no dot prefix).
const LAST_MARK_FILENAME: &str = "lastMark";

/// A checkpoint token representing a snapshot of the journal position.
/// Matches Java's `CheckpointSource.Checkpoint` interface.
///
/// Two-phase protocol:
/// 1. `new_checkpoint(journal)` → captures current journal position
/// 2. Caller performs storage flush
/// 3. `complete(dirs)` → persists lastMark only after flush succeeds
///
/// If the flush fails, the token is dropped without persisting, and
/// lastMark stays at the previous safe position.
#[derive(Debug, Clone)]
pub struct Checkpoint {
    mark: LastMark,
}

impl Checkpoint {
    /// Create a new checkpoint by snapshotting the current journal position.
    /// Matches Java's `Journal.newCheckpoint()`.
    pub fn new(log_id: i64, offset: i64) -> Self {
        Self { mark: LastMark::new(log_id, offset) }
    }

    /// Create from an existing LastMark.
    pub fn from_mark(mark: LastMark) -> Self {
        Self { mark }
    }

    /// The captured LastMark.
    pub fn mark(&self) -> &LastMark {
        &self.mark
    }

    /// Complete the checkpoint by persisting lastMark to the given directory.
    /// Only call this AFTER the storage flush succeeds.
    /// Matches Java's `journal.checkpointComplete(cp, true)`.
    pub fn complete(&self, base_dir: &Path) -> Result<(), StorageError> {
        let mark_path = base_dir.join("current").join(LAST_MARK_FILENAME);
        write_last_mark(&mark_path, &self.mark)
    }

    /// Complete the checkpoint by persisting lastMark to multiple directories.
    pub fn complete_multi(&self, dirs: &[PathBuf]) -> Result<(), StorageError> {
        for dir in dirs {
            let mark_path = dir.join(LAST_MARK_FILENAME);
            write_last_mark(&mark_path, &self.mark)?;
        }
        Ok(())
    }
}

pub fn write_last_mark(path: &Path, mark: &LastMark) -> Result<(), StorageError> {
    use std::io::Write;
    let data = mark.to_bytes();
    let mut file = fs::File::create(path)?;
    file.write_all(&data)?;
    file.sync_data()?;
    Ok(())
}

pub fn read_last_mark(path: &Path) -> Result<LastMark, StorageError> {
    match fs::read(path) {
        Ok(data) => LastMark::from_bytes(&data),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(LastMark::new(0, 0)),
        Err(e) => Err(StorageError::Io(e)),
    }
}

pub fn find_latest_last_mark(ledger_dirs: &[PathBuf]) -> Result<LastMark, StorageError> {
    let mut latest = LastMark::new(0, 0);
    for dir in ledger_dirs {
        let path = dir.join(LAST_MARK_FILENAME);
        let mark = read_last_mark(&path)?;
        if mark.log_id() > latest.log_id()
            || (mark.log_id() == latest.log_id() && mark.offset() > latest.offset())
        {
            latest = mark;
        }
    }
    Ok(latest)
}
