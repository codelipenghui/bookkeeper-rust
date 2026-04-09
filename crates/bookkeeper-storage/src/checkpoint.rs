use std::fs;
use std::path::{Path, PathBuf};
use crate::StorageError;
use crate::journal::format::LastMark;

/// Matches Java's `LAST_MARK_DEFAULT_NAME = "lastMark"` (no dot prefix).
const LAST_MARK_FILENAME: &str = "lastMark";

pub fn write_last_mark(path: &Path, mark: &LastMark) -> Result<(), StorageError> {
    fs::write(path, &mark.to_bytes())?;
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
