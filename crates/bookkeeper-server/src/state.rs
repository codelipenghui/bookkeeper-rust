use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BookieState {
    Starting = 0,
    Running = 1,
    ReadOnly = 2,
    ShuttingDown = 3,
}

impl BookieState {
    fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Starting,
            1 => Self::Running,
            2 => Self::ReadOnly,
            3 => Self::ShuttingDown,
            _ => Self::ShuttingDown,
        }
    }

    /// Serialize to string for disk persistence. Matches Java's BookieStatus format.
    fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "STARTING",
            Self::Running => "RUNNING",
            Self::ReadOnly => "READ_ONLY",
            Self::ShuttingDown => "SHUTTING_DOWN",
        }
    }

    fn from_str(s: &str) -> Option<Self> {
        match s.trim() {
            "RUNNING" => Some(Self::Running),
            "READ_ONLY" => Some(Self::ReadOnly),
            _ => None,
        }
    }
}

pub struct BookieStateManager {
    state: AtomicU8,
    /// Optional path to persist bookie status.
    /// When set, state transitions are written to this file so they
    /// survive restarts. Matches Java's `persistBookieStatusEnabled`.
    status_file: Option<PathBuf>,
    /// When true, the bookie was manually set to read-only (via HTTP API)
    /// and should NOT be automatically recovered by the disk monitor.
    /// Matches Java's `isManuallyModifiedToReadOnly()`.
    manually_read_only: std::sync::atomic::AtomicBool,
}

impl BookieStateManager {
    pub fn new() -> Self {
        Self {
            state: AtomicU8::new(BookieState::Starting as u8),
            status_file: None,
            manually_read_only: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Create with persistent state file. Restores last persisted state if the file exists.
    pub fn with_persistence(status_file: &Path) -> Self {
        let initial = std::fs::read_to_string(status_file)
            .ok()
            .and_then(|s| BookieState::from_str(&s))
            .map(|s| s as u8)
            .unwrap_or(BookieState::Starting as u8);
        Self {
            state: AtomicU8::new(initial),
            status_file: Some(status_file.to_path_buf()),
            manually_read_only: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub fn state(&self) -> BookieState {
        BookieState::from_u8(self.state.load(Ordering::SeqCst))
    }

    pub fn transition_to(&self, new_state: BookieState) -> bool {
        loop {
            let current_val = self.state.load(Ordering::SeqCst);
            let current = BookieState::from_u8(current_val);
            let allowed = match (current, new_state) {
                (BookieState::ShuttingDown, _) => false,
                (_, BookieState::ShuttingDown) => true,
                (BookieState::Starting, BookieState::Running) => true,
                (BookieState::Running, BookieState::ReadOnly) => true,
                (BookieState::ReadOnly, BookieState::Running) => true,
                _ => false,
            };
            if !allowed {
                return false;
            }
            match self.state.compare_exchange(
                current_val, new_state as u8,
                Ordering::SeqCst, Ordering::SeqCst
            ) {
                Ok(_) => {
                    self.persist(new_state);
                    return true;
                }
                Err(_) => continue,
            }
        }
    }

    fn persist(&self, state: BookieState) {
        if let Some(ref path) = self.status_file {
            let _ = std::fs::write(path, state.as_str());
        }
    }

    pub fn is_writable(&self) -> bool {
        self.state() == BookieState::Running
    }

    pub fn is_readable(&self) -> bool {
        matches!(self.state(), BookieState::Running | BookieState::ReadOnly)
    }

    pub fn is_shutting_down(&self) -> bool {
        self.state() == BookieState::ShuttingDown
    }

    /// Transition to read-only with manual flag.
    /// When `is_manual` is true, the disk monitor will NOT auto-recover to Running.
    /// Matches Java's `doTransitionToReadOnlyMode(isManuallyModify=true)`.
    pub fn transition_to_read_only(&self, is_manual: bool) -> bool {
        if is_manual {
            self.manually_read_only.store(true, std::sync::atomic::Ordering::Release);
        }
        self.transition_to(BookieState::ReadOnly)
    }

    /// Try to recover from read-only to running.
    /// Fails if the bookie was manually set to read-only.
    /// Matches Java's `doTransitionToWritableMode()` which checks `isManuallyModified`.
    pub fn try_recover_to_running(&self) -> bool {
        if self.manually_read_only.load(std::sync::atomic::Ordering::Acquire) {
            return false;
        }
        self.transition_to(BookieState::Running)
    }

    /// Check if the bookie was manually set to read-only.
    pub fn is_manually_read_only(&self) -> bool {
        self.manually_read_only.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Clear the manual read-only flag (e.g., when explicitly set back to writable via API).
    pub fn clear_manual_read_only(&self) {
        self.manually_read_only.store(false, std::sync::atomic::Ordering::Release);
    }
}

impl Default for BookieStateManager {
    fn default() -> Self {
        Self::new()
    }
}
