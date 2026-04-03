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
}

pub struct BookieStateManager {
    state: AtomicU8,
}

impl BookieStateManager {
    pub fn new() -> Self {
        Self { state: AtomicU8::new(BookieState::Starting as u8) }
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
                Ok(_) => return true,
                Err(_) => continue, // State changed concurrently, retry
            }
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
}

impl Default for BookieStateManager {
    fn default() -> Self {
        Self::new()
    }
}
