use bookkeeper_server::state::{BookieState, BookieStateManager};

#[test]
fn initial_state_is_starting() {
    let mgr = BookieStateManager::new();
    assert_eq!(mgr.state(), BookieState::Starting);
}

#[test]
fn transition_to_running() {
    let mgr = BookieStateManager::new();
    assert!(mgr.transition_to(BookieState::Running));
    assert_eq!(mgr.state(), BookieState::Running);
}

#[test]
fn running_to_read_only() {
    let mgr = BookieStateManager::new();
    mgr.transition_to(BookieState::Running);
    assert!(mgr.transition_to(BookieState::ReadOnly));
    assert_eq!(mgr.state(), BookieState::ReadOnly);
}

#[test]
fn read_only_back_to_running() {
    let mgr = BookieStateManager::new();
    mgr.transition_to(BookieState::Running);
    mgr.transition_to(BookieState::ReadOnly);
    assert!(mgr.transition_to(BookieState::Running));
}

#[test]
fn any_to_shutting_down() {
    let mgr = BookieStateManager::new();
    mgr.transition_to(BookieState::Running);
    assert!(mgr.transition_to(BookieState::ShuttingDown));
    assert_eq!(mgr.state(), BookieState::ShuttingDown);
}

#[test]
fn cannot_transition_from_shutting_down() {
    let mgr = BookieStateManager::new();
    mgr.transition_to(BookieState::Running);
    mgr.transition_to(BookieState::ShuttingDown);
    assert!(!mgr.transition_to(BookieState::Running));
    assert_eq!(mgr.state(), BookieState::ShuttingDown);
}

#[test]
fn is_writable() {
    let mgr = BookieStateManager::new();
    assert!(!mgr.is_writable()); // Starting is not writable
    mgr.transition_to(BookieState::Running);
    assert!(mgr.is_writable());
    mgr.transition_to(BookieState::ReadOnly);
    assert!(!mgr.is_writable());
}

#[test]
fn is_readable() {
    let mgr = BookieStateManager::new();
    mgr.transition_to(BookieState::Running);
    assert!(mgr.is_readable());
    mgr.transition_to(BookieState::ReadOnly);
    assert!(mgr.is_readable()); // Read-only can still serve reads
    mgr.transition_to(BookieState::ShuttingDown);
    assert!(!mgr.is_readable());
}
