//! Unit tests for the disk monitor's pure decision function
//! `decide_disk_monitor_action`. Pinning the policy semantics so future
//! refactors can't accidentally reintroduce the macOS-disk false-trip
//! (host disk reports >95% used → bookie flips to ReadOnly at startup).

use bookkeeper_server::server::{decide_disk_monitor_action, DiskMonitorAction};
use bookkeeper_server::state::BookieState;

// ---------------------------------------------------------------------------
// Going to ReadOnly (high water mark trip)
// ---------------------------------------------------------------------------

#[test]
fn going_readonly_when_any_dir_full_and_currently_running() {
    let action = decide_disk_monitor_action(
        BookieState::Running,
        true,
        true, // all_below_lwm
        0.50, // total_usage doesn't matter for the trip branch
        0.95,
        2,
        false,
    );
    assert_eq!(action, DiskMonitorAction::GoReadOnly);
}

#[test]
fn no_action_when_any_dir_full_but_already_readonly() {
    let action = decide_disk_monitor_action(
        BookieState::ReadOnly,
        true,
        false, // all_below_lwm
        0.97,
        0.95,
        2,
        false,
    );
    assert_eq!(action, DiskMonitorAction::Stay);
}

#[test]
fn no_action_when_full_but_state_is_starting() {
    // Starting → Running is owned by the bookie startup path, not the
    // disk monitor. The monitor only acts in the Running ↔ ReadOnly cycle.
    let action = decide_disk_monitor_action(
        BookieState::Starting,
        true,
        false, // all_below_lwm
        0.99,
        0.95,
        1,
        false,
    );
    assert_eq!(action, DiskMonitorAction::Stay);
}

#[test]
fn no_action_when_full_but_state_is_shutting_down() {
    let action = decide_disk_monitor_action(
        BookieState::ShuttingDown,
        true,
        false, // all_below_lwm
        0.99,
        0.95,
        1,
        false,
    );
    assert_eq!(action, DiskMonitorAction::Stay);
}

// ---------------------------------------------------------------------------
// Recovering to Running (low water mark recovery)
// ---------------------------------------------------------------------------

#[test]
fn recovering_to_running_when_all_dirs_below_lwm() {
    let action = decide_disk_monitor_action(
        BookieState::ReadOnly,
        false, // no dir is full
        true,  // all_below_lwm
        0.50,  // total_usage
        0.90,  // lwm_threshold
        2,
        false,
    );
    assert_eq!(action, DiskMonitorAction::GoRunning);
}

#[test]
fn no_recovery_when_total_usage_at_lwm_boundary() {
    // Strict less-than (`<`): usage exactly equal to lwm does NOT recover.
    let action = decide_disk_monitor_action(
        BookieState::ReadOnly,
        false,
        false, // all_below_lwm
        0.95,
        0.95,
        2,
        false,
    );
    assert_eq!(action, DiskMonitorAction::Stay);
}

#[test]
fn no_recovery_when_total_usage_just_above_lwm() {
    let action = decide_disk_monitor_action(
        BookieState::ReadOnly,
        false,
        false, // all_below_lwm
        0.96,
        0.95,
        2,
        false,
    );
    assert_eq!(action, DiskMonitorAction::Stay);
}

#[test]
fn no_recovery_when_state_is_running_already() {
    let action = decide_disk_monitor_action(
        BookieState::Running,
        false,
        true, // all_below_lwm
        0.10,
        0.95,
        2,
        false,
    );
    assert_eq!(action, DiskMonitorAction::Stay);
}

#[test]
fn no_recovery_when_state_is_starting() {
    let action = decide_disk_monitor_action(
        BookieState::Starting,
        false,
        true, // all_below_lwm
        0.10,
        0.95,
        2,
        false,
    );
    assert_eq!(action, DiskMonitorAction::Stay);
}

// ---------------------------------------------------------------------------
// dir_count=0 guards (the "all directories errored" case)
// ---------------------------------------------------------------------------

#[test]
fn no_recovery_when_dir_count_zero_even_if_total_usage_is_zero() {
    // If statvfs failed for every ledger directory, total_used/total_capacity
    // are both 0 → total_usage = 0/0 → 0.0. Without the `dir_count > 0` guard
    // the policy would misread that as "disk recovered to 0% used".
    let action = decide_disk_monitor_action(
        BookieState::ReadOnly,
        false,
        true, // all_below_lwm
        0.0,
        0.95,
        0,
        false,
    );
    assert_eq!(action, DiskMonitorAction::Stay);
}

#[test]
fn going_readonly_works_even_when_dir_count_zero_if_any_full() {
    // Asymmetry: dir_count guards recovery (safe direction), not tripping
    // (the safe direction is to go ReadOnly).
    let action = decide_disk_monitor_action(
        BookieState::Running,
        true,
        true, // all_below_lwm
        0.0,
        0.95,
        0,
        false,
    );
    assert_eq!(action, DiskMonitorAction::GoReadOnly);
}

// ---------------------------------------------------------------------------
// Common steady-state non-actions
// ---------------------------------------------------------------------------

#[test]
fn healthy_running_bookie_stays_running() {
    let action = decide_disk_monitor_action(
        BookieState::Running,
        false,
        true, // all_below_lwm
        0.50,
        0.95,
        3,
        false,
    );
    assert_eq!(action, DiskMonitorAction::Stay);
}

#[test]
fn readonly_bookie_with_high_total_usage_stays_readonly() {
    let action = decide_disk_monitor_action(
        BookieState::ReadOnly,
        false,
        false, // all_below_lwm
        0.96,
        0.95,
        3,
        false,
    );
    assert_eq!(action, DiskMonitorAction::Stay);
}

// ---------------------------------------------------------------------------
// macOS-disk false-trip regression
// ---------------------------------------------------------------------------

#[test]
fn fresh_running_bookie_with_mac_98_percent_disk_trips_to_readonly() {
    // Mitigations on a high-usage host: raise `disk_usage_threshold` or set
    // `read_only_mode_enabled = false`.
    let action = decide_disk_monitor_action(
        BookieState::Running,
        true,
        false, // all_below_lwm
        0.98,
        0.95,
        1,
        false,
    );
    assert_eq!(action, DiskMonitorAction::GoReadOnly);
}

#[test]
fn fresh_running_bookie_with_high_threshold_stays_running_on_98_percent_disk() {
    // The ergonomic mitigation: bumping disk_usage_threshold above the
    // host's actual usage level. With threshold=0.99 and host at 0.98,
    // any_full=false → no trip.
    let action = decide_disk_monitor_action(
        BookieState::Running,
        false, // 0.98 < 0.99 threshold → not full
        true,  // all_below_lwm
        0.98,
        0.99,
        1,
        false,
    );
    assert_eq!(action, DiskMonitorAction::Stay);
}

// ---------------------------------------------------------------------------
// Disk I/O error → Shutdown (S5)
// ---------------------------------------------------------------------------

#[test]
fn disk_io_error_triggers_shutdown_from_running() {
    let action = decide_disk_monitor_action(
        BookieState::Running,
        false,
        true,
        0.50,
        0.95,
        1,
        true, // has_disk_error
    );
    assert_eq!(action, DiskMonitorAction::Shutdown);
}

#[test]
fn disk_io_error_triggers_shutdown_from_readonly() {
    let action = decide_disk_monitor_action(
        BookieState::ReadOnly,
        false,
        true,
        0.50,
        0.95,
        1,
        true,
    );
    assert_eq!(action, DiskMonitorAction::Shutdown);
}

#[test]
fn disk_io_error_takes_precedence_over_space_check() {
    // Even if disk is full, I/O error → Shutdown, not GoReadOnly
    let action = decide_disk_monitor_action(
        BookieState::Running,
        true, // any_full
        false,
        0.99,
        0.95,
        1,
        true, // has_disk_error
    );
    assert_eq!(action, DiskMonitorAction::Shutdown);
}
