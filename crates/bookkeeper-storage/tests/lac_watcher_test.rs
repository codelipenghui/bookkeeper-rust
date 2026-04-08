//! Tests for the LacWatcherRegistry — the long-poll read mechanism.
//!
//! Long poll reads block waiting for the ledger's LAC (Last Add Confirmed)
//! to advance past a previous value, with a timeout. This matches Java's
//! LongPollReadEntryProcessorV3 + TransientLedgerInfo behavior.

use std::sync::Arc;
use std::time::Duration;

use bookkeeper_storage::lac_watcher::LacWatcherRegistry;

#[test]
fn watch_returns_none_when_lac_already_advanced() {
    let reg = LacWatcherRegistry::new();
    // current_lac (5) > previous_lac (3): no need to wait
    let result = reg.watch(1, 3, 5);
    assert!(result.is_none(), "should short-circuit when LAC already advanced");
}

#[test]
fn watch_returns_some_when_must_wait() {
    let reg = LacWatcherRegistry::new();
    // current_lac (3) == previous_lac (3): block until advance
    let result = reg.watch(1, 3, 3);
    assert!(result.is_some(), "should register watcher when LAC has not advanced");
}

#[test]
fn notify_wakes_waiting_watcher() {
    let reg = Arc::new(LacWatcherRegistry::new());
    let rx = reg.watch(42, 10, 10).unwrap();

    // Notify on a separate thread
    let reg2 = reg.clone();
    let handle = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(50));
        reg2.notify_lac_advance(42, 15);
    });

    let new_lac = rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(new_lac, 15);
    handle.join().unwrap();
}

#[test]
fn watcher_times_out_when_no_advance() {
    let reg = LacWatcherRegistry::new();
    let rx = reg.watch(1, 5, 5).unwrap();
    let result = rx.recv_timeout(Duration::from_millis(100));
    assert!(result.is_err(), "should time out when no advance happens");
}

#[test]
fn notify_wakes_only_watchers_below_new_lac() {
    let reg = LacWatcherRegistry::new();
    let rx_low = reg.watch(1, 3, 3).unwrap();   // wants > 3
    let rx_mid = reg.watch(1, 5, 5).unwrap();   // wants > 5
    let rx_high = reg.watch(1, 10, 10).unwrap(); // wants > 10

    // Advance to 7: should wake low + mid, NOT high
    reg.notify_lac_advance(1, 7);

    assert_eq!(rx_low.recv_timeout(Duration::from_millis(100)).unwrap(), 7);
    assert_eq!(rx_mid.recv_timeout(Duration::from_millis(100)).unwrap(), 7);
    assert!(rx_high.recv_timeout(Duration::from_millis(100)).is_err(),
        "high watcher should still be waiting");
}

#[test]
fn notify_advances_high_watcher_on_second_call() {
    let reg = LacWatcherRegistry::new();
    let rx = reg.watch(1, 100, 100).unwrap();

    reg.notify_lac_advance(1, 50);  // not enough, watcher stays
    let result = rx.recv_timeout(Duration::from_millis(50));
    assert!(result.is_err(), "watcher should still be waiting after insufficient advance");

    reg.notify_lac_advance(1, 101); // now advances past 100
    let new_lac = rx.recv_timeout(Duration::from_millis(100)).unwrap();
    assert_eq!(new_lac, 101);
}

#[test]
fn notify_ledger_closed_wakes_all_watchers() {
    let reg = LacWatcherRegistry::new();
    let rx1 = reg.watch(1, 5, 5).unwrap();
    let rx2 = reg.watch(1, 10, 10).unwrap();

    reg.notify_ledger_closed(1);

    // All watchers should receive i64::MAX
    assert_eq!(rx1.recv_timeout(Duration::from_millis(100)).unwrap(), i64::MAX);
    assert_eq!(rx2.recv_timeout(Duration::from_millis(100)).unwrap(), i64::MAX);
}

#[test]
fn notify_for_other_ledger_does_not_wake() {
    let reg = LacWatcherRegistry::new();
    let rx = reg.watch(42, 5, 5).unwrap();

    reg.notify_lac_advance(99, 100); // different ledger
    assert!(rx.recv_timeout(Duration::from_millis(100)).is_err());

    reg.notify_lac_advance(42, 6); // correct ledger
    assert_eq!(rx.recv_timeout(Duration::from_millis(100)).unwrap(), 6);
}

#[test]
fn concurrent_watchers_and_notifications() {
    let reg = Arc::new(LacWatcherRegistry::new());
    let mut handles = Vec::new();

    // 10 watchers, each waiting for LAC to advance past their threshold
    let mut receivers = Vec::new();
    for i in 0..10 {
        let rx = reg.watch(7, i, i).unwrap();
        receivers.push((i, rx));
    }

    // Spawn notifier thread that advances LAC step by step
    let reg2 = reg.clone();
    handles.push(std::thread::spawn(move || {
        for lac in 1..=10 {
            std::thread::sleep(Duration::from_millis(10));
            reg2.notify_lac_advance(7, lac);
        }
    }));

    // Each watcher should be woken in order
    for (threshold, rx) in receivers {
        let new_lac = rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(new_lac > threshold);
    }

    for h in handles { h.join().unwrap(); }
}
