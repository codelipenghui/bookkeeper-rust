//! Tests for speculative reads in `read_handle.rs`.
//!
//! Speculative reads send a read to the next bookie in the write set after a
//! configurable timeout, racing the original read against speculative attempts.

use bookkeeper_client::read_handle::{HeardFromHosts, SpeculativeReadPolicy};

#[test]
fn policy_disabled_when_timeouts_zero() {
    let p = SpeculativeReadPolicy::from_config(0, 0, 0, 2.0);
    assert!(p.is_none(), "policy should be disabled when all timeouts are 0");
}

#[test]
fn policy_disabled_when_timeouts_negative() {
    let p = SpeculativeReadPolicy::from_config(-1, -1, -1, 2.0);
    assert!(p.is_none(), "policy should be disabled when timeouts are negative");
}

#[test]
fn policy_uses_speculative_read_timeout_as_default() {
    let p = SpeculativeReadPolicy::from_config(2000, 0, 0, 2.0).unwrap();
    assert_eq!(p.first_timeout_ms, 2000);
    assert_eq!(p.max_timeout_ms, 2000);
    assert_eq!(p.backoff_multiplier, 2.0);
}

#[test]
fn policy_uses_first_speculative_timeout_when_set() {
    let p = SpeculativeReadPolicy::from_config(2000, 500, 0, 2.0).unwrap();
    assert_eq!(p.first_timeout_ms, 500);
    // max defaults to speculative_read_timeout when not set
    assert_eq!(p.max_timeout_ms, 2000);
}

#[test]
fn policy_explicit_max_overrides_default() {
    let p = SpeculativeReadPolicy::from_config(2000, 500, 8000, 2.0).unwrap();
    assert_eq!(p.first_timeout_ms, 500);
    assert_eq!(p.max_timeout_ms, 8000);
}

#[test]
fn policy_zero_multiplier_defaults_to_2() {
    let p = SpeculativeReadPolicy::from_config(2000, 0, 0, 0.0).unwrap();
    assert_eq!(p.backoff_multiplier, 2.0);
}

#[test]
fn heardfrom_marks_and_checks_bits() {
    let h = HeardFromHosts::new();
    assert!(!h.any_heard(&[0, 1, 2]));
    h.mark_heard(1);
    assert!(h.any_heard(&[0, 1, 2]));
    assert!(h.any_heard(&[1]));
    assert!(!h.any_heard(&[0, 2]));
    h.mark_heard(0);
    h.mark_heard(2);
    assert!(h.any_heard(&[0]));
    assert!(h.any_heard(&[2]));
}

#[test]
fn heardfrom_handles_high_indices_safely() {
    let h = HeardFromHosts::new();
    // Index >= 64 silently ignored (no panic)
    h.mark_heard(100);
    assert!(!h.any_heard(&[100]));
    assert!(!h.any_heard(&[0, 50, 63]));
}

#[test]
fn heardfrom_clone_shares_state() {
    let h1 = HeardFromHosts::new();
    let h2 = h1.clone();
    h1.mark_heard(5);
    // Both views see the bit (Arc<AtomicU64>)
    assert!(h2.any_heard(&[5]));
}
