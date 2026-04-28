use bookkeeper_client::distribution::{RoundRobinDistributionSchedule, AckSet};

#[test]
fn write_set_for_entry_0() {
    let sched = RoundRobinDistributionSchedule::new(5, 3, 2);
    let ws = sched.write_set(0);
    assert_eq!(ws, vec![0, 1, 2]);
}

#[test]
fn write_set_wraps_around() {
    let sched = RoundRobinDistributionSchedule::new(5, 3, 2);
    let ws = sched.write_set(4);
    assert_eq!(ws, vec![4, 0, 1]);
}

#[test]
fn write_set_entry_7() {
    let sched = RoundRobinDistributionSchedule::new(5, 3, 2);
    let ws = sched.write_set(7);
    assert_eq!(ws, vec![2, 3, 4]);
}

#[test]
fn ack_set_quorum_reached() {
    let mut ack = AckSet::new(3, 3, 2);
    assert!(!ack.complete_bookie(0));
    assert!(ack.complete_bookie(1)); // 2 acks = quorum
}

#[test]
fn ack_set_failure_detected() {
    let mut ack = AckSet::new(3, 3, 2);
    assert!(!ack.fail_bookie(0)); // 1 fail, still possible
    assert!(ack.fail_bookie(1));  // 2 fails > (3-2)=1, quorum impossible
}

#[test]
fn ack_set_mixed() {
    let mut ack = AckSet::new(3, 3, 2);
    ack.fail_bookie(0);
    assert!(!ack.complete_bookie(1)); // 1 ack, need 2
    assert!(ack.complete_bookie(2));  // 2 acks = quorum
}

#[test]
fn write_set_ensemble_equals_quorum() {
    let sched = RoundRobinDistributionSchedule::new(3, 3, 2);
    assert_eq!(sched.write_set(0), vec![0, 1, 2]);
    assert_eq!(sched.write_set(1), vec![1, 2, 0]);
}

// --- per-bookie ack tracking ---
//
// Mirrors Java's `RoundRobinDistributionSchedule.AckSetImpl`
// (BitSet-backed). `completeBookieAndCheck` is set-on-bit; quorum is
// reached when `cardinality() >= ackQuorumSize`. `failBookieAndCheck`
// is set-on-failure-map; threshold is `failed() > writeQuorum -
// ackQuorum`. Crucially, both methods clear the OPPOSITE bit at the
// same index, so a bookie that fails after acking is correctly
// counted as "failed", and a bookie that acks after failing is
// correctly counted as "acked" (recovers from a transient error).

#[test]
fn duplicate_ack_from_same_bookie_is_idempotent() {
    // E=3, W=3, A=2 — two distinct acks needed for quorum.
    let mut ack = AckSet::new(3, 3, 2);
    let q1 = ack.complete_bookie(0);
    let q2 = ack.complete_bookie(0);
    assert!(!q1, "single ack from bookie 0 must not satisfy ack quorum=2");
    assert!(
        !q2,
        "duplicate ack from the same bookie 0 must NOT count as a \
         second distinct ack — quorum is the cardinality of distinct \
         heard-from bookies, not the count of ack messages"
    );
    // Now a different bookie acks for real — quorum reached.
    assert!(ack.complete_bookie(1));
}

#[test]
fn fail_after_ack_clears_the_ack_bit() {
    // E=3, W=3, A=2. If bookie 0 acks then fails (e.g., later
    // timeout), it must NOT still count toward ack quorum.
    let mut ack = AckSet::new(3, 3, 2);
    ack.complete_bookie(0);
    let became_impossible = ack.fail_bookie(0);
    assert!(
        !became_impossible,
        "one failure (out of W=3, A=2 — tolerable=1) must not flip to \
         quorum-impossible; got {became_impossible}"
    );
    // Bookie 1's ack alone is not enough — bookie 0's ack was
    // invalidated by the subsequent failure.
    assert!(
        !ack.complete_bookie(1),
        "after bookie 0 failed (clearing its ack), bookie 1's lone \
         ack must not satisfy quorum=2"
    );
    assert!(
        ack.complete_bookie(2),
        "ack from a third distinct bookie reaches quorum"
    );
}

#[test]
fn ack_after_fail_clears_the_failure_bit() {
    // Bookie 0 fails then later acks (transient timeout that
    // recovers). The failure must be retracted; failed-count goes
    // back to 0 so further failures don't cross the impossibility
    // threshold prematurely.
    let mut ack = AckSet::new(3, 3, 2);
    ack.fail_bookie(0); // failed=1, tolerable=1
    let _ = ack.complete_bookie(0); // failed should drop back to 0
    let still_possible = ack.fail_bookie(1); // failed=1 again, still OK
    assert!(
        !still_possible,
        "if bookie 0's earlier failure had not been retracted, \
         bookie 1's failure would push failed=2 > tolerable=1 and \
         flip the quorum to impossible. Got {still_possible}"
    );
}

#[test]
fn duplicate_failure_from_same_bookie_is_idempotent() {
    // Two failure reports from the same bookie must not double-count
    // toward the impossibility threshold.
    let mut ack = AckSet::new(5, 3, 2);
    assert!(!ack.fail_bookie(0));
    assert!(
        !ack.fail_bookie(0),
        "duplicate failure from same bookie must not push past the \
         tolerable=1 threshold"
    );
}
