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
    let mut ack = AckSet::new(3, 2);
    assert!(!ack.complete_bookie(0));
    assert!(ack.complete_bookie(1)); // 2 acks = quorum
}

#[test]
fn ack_set_failure_detected() {
    let mut ack = AckSet::new(3, 2);
    assert!(!ack.fail_bookie(0)); // 1 fail, still possible
    assert!(ack.fail_bookie(1));  // 2 fails > (3-2)=1, quorum impossible
}

#[test]
fn ack_set_mixed() {
    let mut ack = AckSet::new(3, 2);
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
