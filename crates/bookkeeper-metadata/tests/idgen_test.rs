//! Tests for ledger ID generation schemes (path encoding, HOB composition).
//!
//! These tests focus on the deterministic encoding aspects (no ZK needed).
//! End-to-end ZK ID allocation is exercised by the existing #[ignore]'d tests
//! in `zk_client_test.rs` which require a running ZooKeeper.

use bookkeeper_metadata::zk_path::*;

// ---------------------------------------------------------------------------
// Short hierarchical path (ledger_id < i32::MAX)
// ---------------------------------------------------------------------------

#[test]
fn short_path_zero() {
    assert_eq!(ledger_path("/ledgers", 0), "/ledgers/00/0000/L0000");
}

#[test]
fn short_path_smallest() {
    assert_eq!(ledger_path("/ledgers", 1), "/ledgers/00/0000/L0001");
}

#[test]
fn short_path_just_below_overflow() {
    // i32::MAX - 1 = 2147483646
    assert_eq!(
        ledger_path("/ledgers", 2147483646),
        "/ledgers/21/4748/L3646"
    );
}

#[test]
fn short_path_with_custom_root() {
    assert_eq!(ledger_path("/custom", 42), "/custom/00/0000/L0042");
}

#[test]
fn short_path_each_digit_position() {
    // Check that each 2-4-4 digit position is correctly extracted
    // 01_2345_6789
    assert_eq!(ledger_path("/r", 123456789), "/r/01/2345/L6789");
}

// ---------------------------------------------------------------------------
// Long hierarchical path (ledger_id >= i32::MAX)
// ---------------------------------------------------------------------------

#[test]
fn long_path_at_overflow_boundary() {
    // i32::MAX = 2147483647 → switches to long path
    assert_eq!(
        ledger_path("/ledgers", 2147483647),
        "/ledgers/000/0000/0021/4748/L3647"
    );
}

#[test]
fn long_path_for_large_id() {
    // 4294967296 = 2^32
    assert_eq!(
        ledger_path("/ledgers", 4294967296),
        "/ledgers/000/0000/0042/9496/L7296"
    );
}

#[test]
fn long_path_for_very_large_id() {
    // 1234567890123456789 → 19 digits, split as 3-4-4-4-4
    let path = ledger_path("/ledgers", 1234567890123456789);
    assert!(path.starts_with("/ledgers/"));
    // 19-char zero-padded: "1234567890123456789"
    // 123 / 4567 / 8901 / 2345 / 6789
    assert_eq!(path, "/ledgers/123/4567/8901/2345/L6789");
}

// ---------------------------------------------------------------------------
// Underreplication paths
// ---------------------------------------------------------------------------

#[test]
fn underreplication_path_zero() {
    let path = underreplication_ledger_path("/ledgers/underreplication/ledgers", 0);
    assert_eq!(path, "/ledgers/underreplication/ledgers/0000/0000/0000/0000/urL0000000000");
}

#[test]
fn underreplication_path_with_specific_id() {
    // Use a hand-computed ID that maps to known hex chunks.
    // ledger_id = 0x0001_0002_0003_0004 = 281483566841860
    let id: i64 = 281483566841860;
    let path = underreplication_ledger_path("/ur/ledgers", id);
    assert_eq!(path, "/ur/ledgers/0001/0002/0003/0004/urL281483566841860");
}

#[test]
fn underreplication_path_for_high_id() {
    // 0xABCD_0000_0000_0001
    let id: i64 = 0x0001_2345_6789_ABCDi64;
    let path = underreplication_ledger_path("/ur", id);
    assert_eq!(path, format!("/ur/0001/2345/6789/abcd/urL{}", id));
}

#[test]
fn underreplication_lock_path_format() {
    // Lock path is flat: {base}/urL{id zero-padded}
    let path = underreplication_lock_path("/ur/locks", 42);
    assert_eq!(path, "/ur/locks/urL0000000042");
}

#[test]
fn underreplication_lock_path_zero_padded() {
    let path = underreplication_lock_path("/locks", 0);
    assert_eq!(path, "/locks/urL0000000000");
}

#[test]
fn underreplication_lock_path_large_id() {
    let path = underreplication_lock_path("/locks", 9999999999);
    // 10 digits — exactly at the edge of %010d
    assert_eq!(path, "/locks/urL9999999999");
}

// ---------------------------------------------------------------------------
// HOB encoding (used by long ID generator)
// ---------------------------------------------------------------------------

#[test]
fn hob_zero_padding() {
    // HOB directories use %010d formatting
    assert_eq!(format!("HOB-{:010}", 1), "HOB-0000000001");
    assert_eq!(format!("HOB-{:010}", 999), "HOB-0000000999");
    assert_eq!(format!("HOB-{:010}", 1234567890), "HOB-1234567890");
}

#[test]
fn hob_id_composition() {
    // (high << 32) | low — verify the bit layout
    let high: i64 = 1;
    let low: i64 = 42;
    let combined = (high << 32) | low;
    assert_eq!(combined, 4294967338);
    // Decompose
    assert_eq!(combined >> 32, high);
    assert_eq!(combined & 0xFFFF_FFFF, low);
}

#[test]
fn hob_id_max_low_bits() {
    let high: i64 = 5;
    let low: i64 = i32::MAX as i64 - 1;
    let combined = (high << 32) | low;
    assert_eq!(combined >> 32, high);
    assert_eq!(combined & 0xFFFF_FFFF, low);
}

// ---------------------------------------------------------------------------
// ZkPaths constructor — verify all standard subpaths
// ---------------------------------------------------------------------------

#[test]
fn zkpaths_default_root() {
    let paths = ZkPaths::new("/ledgers");
    assert_eq!(paths.root(), "/ledgers");
    assert_eq!(paths.available(), "/ledgers/available");
    assert_eq!(paths.available_readonly(), "/ledgers/available/readonly");
    assert_eq!(paths.cookies(), "/ledgers/cookies");
    assert_eq!(paths.layout(), "/ledgers/LAYOUT");
    assert_eq!(paths.instance_id(), "/ledgers/INSTANCEID");
    assert_eq!(paths.idgen(), "/ledgers/idgen");
    assert_eq!(paths.idgen_long(), "/ledgers/idgen-long");
    assert_eq!(paths.underreplication(), "/ledgers/underreplication");
}

#[test]
fn zkpaths_custom_root() {
    let paths = ZkPaths::new("/my/custom/root");
    assert_eq!(paths.root(), "/my/custom/root");
    assert_eq!(paths.available(), "/my/custom/root/available");
    assert_eq!(paths.cookies(), "/my/custom/root/cookies");
}

#[test]
fn zkpaths_cookie_path_per_bookie() {
    let paths = ZkPaths::new("/ledgers");
    assert_eq!(paths.cookie_path("bookie1:3181"), "/ledgers/cookies/bookie1:3181");
}

#[test]
fn zkpaths_clone_preserves_all() {
    let p1 = ZkPaths::new("/r");
    let p2 = p1.clone();
    assert_eq!(p1.available(), p2.available());
    assert_eq!(p1.idgen_long(), p2.idgen_long());
}
