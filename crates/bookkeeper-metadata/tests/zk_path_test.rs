use bookkeeper_metadata::zk_path::*;

#[test]
fn short_path_ledger_1() {
    assert_eq!(ledger_path("/ledgers", 1), "/ledgers/00/0000/L0001");
}

#[test]
fn short_path_ledger_12345678() {
    assert_eq!(ledger_path("/ledgers", 12345678), "/ledgers/00/1234/L5678");
}

#[test]
fn short_path_max_int_minus_1() {
    // 2^31 - 2 = 2147483646
    assert_eq!(ledger_path("/ledgers", 2147483646), "/ledgers/21/4748/L3646");
}

#[test]
fn long_path_max_int() {
    // 2^31 - 1 = 2147483647 → uses long path (not < i32::MAX)
    // format!("{:019}", 2147483647) = "0000000002147483647"
    // split 3-4-4-4-4: "000" / "0000" / "0021" / "4748" / "3647"
    // path: /ledgers/000/0000/0021/4748/L3647
    assert_eq!(ledger_path("/ledgers", 2147483647), "/ledgers/000/0000/0021/4748/L3647");
}

#[test]
fn long_path_large_id() {
    // 4294967296 → "0000000004294967296" → 000/0000/0042/9496/L7296
    assert_eq!(
        ledger_path("/ledgers", 4294967296),
        "/ledgers/000/0000/0042/9496/L7296"
    );
}

#[test]
fn underreplication_path() {
    // ledgerId = 0x0001_0002_0003_0004 = 281483566841860
    let path = underreplication_ledger_path("/ledgers/underreplication/ledgers", 281483566841860);
    assert_eq!(path, "/ledgers/underreplication/ledgers/0001/0002/0003/0004/urL281483566841860");
}

#[test]
fn underreplication_path_zero() {
    let path = underreplication_ledger_path("/ledgers/underreplication/ledgers", 0);
    assert_eq!(path, "/ledgers/underreplication/ledgers/0000/0000/0000/0000/urL0000000000");
}

#[test]
fn base_paths() {
    let paths = ZkPaths::new("/ledgers");
    assert_eq!(paths.available(), "/ledgers/available");
    assert_eq!(paths.available_readonly(), "/ledgers/available/readonly");
    assert_eq!(paths.cookies(), "/ledgers/cookies");
    assert_eq!(paths.layout(), "/ledgers/LAYOUT");
    assert_eq!(paths.instance_id(), "/ledgers/instanceid");
    assert_eq!(paths.idgen_long(), "/ledgers/idgen-long");
}
