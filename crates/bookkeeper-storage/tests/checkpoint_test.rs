use bookkeeper_storage::checkpoint::*;
use bookkeeper_storage::journal::format::LastMark;
use tempfile::TempDir;

#[test]
fn write_and_read_last_mark() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join(".lastMark");
    let mark = LastMark::new(10, 5000);
    write_last_mark(&path, &mark).unwrap();
    let read_mark = read_last_mark(&path).unwrap();
    assert_eq!(read_mark.log_id(), 10);
    assert_eq!(read_mark.offset(), 5000);
}

#[test]
fn read_missing_last_mark_returns_zero() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join(".lastMark");
    let mark = read_last_mark(&path).unwrap();
    assert_eq!(mark.log_id(), 0);
    assert_eq!(mark.offset(), 0);
}

#[test]
fn latest_mark_across_dirs() {
    let dir1 = TempDir::new().unwrap();
    let dir2 = TempDir::new().unwrap();
    write_last_mark(&dir1.path().join(".lastMark"), &LastMark::new(5, 100)).unwrap();
    write_last_mark(&dir2.path().join(".lastMark"), &LastMark::new(10, 200)).unwrap();
    let dirs = vec![dir1.path().to_path_buf(), dir2.path().to_path_buf()];
    let latest = find_latest_last_mark(&dirs).unwrap();
    assert_eq!(latest.log_id(), 10);
    assert_eq!(latest.offset(), 200);
}
