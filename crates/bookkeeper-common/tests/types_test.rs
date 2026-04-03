use bookkeeper_common::types::BookieId;

#[test]
fn bookie_id_from_host_port() {
    let id = BookieId::new("bookie1", 3181);
    assert_eq!(id.to_string(), "bookie1:3181");
    assert_eq!(id.host(), "bookie1");
    assert_eq!(id.port(), 3181);
}

#[test]
fn bookie_id_parse() {
    let id: BookieId = "bookie1:3181".parse().unwrap();
    assert_eq!(id.host(), "bookie1");
    assert_eq!(id.port(), 3181);
}

#[test]
fn bookie_id_parse_invalid() {
    assert!("invalid".parse::<BookieId>().is_err());
}
