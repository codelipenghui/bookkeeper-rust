use bookkeeper_metadata::service_info::BookieServiceInfo;

#[test]
fn default_service_info() {
    let info = BookieServiceInfo::new("bookie1", "bookie1", 3181);
    assert_eq!(info.bookie_id(), "bookie1");
    let bytes = info.to_protobuf_bytes();
    assert!(!bytes.is_empty());
    let parsed = BookieServiceInfo::from_protobuf_bytes(&bytes).unwrap();
    assert_eq!(parsed.bookie_id(), "bookie1");
}

#[test]
fn service_info_endpoint() {
    let info = BookieServiceInfo::new("host1:3181", "host1", 3181);
    let bytes = info.to_protobuf_bytes();
    let parsed = BookieServiceInfo::from_protobuf_bytes(&bytes).unwrap();
    assert_eq!(parsed.host(), "host1");
    assert_eq!(parsed.port(), 3181);
}
