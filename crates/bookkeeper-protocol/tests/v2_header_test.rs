use bookkeeper_protocol::v2::header::PacketHeader;

#[test]
fn encode_v2_header() {
    let h = PacketHeader::new(2, 1, 0);
    assert_eq!(h.to_u32(), 0x02_01_0000);
}

#[test]
fn decode_v2_header() {
    let h = PacketHeader::from_u32(0x02_01_0000);
    assert_eq!(h.version(), 2);
    assert_eq!(h.opcode(), 1);
    assert_eq!(h.flags(), 0);
}

#[test]
fn encode_v2_header_with_flags() {
    let h = PacketHeader::new(2, 2, 0x0001);
    assert_eq!(h.to_u32(), 0x02_02_0001);
}

#[test]
fn v0_backward_compat() {
    let h = PacketHeader::from_u32(1);
    assert_eq!(h.version(), 0);
    assert_eq!(h.opcode(), 1);
    assert_eq!(h.flags(), 0);
}

#[test]
fn round_trip() {
    let original = PacketHeader::new(2, 5, 0x0004);
    let decoded = PacketHeader::from_u32(original.to_u32());
    assert_eq!(decoded.version(), 2);
    assert_eq!(decoded.opcode(), 5);
    assert_eq!(decoded.flags(), 0x0004);
}
