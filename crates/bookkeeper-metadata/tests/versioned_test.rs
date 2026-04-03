use bookkeeper_metadata::versioned::Versioned;

#[test]
fn versioned_wraps_value_and_version() {
    let v = Versioned::new("hello", 5);
    assert_eq!(v.value(), &"hello");
    assert_eq!(v.version(), 5);
}

#[test]
fn versioned_map() {
    let v = Versioned::new(42, 1);
    let v2 = v.map(|x| x * 2);
    assert_eq!(v2.value(), &84);
    assert_eq!(v2.version(), 1);
}
