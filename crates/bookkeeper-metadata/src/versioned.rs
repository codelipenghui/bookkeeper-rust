/// A value paired with a version for optimistic concurrency control.
/// The version is opaque — for ZK, it's the znode stat version (i32).
#[derive(Debug, Clone)]
pub struct Versioned<T> {
    value: T,
    version: i32,
}

impl<T> Versioned<T> {
    pub fn new(value: T, version: i32) -> Self {
        Self { value, version }
    }

    pub fn value(&self) -> &T { &self.value }
    pub fn into_value(self) -> T { self.value }
    pub fn version(&self) -> i32 { self.version }

    pub fn map<U, F: FnOnce(T) -> U>(self, f: F) -> Versioned<U> {
        Versioned { value: f(self.value), version: self.version }
    }
}
