/// A value paired with a version for optimistic concurrency control.
/// Version semantics are backend-specific — for ZK it is the znode
/// `stat.version` (widened from i32 on read); for oxia it is
/// `Version.version_id`. Both fit in i64.
#[derive(Debug, Clone)]
pub struct Versioned<T> {
    value: T,
    version: i64,
}

impl<T> Versioned<T> {
    pub fn new(value: T, version: i64) -> Self {
        Self { value, version }
    }

    pub fn value(&self) -> &T { &self.value }
    pub fn into_value(self) -> T { self.value }
    pub fn version(&self) -> i64 { self.version }

    pub fn map<U, F: FnOnce(T) -> U>(self, f: F) -> Versioned<U> {
        Versioned { value: f(self.value), version: self.version }
    }
}
