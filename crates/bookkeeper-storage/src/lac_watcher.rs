use std::collections::HashMap;
use std::sync::Mutex;

/// Trait abstracting over sync and async sender types for the drain helper.
trait WatcherSender: Send {
    fn fire(self, value: i64);
}
impl WatcherSender for std::sync::mpsc::Sender<i64> {
    fn fire(self, v: i64) { let _ = self.send(v); }
}
impl WatcherSender for tokio::sync::oneshot::Sender<i64> {
    fn fire(self, v: i64) { let _ = self.send(v); }
}

/// Drain watchers from a map entry where the predicate matches, returning
/// senders to fire. Keeps non-matching watchers in the list.
fn drain_matching<S: WatcherSender>(
    map: &mut HashMap<i64, Vec<(i64, S)>>,
    ledger_id: i64,
    predicate: impl Fn(i64) -> bool,
) -> Vec<S> {
    let Some(list) = map.get_mut(&ledger_id) else { return Vec::new() };
    let mut remaining = Vec::new();
    let mut notify = Vec::new();
    for (prev, tx) in list.drain(..) {
        if predicate(prev) {
            notify.push(tx);
        } else {
            remaining.push((prev, tx));
        }
    }
    *list = remaining;
    notify
}

/// Remove all watchers for a ledger and return senders to fire.
fn remove_all<S: WatcherSender>(
    map: &mut HashMap<i64, Vec<(i64, S)>>,
    ledger_id: i64,
) -> Vec<S> {
    map.remove(&ledger_id)
        .unwrap_or_default()
        .into_iter()
        .map(|(_, tx)| tx)
        .collect()
}

/// Per-ledger LAC watcher registry for long poll reads.
pub struct LacWatcherRegistry {
    watchers: Mutex<HashMap<i64, Vec<(i64, std::sync::mpsc::Sender<i64>)>>>,
    async_watchers: Mutex<HashMap<i64, Vec<(i64, tokio::sync::oneshot::Sender<i64>)>>>,
}

impl LacWatcherRegistry {
    pub fn new() -> Self {
        Self {
            watchers: Mutex::new(HashMap::new()),
            async_watchers: Mutex::new(HashMap::new()),
        }
    }

    /// Register a sync watcher. Returns None if LAC already advanced.
    pub fn watch(
        &self,
        ledger_id: i64,
        previous_lac: i64,
        current_lac: i64,
    ) -> Option<std::sync::mpsc::Receiver<i64>> {
        if current_lac > previous_lac {
            return None;
        }
        let (tx, rx) = std::sync::mpsc::channel();
        self.watchers.lock().unwrap().entry(ledger_id).or_default().push((previous_lac, tx));
        Some(rx)
    }

    /// Register an async watcher. Returns None if LAC already advanced.
    pub fn watch_async(
        &self,
        ledger_id: i64,
        previous_lac: i64,
        current_lac: i64,
    ) -> Option<tokio::sync::oneshot::Receiver<i64>> {
        if current_lac > previous_lac {
            return None;
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.async_watchers.lock().unwrap().entry(ledger_id).or_default().push((previous_lac, tx));
        Some(rx)
    }

    /// Race-free version of `watch_async`: registers the watcher, then
    /// re-reads LAC via `recheck` while still holding the watcher lock.
    /// If LAC advanced between the caller's snapshot and registration,
    /// the just-registered watcher is fired immediately instead of
    /// being left to wait for a notification that already happened.
    ///
    /// Without this, a long-poll caller can race with a concurrent
    /// `notify_lac_advance` and hang to its full timeout: the notifier
    /// drains an empty watcher list, then the watcher is registered
    /// against an already-stale `previous_lac`.
    pub fn watch_async_with_recheck<F>(
        &self,
        ledger_id: i64,
        previous_lac: i64,
        current_lac: i64,
        recheck: F,
    ) -> Option<tokio::sync::oneshot::Receiver<i64>>
    where
        F: FnOnce() -> i64,
    {
        if current_lac > previous_lac {
            return None;
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        let mut watchers = self.async_watchers.lock().unwrap();
        watchers.entry(ledger_id).or_default().push((previous_lac, tx));
        // Re-snapshot under the watcher lock. notify_lac_advance also
        // takes this lock, so a value seen here cannot have a matching
        // notify slip past us.
        let after = recheck();
        if after > previous_lac {
            for tx in drain_matching(&mut watchers, ledger_id, |prev| after > prev) {
                tx.fire(after);
            }
        }
        Some(rx)
    }

    /// Notify all watchers for a ledger that LAC has advanced.
    pub fn notify_lac_advance(&self, ledger_id: i64, new_lac: i64) {
        let sync = drain_matching(&mut self.watchers.lock().unwrap(), ledger_id, |prev| new_lac > prev);
        for tx in sync { tx.fire(new_lac); }

        let async_ = drain_matching(&mut self.async_watchers.lock().unwrap(), ledger_id, |prev| new_lac > prev);
        for tx in async_ { tx.fire(new_lac); }
    }

    /// Fire all watchers for a ledger (e.g., ledger closed).
    pub fn notify_ledger_closed(&self, ledger_id: i64) {
        let sync = remove_all(&mut self.watchers.lock().unwrap(), ledger_id);
        for tx in sync { tx.fire(i64::MAX); }

        let async_ = remove_all(&mut self.async_watchers.lock().unwrap(), ledger_id);
        for tx in async_ { tx.fire(i64::MAX); }
    }

    /// Remove all watchers for a ledger.
    pub fn cancel_watchers(&self, ledger_id: i64) {
        self.watchers.lock().unwrap().remove(&ledger_id);
        self.async_watchers.lock().unwrap().remove(&ledger_id);
    }

    /// Clean up watchers for ledgers that no longer exist.
    pub fn cleanup_stale(&self, active_ledgers: &std::collections::HashSet<i64>) {
        self.watchers.lock().unwrap().retain(|lid, _| active_ledgers.contains(lid));
        self.async_watchers.lock().unwrap().retain(|lid, _| active_ledgers.contains(lid));
    }
}
