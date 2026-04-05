use std::collections::HashMap;
use std::sync::Mutex;

/// Per-ledger LAC watcher registry for long poll reads.
///
/// Uses std::sync channels (not tokio) so watchers work from sync context
/// (spawn_blocking). Matches Java's TransientLedgerInfo watcher mechanism.
pub struct LacWatcherRegistry {
    watchers: Mutex<HashMap<i64, Vec<(i64, std::sync::mpsc::Sender<i64>)>>>,
}

impl LacWatcherRegistry {
    pub fn new() -> Self {
        Self {
            watchers: Mutex::new(HashMap::new()),
        }
    }

    /// Register a watcher that fires when LAC advances past `previous_lac`.
    /// Returns a sync receiver that blocks until notified or timeout.
    /// If current LAC already > previous_lac, returns None (read immediately).
    pub fn watch(
        &self,
        ledger_id: i64,
        previous_lac: i64,
        current_lac: i64,
    ) -> Option<std::sync::mpsc::Receiver<i64>> {
        if current_lac > previous_lac {
            return None; // already advanced
        }
        let (tx, rx) = std::sync::mpsc::channel();
        let mut watchers = self.watchers.lock().unwrap();
        watchers.entry(ledger_id).or_default().push((previous_lac, tx));
        Some(rx)
    }

    /// Notify all watchers for a ledger that LAC has advanced.
    pub fn notify_lac_advance(&self, ledger_id: i64, new_lac: i64) {
        let mut watchers = self.watchers.lock().unwrap();
        if let Some(list) = watchers.get_mut(&ledger_id) {
            let remaining: Vec<_> = list
                .drain(..)
                .filter_map(|(prev, tx)| {
                    if new_lac > prev {
                        let _ = tx.send(new_lac);
                        None
                    } else {
                        Some((prev, tx))
                    }
                })
                .collect();
            *list = remaining;
        }
    }

    /// Fire all watchers for a ledger (e.g., ledger closed).
    /// Matches Java's TransientLedgerInfo notifyWatchers(Long.MAX_VALUE) on close.
    pub fn notify_ledger_closed(&self, ledger_id: i64) {
        let mut watchers = self.watchers.lock().unwrap();
        if let Some(list) = watchers.remove(&ledger_id) {
            for (_, tx) in list {
                let _ = tx.send(i64::MAX);
            }
        }
    }

    /// Remove all watchers for a ledger.
    pub fn cancel_watchers(&self, ledger_id: i64) {
        let mut watchers = self.watchers.lock().unwrap();
        watchers.remove(&ledger_id);
    }

    /// Clean up watchers for ledgers that no longer exist.
    /// Call periodically to prevent memory leaks from abandoned watchers.
    pub fn cleanup_stale(&self, active_ledgers: &std::collections::HashSet<i64>) {
        let mut watchers = self.watchers.lock().unwrap();
        watchers.retain(|lid, _| active_ledgers.contains(lid));
    }
}
