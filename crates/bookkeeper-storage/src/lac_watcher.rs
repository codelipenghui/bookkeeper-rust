use std::collections::HashMap;
use std::sync::Mutex;
use tokio::sync::oneshot;

/// Per-ledger LAC watcher registry for long poll reads.
///
/// Matches Java's TransientLedgerInfo watcher mechanism:
/// - Readers register a watcher with their known previousLAC
/// - When an AddEntry advances the LAC past previousLAC, the watcher fires
/// - The watcher sends the new LAC value via a oneshot channel
pub struct LacWatcherRegistry {
    watchers: Mutex<HashMap<i64, Vec<(i64, oneshot::Sender<i64>)>>>,
}

impl LacWatcherRegistry {
    pub fn new() -> Self {
        Self {
            watchers: Mutex::new(HashMap::new()),
        }
    }

    /// Register a watcher that fires when LAC advances past `previous_lac`.
    /// Returns a oneshot receiver that resolves with the new LAC value.
    /// If current LAC is already > previous_lac, returns None (caller should read immediately).
    pub fn watch(
        &self,
        ledger_id: i64,
        previous_lac: i64,
        current_lac: i64,
    ) -> Option<oneshot::Receiver<i64>> {
        if current_lac > previous_lac {
            return None; // already advanced
        }
        let (tx, rx) = oneshot::channel();
        let mut watchers = self.watchers.lock().unwrap();
        watchers.entry(ledger_id).or_default().push((previous_lac, tx));
        Some(rx)
    }

    /// Notify all watchers for a ledger that LAC has advanced.
    /// Called from add_entry path when a new entry is stored.
    pub fn notify_lac_advance(&self, ledger_id: i64, new_lac: i64) {
        let mut watchers = self.watchers.lock().unwrap();
        if let Some(list) = watchers.get_mut(&ledger_id) {
            // Drain entries where new_lac > previous_lac (fire them)
            let remaining: Vec<_> = list.drain(..).filter_map(|(prev, tx)| {
                if new_lac > prev {
                    let _ = tx.send(new_lac); // fire watcher
                    None // consumed
                } else {
                    Some((prev, tx)) // keep waiting
                }
            }).collect();
            *list = remaining;
        }
    }

    /// Cancel all watchers for a ledger.
    pub fn cancel_watchers(&self, ledger_id: i64) {
        let mut watchers = self.watchers.lock().unwrap();
        watchers.remove(&ledger_id);
    }
}
