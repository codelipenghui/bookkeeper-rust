/// Round-robin distribution schedule.
/// For entry `e`, write set = [(e+0)%E, (e+1)%E, ..., (e+W-1)%E]
/// where E=ensembleSize and W=writeQuorumSize.
#[derive(Debug, Clone)]
pub struct RoundRobinDistributionSchedule {
    ensemble_size: usize,
    write_quorum_size: usize,
    ack_quorum_size: usize,
}

impl RoundRobinDistributionSchedule {
    pub fn new(ensemble_size: usize, write_quorum_size: usize, ack_quorum_size: usize) -> Self {
        Self { ensemble_size, write_quorum_size, ack_quorum_size }
    }

    pub fn ensemble_size(&self) -> usize { self.ensemble_size }
    pub fn write_quorum_size(&self) -> usize { self.write_quorum_size }
    pub fn ack_quorum_size(&self) -> usize { self.ack_quorum_size }

    /// Compute write set indices for a given entry ID.
    pub fn write_set(&self, entry_id: i64) -> Vec<usize> {
        let e = entry_id as usize;
        (0..self.write_quorum_size)
            .map(|i| (e + i) % self.ensemble_size)
            .collect()
    }

    /// Create a new AckSet sized to this schedule's ensemble.
    pub fn new_ack_set(&self) -> AckSet {
        AckSet::new(self.ensemble_size, self.write_quorum_size, self.ack_quorum_size)
    }
}

/// Tracks which bookies in an ensemble have acknowledged a write and
/// which have failed it. Quorum is the cardinality of the heard-from
/// set, not the count of ack messages — a bookie that acks twice
/// (e.g. retransmits) counts once. A bookie that acks then fails has
/// its ack retracted; a bookie that fails then acks has its failure
/// retracted.
///
/// Mirrors Apache BookKeeper's
/// `RoundRobinDistributionSchedule.AckSetImpl`: a `BitSet ackSet` plus
/// a `BookieId[] failureMap`, with `completeBookieAndCheck` /
/// `failBookieAndCheck` toggling the bits at the heard-from index in
/// the obvious way.
#[derive(Debug)]
pub struct AckSet {
    write_quorum_size: usize,
    ack_quorum_size: usize,
    /// `true` at index `i` means bookie at ensemble index `i` has
    /// acked and not since failed.
    acked: Vec<bool>,
    /// `true` at index `i` means bookie at ensemble index `i` has
    /// failed and not since acked. Java keeps the actual `BookieId`
    /// here so callers can build a "failed bookies" map for the
    /// ensemble-change path; the Rust client does not currently need
    /// the address (it has the ensemble vector by index already), so
    /// a presence bit is enough.
    failed: Vec<bool>,
    ack_count: usize,
    fail_count: usize,
}

impl AckSet {
    pub fn new(ensemble_size: usize, write_quorum_size: usize, ack_quorum_size: usize) -> Self {
        Self {
            write_quorum_size,
            ack_quorum_size,
            acked: vec![false; ensemble_size],
            failed: vec![false; ensemble_size],
            ack_count: 0,
            fail_count: 0,
        }
    }

    /// Record a successful ack from `bookie_index`. Returns true when
    /// the ack quorum is now reached. Idempotent for repeated acks
    /// from the same bookie. Retracts a prior failure recorded for
    /// the same bookie.
    pub fn complete_bookie(&mut self, bookie_index: usize) -> bool {
        if let Some(slot) = self.failed.get_mut(bookie_index) {
            if *slot {
                *slot = false;
                self.fail_count -= 1;
            }
        }
        if let Some(slot) = self.acked.get_mut(bookie_index) {
            if !*slot {
                *slot = true;
                self.ack_count += 1;
            }
        }
        self.ack_count >= self.ack_quorum_size
    }

    /// Record a failure from `bookie_index`. Returns true when the
    /// number of failed bookies exceeds the per-write tolerance
    /// (`write_quorum - ack_quorum`), i.e. ack quorum is now
    /// impossible. Idempotent for repeated failures from the same
    /// bookie. Retracts a prior ack recorded for the same bookie.
    pub fn fail_bookie(&mut self, bookie_index: usize) -> bool {
        if let Some(slot) = self.acked.get_mut(bookie_index) {
            if *slot {
                *slot = false;
                self.ack_count -= 1;
            }
        }
        if let Some(slot) = self.failed.get_mut(bookie_index) {
            if !*slot {
                *slot = true;
                self.fail_count += 1;
            }
        }
        self.fail_count > self.write_quorum_size - self.ack_quorum_size
    }

    pub fn is_quorum_reached(&self) -> bool {
        self.ack_count >= self.ack_quorum_size
    }
}
