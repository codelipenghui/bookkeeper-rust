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

    /// Create a new [`QuorumCoverageSet`] sized to this schedule's
    /// ensemble. Used by fence / read-LAC paths to decide whether
    /// enough bookies have given a definitive response (Ok / NoEntry
    /// / NoLedger) that no length-W window of the round-robin
    /// schedule could still hide an entry committed by an
    /// ack-quorum-sized subset of unobserved bookies.
    pub fn new_coverage_set(&self) -> QuorumCoverageSet {
        QuorumCoverageSet::new(self.ensemble_size, self.write_quorum_size, self.ack_quorum_size)
    }
}

/// Categorisation of a bookie's response for coverage purposes.
/// Mirrors Apache BookKeeper's `RRQuorumCoverageSet.checkCovered`
/// special-casing of `BKException.Code.OK`,
/// `NoSuchEntryException`, and `NoSuchLedgerExistsException` as
/// "known" — every other status (or timeout) is "unknown" because
/// the bookie may still have accepted the entry and be unable to
/// tell us right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookieResponse {
    /// Bookie acknowledged the request — we know the contents.
    Ok,
    /// Bookie has the ledger but not this entry — definitive answer.
    NoEntry,
    /// Bookie does not have the ledger at all — definitive answer.
    NoLedger,
    /// Anything else: timeout, transport error, fenced reply,
    /// unauthorized, etc. The bookie's contents are unknown to us.
    Other,
    /// Default state before any response has been recorded for this
    /// ensemble index — also "unknown".
    Uninitialized,
}

impl BookieResponse {
    /// Whether this response is a definitive ("known") answer about
    /// the bookie's contents. Per Java: Ok or NoEntry or NoLedger.
    pub fn is_known(self) -> bool {
        matches!(self, Self::Ok | Self::NoEntry | Self::NoLedger)
    }
}

/// Per-bookie response tracker that decides when a fence / read-LAC
/// has heard from enough bookies to rule out any
/// ack-quorum-sized subset committing an entry our scan never saw.
///
/// Mirrors `RoundRobinDistributionSchedule.RRQuorumCoverageSet`. The
/// naive "count >= write_quorum" check the previous code used is
/// both too strict in some shapes (E=W common case rejects fences
/// Java would accept on W-1 known responses) and too lax in others
/// (E>W striped case can declare coverage while leaving an entire
/// ack-quorum-sized window unobserved — silent ledger truncation if
/// recovery proceeds).
#[derive(Debug, Clone)]
pub struct QuorumCoverageSet {
    ensemble_size: usize,
    write_quorum_size: usize,
    ack_quorum_size: usize,
    covered: Vec<BookieResponse>,
}

impl QuorumCoverageSet {
    pub fn new(ensemble_size: usize, write_quorum_size: usize, ack_quorum_size: usize) -> Self {
        Self {
            ensemble_size,
            write_quorum_size,
            ack_quorum_size,
            covered: vec![BookieResponse::Uninitialized; ensemble_size],
        }
    }

    /// Record `response` against the bookie at ensemble index
    /// `bookie_index`. Idempotent — re-recording overwrites the
    /// prior value (matches Java's `addBookie`).
    pub fn add_bookie(&mut self, bookie_index: usize, response: BookieResponse) {
        if let Some(slot) = self.covered.get_mut(bookie_index) {
            *slot = response;
        }
    }

    /// Returns `true` iff for every length-W window starting at any
    /// ensemble index, the count of bookies whose response is NOT
    /// known is strictly less than the ack quorum. Equivalent to
    /// "there is no ack-quorum-sized subset of bookies that could
    /// have together accepted an entry we never observed".
    pub fn is_covered(&self) -> bool {
        for i in 0..self.ensemble_size {
            let mut nodes_unknown = 0;
            for j in 0..self.write_quorum_size {
                let node_index = (i + j) % self.ensemble_size;
                if !self.covered[node_index].is_known() {
                    nodes_unknown += 1;
                }
            }
            if nodes_unknown >= self.ack_quorum_size {
                return false;
            }
        }
        true
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
