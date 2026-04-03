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

    /// Create a new AckSet for tracking write acknowledgments.
    pub fn new_ack_set(&self) -> AckSet {
        AckSet::new(self.write_quorum_size, self.ack_quorum_size)
    }
}

/// Tracks which bookies in a write set have acknowledged.
#[derive(Debug)]
pub struct AckSet {
    write_quorum_size: usize,
    ack_quorum_size: usize,
    ack_count: usize,
    fail_count: usize,
}

impl AckSet {
    pub fn new(write_quorum_size: usize, ack_quorum_size: usize) -> Self {
        Self { write_quorum_size, ack_quorum_size, ack_count: 0, fail_count: 0 }
    }

    /// Record a successful ack. Returns true if ack quorum is now reached.
    pub fn complete_bookie(&mut self, _bookie_index: usize) -> bool {
        self.ack_count += 1;
        self.ack_count >= self.ack_quorum_size
    }

    /// Record a failure. Returns true if ack quorum is now impossible.
    pub fn fail_bookie(&mut self, _bookie_index: usize) -> bool {
        self.fail_count += 1;
        self.fail_count > (self.write_quorum_size - self.ack_quorum_size)
    }

    pub fn is_quorum_reached(&self) -> bool {
        self.ack_count >= self.ack_quorum_size
    }
}
