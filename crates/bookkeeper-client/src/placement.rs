//! Ensemble placement policies matching Java's EnsemblePlacementPolicy.
//!
//! Determines how bookies are selected for ledger ensembles and replacements,
//! with optional rack-awareness for fault domain isolation.

use std::collections::{HashMap, HashSet};

/// Result of a placement operation.
pub struct PlacementResult {
    pub bookies: Vec<String>,
    pub adheres_to_policy: bool,
}

/// Trait for ensemble placement policies.
/// Matches Java's EnsemblePlacementPolicy interface.
pub trait EnsemblePlacementPolicy: Send + Sync {
    /// Select bookies for a new ensemble.
    fn new_ensemble(
        &self,
        ensemble_size: usize,
        write_quorum_size: usize,
        ack_quorum_size: usize,
        exclude: &HashSet<String>,
    ) -> Result<PlacementResult, PlacementError>;

    /// Select a replacement for a failed bookie in an existing ensemble.
    fn replace_bookie(
        &self,
        ensemble_size: usize,
        write_quorum_size: usize,
        ack_quorum_size: usize,
        current_ensemble: &[String],
        bookie_to_replace: &str,
        exclude: &HashSet<String>,
    ) -> Result<String, PlacementError>;

    /// Update the set of available bookies.
    fn on_cluster_changed(&mut self, writable: &[String], readonly: &[String]);
}

#[derive(Debug, thiserror::Error)]
pub enum PlacementError {
    #[error("not enough bookies: need {needed}, available {available}")]
    NotEnoughBookies { needed: usize, available: usize },
}

// ---------------------------------------------------------------------------
// Default policy: random selection (no rack awareness)
// Matches Java's DefaultEnsemblePlacementPolicy
// ---------------------------------------------------------------------------

pub struct DefaultPlacementPolicy {
    writable_bookies: Vec<String>,
}

impl DefaultPlacementPolicy {
    pub fn new() -> Self {
        Self {
            writable_bookies: Vec::new(),
        }
    }
}

impl EnsemblePlacementPolicy for DefaultPlacementPolicy {
    fn new_ensemble(
        &self,
        ensemble_size: usize,
        _write_quorum_size: usize,
        _ack_quorum_size: usize,
        exclude: &HashSet<String>,
    ) -> Result<PlacementResult, PlacementError> {
        let available: Vec<&String> = self
            .writable_bookies
            .iter()
            .filter(|b| !exclude.contains(*b))
            .collect();
        if available.len() < ensemble_size {
            return Err(PlacementError::NotEnoughBookies {
                needed: ensemble_size,
                available: available.len(),
            });
        }
        Ok(PlacementResult {
            bookies: available[..ensemble_size].iter().map(|s| s.to_string()).collect(),
            adheres_to_policy: true,
        })
    }

    fn replace_bookie(
        &self,
        _ensemble_size: usize,
        _write_quorum_size: usize,
        _ack_quorum_size: usize,
        current_ensemble: &[String],
        _bookie_to_replace: &str,
        exclude: &HashSet<String>,
    ) -> Result<String, PlacementError> {
        let current_set: HashSet<&str> = current_ensemble.iter().map(|s| s.as_str()).collect();
        for b in &self.writable_bookies {
            if !current_set.contains(b.as_str()) && !exclude.contains(b) {
                return Ok(b.clone());
            }
        }
        Err(PlacementError::NotEnoughBookies {
            needed: 1,
            available: 0,
        })
    }

    fn on_cluster_changed(&mut self, writable: &[String], _readonly: &[String]) {
        self.writable_bookies = writable.to_vec();
    }
}

// ---------------------------------------------------------------------------
// Rack-aware policy: spread bookies across racks
// Matches Java's RackawareEnsemblePlacementPolicyImpl
// ---------------------------------------------------------------------------

/// Maps bookie addresses to rack paths.
/// Default: all bookies in "/default-region/default-rack".
pub trait RackResolver: Send + Sync {
    fn resolve(&self, bookie: &str) -> String;
}

/// Static rack mapping from config.
pub struct StaticRackResolver {
    mapping: HashMap<String, String>,
    default_rack: String,
}

impl StaticRackResolver {
    pub fn new(mapping: HashMap<String, String>) -> Self {
        Self {
            mapping,
            default_rack: "/default-region/default-rack".to_string(),
        }
    }
}

impl RackResolver for StaticRackResolver {
    fn resolve(&self, bookie: &str) -> String {
        self.mapping
            .get(bookie)
            .cloned()
            .unwrap_or_else(|| self.default_rack.clone())
    }
}

/// Rack-aware ensemble placement policy.
/// Ensures write quorums span multiple racks for fault domain isolation.
pub struct RackAwarePlacementPolicy {
    writable_bookies: Vec<String>,
    resolver: Box<dyn RackResolver>,
    min_racks_per_write_quorum: usize,
    enforce_min_racks: bool,
}

impl RackAwarePlacementPolicy {
    pub fn new(
        resolver: Box<dyn RackResolver>,
        min_racks_per_write_quorum: usize,
        enforce_min_racks: bool,
    ) -> Self {
        Self {
            writable_bookies: Vec::new(),
            resolver,
            min_racks_per_write_quorum: min_racks_per_write_quorum.max(1),
            enforce_min_racks,
        }
    }

    /// Get the rack for a bookie.
    fn rack_of(&self, bookie: &str) -> String {
        self.resolver.resolve(bookie)
    }

    /// Count distinct racks in a set of bookies.
    fn count_racks(&self, bookies: &[String]) -> usize {
        let racks: HashSet<String> = bookies.iter().map(|b| self.rack_of(b)).collect();
        racks.len()
    }

    /// Group available bookies by rack.
    fn bookies_by_rack(&self, exclude: &HashSet<String>) -> HashMap<String, Vec<String>> {
        let mut by_rack: HashMap<String, Vec<String>> = HashMap::new();
        for b in &self.writable_bookies {
            if !exclude.contains(b) {
                let rack = self.rack_of(b);
                by_rack.entry(rack).or_default().push(b.clone());
            }
        }
        by_rack
    }

    /// Check if a write quorum window of the ensemble spans enough racks.
    fn check_write_quorum_coverage(
        &self,
        ensemble: &[String],
        write_quorum_size: usize,
    ) -> bool {
        if ensemble.len() < write_quorum_size {
            return true; // not enough bookies yet to check
        }
        for start in 0..ensemble.len() {
            let mut racks = HashSet::new();
            for i in 0..write_quorum_size {
                let idx = (start + i) % ensemble.len();
                racks.insert(self.rack_of(&ensemble[idx]));
            }
            if racks.len() < self.min_racks_per_write_quorum {
                return false;
            }
        }
        true
    }
}

impl EnsemblePlacementPolicy for RackAwarePlacementPolicy {
    fn new_ensemble(
        &self,
        ensemble_size: usize,
        write_quorum_size: usize,
        _ack_quorum_size: usize,
        exclude: &HashSet<String>,
    ) -> Result<PlacementResult, PlacementError> {
        let by_rack = self.bookies_by_rack(exclude);
        let num_racks = by_rack.len();
        let total_available: usize = by_rack.values().map(|v| v.len()).sum();

        if total_available < ensemble_size {
            return Err(PlacementError::NotEnoughBookies {
                needed: ensemble_size,
                available: total_available,
            });
        }

        // If only 1 rack and enforcement enabled, reject
        if num_racks < self.min_racks_per_write_quorum && self.enforce_min_racks {
            return Err(PlacementError::NotEnoughBookies {
                needed: ensemble_size,
                available: total_available,
            });
        }

        // Round-robin across racks to spread bookies.
        // Matches Java's RRTopologyAwareCoverageEnsemble approach.
        let mut ensemble: Vec<String> = Vec::with_capacity(ensemble_size);
        let mut rack_names: Vec<String> = by_rack.keys().cloned().collect();
        rack_names.sort(); // deterministic order
        let mut rack_idx = 0;
        let mut rack_offsets: HashMap<String, usize> = HashMap::new();

        while ensemble.len() < ensemble_size {
            let rack = &rack_names[rack_idx % rack_names.len()];
            let offset = rack_offsets.entry(rack.clone()).or_insert(0);
            let bookies_in_rack = &by_rack[rack];

            if *offset < bookies_in_rack.len() {
                let bookie = &bookies_in_rack[*offset];
                if !ensemble.contains(bookie) {
                    ensemble.push(bookie.clone());
                }
                *offset += 1;
            }
            rack_idx += 1;

            // Safety: prevent infinite loop if we can't find enough
            if rack_idx > ensemble_size * rack_names.len() * 2 {
                break;
            }
        }

        if ensemble.len() < ensemble_size {
            return Err(PlacementError::NotEnoughBookies {
                needed: ensemble_size,
                available: ensemble.len(),
            });
        }

        let adheres = self.check_write_quorum_coverage(&ensemble, write_quorum_size);
        Ok(PlacementResult {
            bookies: ensemble,
            adheres_to_policy: adheres,
        })
    }

    fn replace_bookie(
        &self,
        _ensemble_size: usize,
        write_quorum_size: usize,
        _ack_quorum_size: usize,
        current_ensemble: &[String],
        bookie_to_replace: &str,
        exclude: &HashSet<String>,
    ) -> Result<String, PlacementError> {
        let current_set: HashSet<&str> = current_ensemble.iter().map(|s| s.as_str()).collect();
        let replace_rack = self.rack_of(bookie_to_replace);

        // Collect neighboring racks to avoid (for rack diversity)
        let replace_idx = current_ensemble
            .iter()
            .position(|b| b == bookie_to_replace)
            .unwrap_or(0);
        let mut neighbor_racks: HashSet<String> = HashSet::new();
        for offset in 1..write_quorum_size {
            let before = (replace_idx + current_ensemble.len() - offset) % current_ensemble.len();
            let after = (replace_idx + offset) % current_ensemble.len();
            neighbor_racks.insert(self.rack_of(&current_ensemble[before]));
            neighbor_racks.insert(self.rack_of(&current_ensemble[after]));
        }

        // Prefer a bookie from a DIFFERENT rack than the failed bookie and neighbors
        let mut candidates: Vec<&String> = self
            .writable_bookies
            .iter()
            .filter(|b| {
                !current_set.contains(b.as_str())
                    && !exclude.contains(*b)
                    && self.rack_of(b) != replace_rack
                    && !neighbor_racks.contains(&self.rack_of(b))
            })
            .collect();

        if let Some(best) = candidates.first() {
            return Ok(best.to_string());
        }

        // Fallback: different rack from the failed bookie only
        candidates = self
            .writable_bookies
            .iter()
            .filter(|b| {
                !current_set.contains(b.as_str())
                    && !exclude.contains(*b)
                    && self.rack_of(b) != replace_rack
            })
            .collect();

        if let Some(best) = candidates.first() {
            return Ok(best.to_string());
        }

        // Last resort: any available bookie
        for b in &self.writable_bookies {
            if !current_set.contains(b.as_str()) && !exclude.contains(b) {
                return Ok(b.clone());
            }
        }

        Err(PlacementError::NotEnoughBookies {
            needed: 1,
            available: 0,
        })
    }

    fn on_cluster_changed(&mut self, writable: &[String], _readonly: &[String]) {
        self.writable_bookies = writable.to_vec();
    }
}
