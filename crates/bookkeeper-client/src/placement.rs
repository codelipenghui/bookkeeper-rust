//! Ensemble placement policies matching Java's EnsemblePlacementPolicy.
//!
//! Determines how bookies are selected for ledger ensembles and replacements,
//! with optional rack-awareness for fault domain isolation.

use std::collections::{HashMap, HashSet};
use rand::seq::SliceRandom;
use rand::RngExt;

/// Placement policy adherence level matching Java's PlacementPolicyAdherence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyAdherence {
    /// Placement failed to meet policy requirements.
    Fail,
    /// Placement meets soft requirements (e.g., degraded rack availability).
    MeetsSoft,
    /// Placement fully meets all policy requirements.
    MeetsStrict,
}

/// Result of a placement operation.
pub struct PlacementResult {
    pub bookies: Vec<String>,
    pub adherence: PolicyAdherence,
}

/// Per-bookie disk info used for weighted placement.
/// Matches Java's `BookieInfoReader.BookieInfo` which implements `WeightedObject`.
/// The weight for selection is `free_disk_space`.
#[derive(Debug, Clone, Copy)]
pub struct BookieInfo {
    pub free_disk_space: i64,
    pub total_disk_capacity: i64,
}

impl BookieInfo {
    pub fn new(free_disk_space: i64, total_disk_capacity: i64) -> Self {
        Self { free_disk_space, total_disk_capacity }
    }

    /// Weight used for weighted random selection. Matches Java's `getWeight()`.
    pub fn weight(&self) -> u64 {
        self.free_disk_space.max(0) as u64
    }
}

/// Time-expiring quarantine cache for recently-failed bookies.
///
/// Matches Java's `BookieWatcherImpl.quarantinedBookies` — a Guava
/// `Cache<BookieId, Object>` with a configurable expireAfterWrite.
/// Quarantined bookies are excluded from new ensemble selections until
/// their quarantine expires, giving a failed bookie time to recover
/// before the client attempts to use it again.
///
/// Shared via `Arc` across the client and the placement policy so both
/// `BookieWatcher` (health checks) and `EnsemblePlacementPolicy`
/// (selection) see the same set.
#[derive(Debug)]
pub struct BookieQuarantine {
    entries: parking_lot::Mutex<HashMap<String, std::time::Instant>>,
    ttl: std::time::Duration,
}

impl BookieQuarantine {
    /// Create a new quarantine cache with the given TTL.
    /// Matches Java's `bookieQuarantineTimeSeconds` config (default 1800s = 30m).
    pub fn new(ttl: std::time::Duration) -> Self {
        Self {
            entries: parking_lot::Mutex::new(HashMap::new()),
            ttl,
        }
    }

    /// Add a bookie to the quarantine. Any existing entry is refreshed.
    pub fn quarantine(&self, bookie: &str) {
        self.entries.lock().insert(bookie.to_string(), std::time::Instant::now());
    }

    /// Remove a bookie from the quarantine (e.g., after it's confirmed healthy).
    pub fn release(&self, bookie: &str) {
        self.entries.lock().remove(bookie);
    }

    /// Is this bookie currently quarantined? Also evicts the entry if expired.
    pub fn is_quarantined(&self, bookie: &str) -> bool {
        let mut map = self.entries.lock();
        if let Some(&added) = map.get(bookie) {
            if added.elapsed() < self.ttl {
                return true;
            }
            map.remove(bookie);
        }
        false
    }

    /// Collect all currently quarantined bookies (non-expired).
    /// Used by `EnsemblePlacementPolicy::new_ensemble` to exclude them.
    pub fn quarantined_set(&self) -> HashSet<String> {
        let mut map = self.entries.lock();
        let ttl = self.ttl;
        map.retain(|_, added| added.elapsed() < ttl);
        map.keys().cloned().collect()
    }

    /// Current number of quarantined bookies (after expiry sweep).
    pub fn len(&self) -> usize {
        self.quarantined_set().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
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

    /// Update per-bookie disk info for weighted placement.
    /// Called by `BookieInfoReader` after polling all bookies.
    /// Matches Java's `EnsemblePlacementPolicy.updateBookieInfo(map)`.
    fn update_bookie_info(&mut self, _info: &HashMap<String, BookieInfo>) {}

    /// Check if an existing ensemble adheres to the placement policy.
    /// Default returns MeetsStrict. Override for policies with validation.
    /// Matches Java's `isEnsembleAdheringToPlacementPolicy`.
    fn is_ensemble_adhering(
        &self,
        _ensemble: &[String],
        _write_quorum_size: usize,
        _ack_quorum_size: usize,
    ) -> PolicyAdherence {
        PolicyAdherence::MeetsStrict
    }

    /// Reorder the read sequence for a given write set, putting healthier
    /// bookies first. Called by the read path when `reorderReadSequenceEnabled=true`.
    /// Matches Java's `EnsemblePlacementPolicy.reorderReadSequence()`.
    ///
    /// `failed_bookies` contains addresses of bookies known to have errors.
    /// The default implementation moves failed bookies to the end.
    fn reorder_read_sequence(
        &self,
        ensemble: &[String],
        write_set: &[usize],
        failed_bookies: &HashSet<String>,
    ) -> Vec<usize> {
        if failed_bookies.is_empty() {
            return write_set.to_vec();
        }
        // Partition: healthy first, failed last
        let mut healthy = Vec::new();
        let mut failed = Vec::new();
        for &idx in write_set {
            if idx < ensemble.len() && failed_bookies.contains(&ensemble[idx]) {
                failed.push(idx);
            } else {
                healthy.push(idx);
            }
        }
        healthy.extend(failed);
        healthy
    }

    /// Check if the set of bookies that have acknowledged a write spans
    /// enough distinct fault domains (racks/regions) to satisfy the policy.
    /// Used with `enforceMinNumFaultDomainsForWrite` to delay write completion
    /// until sufficient domain diversity is achieved.
    /// Matches Java's `areAckedBookiesAdheringToPlacementPolicy`.
    fn are_acked_bookies_adhering(
        &self,
        _acked_bookies: &HashSet<String>,
        _write_quorum_size: usize,
        _ack_quorum_size: usize,
    ) -> bool {
        true // Default: always passes (no topology constraints)
    }

    /// Attach a bookie quarantine cache so recently-failed bookies are
    /// excluded from new ensemble selections. Matches Java's
    /// `BookieWatcherImpl.quarantinedBookies` wiring.
    ///
    /// Default is a no-op — policies that want to honor quarantine should
    /// override this and consult the quarantine in `new_ensemble` /
    /// `replace_bookie` via the `exclude` set.
    fn set_quarantine(&mut self, _quarantine: std::sync::Arc<BookieQuarantine>) {}
}

#[derive(Debug, thiserror::Error)]
pub enum PlacementError {
    #[error("not enough bookies: need {needed}, available {available}")]
    NotEnoughBookies { needed: usize, available: usize },
}

// ---------------------------------------------------------------------------
// Weighted random selection
// Matches Java's WeightedRandomSelection: probability proportional to weight
// (free disk space), clamped by `max_multiple` to avoid extreme skew.
// ---------------------------------------------------------------------------

/// Select `n` bookies from `candidates` weighted by their disk info.
/// `max_multiple` caps how much more likely the heaviest bookie is chosen
/// relative to the lightest (matches Java's `maxWeightMultiple`).
/// Falls back to uniform selection when no weights are available.
fn weighted_select<'a>(
    candidates: &[&'a String],
    info: &HashMap<String, BookieInfo>,
    n: usize,
    max_multiple: u32,
) -> Vec<&'a String> {
    if candidates.is_empty() || n == 0 {
        return Vec::new();
    }

    let weights: Vec<u64> = candidates
        .iter()
        .map(|b| info.get(b.as_str()).map_or(1, |i| i.weight().max(1)))
        .collect();

    let min_w = *weights.iter().min().unwrap();
    // Clamp: no bookie has weight > max_multiple * min_weight.
    let cap = min_w.saturating_mul(max_multiple as u64);
    let clamped: Vec<u64> = weights.iter().map(|&w| w.min(cap)).collect();
    let total: u64 = clamped.iter().sum();

    let mut rng = rand::rng();
    let mut selected: Vec<&'a String> = Vec::with_capacity(n);
    let mut used: HashSet<usize> = HashSet::new();

    while selected.len() < n && used.len() < candidates.len() {
        let mut r = rng.random_range(0..total);
        for (i, &w) in clamped.iter().enumerate() {
            if used.contains(&i) {
                continue;
            }
            if r < w {
                selected.push(candidates[i]);
                used.insert(i);
                break;
            }
            r -= w;
        }
    }
    selected
}

// ---------------------------------------------------------------------------
// Default policy: random selection (no rack awareness)
// Matches Java's DefaultEnsemblePlacementPolicy
// ---------------------------------------------------------------------------

pub struct DefaultPlacementPolicy {
    writable_bookies: Vec<String>,
    /// Per-bookie disk info for weighted placement. Empty = uniform selection.
    bookie_info: HashMap<String, BookieInfo>,
    /// Max weight multiple. Matches Java's `bookieMaxWeightMultipleForWeightBasedPlacement`.
    max_weight_multiple: u32,
    /// Quarantined bookies cache — when attached, these are excluded from
    /// new ensemble selections and replacements.
    quarantine: Option<std::sync::Arc<BookieQuarantine>>,
}

impl DefaultPlacementPolicy {
    pub fn new() -> Self {
        Self {
            writable_bookies: Vec::new(),
            bookie_info: HashMap::new(),
            max_weight_multiple: 3,
            quarantine: None,
        }
    }

    /// Set the max weight multiple for disk-weight selection.
    pub fn set_max_weight_multiple(&mut self, multiple: u32) {
        self.max_weight_multiple = multiple.max(1);
    }

    /// Build the effective exclude set: caller's exclude ∪ current quarantine.
    fn effective_exclude(&self, exclude: &HashSet<String>) -> HashSet<String> {
        let mut out = exclude.clone();
        if let Some(ref q) = self.quarantine {
            out.extend(q.quarantined_set());
        }
        out
    }
}

impl EnsemblePlacementPolicy for DefaultPlacementPolicy {
    fn set_quarantine(&mut self, quarantine: std::sync::Arc<BookieQuarantine>) {
        self.quarantine = Some(quarantine);
    }

    fn new_ensemble(
        &self,
        ensemble_size: usize,
        _write_quorum_size: usize,
        _ack_quorum_size: usize,
        exclude: &HashSet<String>,
    ) -> Result<PlacementResult, PlacementError> {
        let exclude = self.effective_exclude(exclude);
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
        let selected = if self.bookie_info.is_empty() {
            available[..ensemble_size].to_vec()
        } else {
            let picked = weighted_select(&available, &self.bookie_info, ensemble_size, self.max_weight_multiple);
            if picked.len() < ensemble_size {
                available[..ensemble_size].to_vec()
            } else {
                picked
            }
        };
        Ok(PlacementResult {
            bookies: selected.into_iter().map(|s| s.to_string()).collect(),
            adherence: PolicyAdherence::MeetsStrict,
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
        let exclude = self.effective_exclude(exclude);
        let current_set: HashSet<&str> = current_ensemble.iter().map(|s| s.as_str()).collect();
        let candidates: Vec<&String> = self
            .writable_bookies
            .iter()
            .filter(|b| !current_set.contains(b.as_str()) && !exclude.contains(*b))
            .collect();
        if candidates.is_empty() {
            return Err(PlacementError::NotEnoughBookies { needed: 1, available: 0 });
        }
        if self.bookie_info.is_empty() {
            return Ok(candidates[0].clone());
        }
        let picked = weighted_select(&candidates, &self.bookie_info, 1, self.max_weight_multiple);
        Ok(picked.first().unwrap_or(&candidates[0]).to_string())
    }

    fn on_cluster_changed(&mut self, writable: &[String], _readonly: &[String]) {
        self.writable_bookies = writable.to_vec();
    }

    fn update_bookie_info(&mut self, info: &HashMap<String, BookieInfo>) {
        self.bookie_info = info.clone();
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
#[derive(Clone)]
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

/// Dynamic rack resolver that executes an external command (script) to map
/// bookie addresses to network locations. Matches Java's `ScriptBasedMapping`
/// which extends `CachedDNSToSwitchMapping`.
///
/// The command is invoked as `<script> <bookie_host>` and should print the
/// rack path (e.g., `/region-a/rack-1`) on stdout.
///
/// Results are cached in a `HashMap` (matching Java's `CachedDNSToSwitchMapping`
/// which uses a `ConcurrentHashMap` with no TTL).
///
/// Config keys (matching Java):
/// - `networkTopologyScriptFileName`: path to the resolver script
pub struct CommandBasedRackResolver {
    script_path: String,
    cache: std::sync::Mutex<HashMap<String, String>>,
    default_rack: String,
}

impl CommandBasedRackResolver {
    pub fn new(script_path: String) -> Self {
        Self {
            script_path,
            cache: std::sync::Mutex::new(HashMap::new()),
            default_rack: "/default-region/default-rack".to_string(),
        }
    }

    /// Execute the resolver script for a given bookie hostname.
    fn execute_script(&self, bookie_host: &str) -> Option<String> {
        match std::process::Command::new(&self.script_path)
            .arg(bookie_host)
            .output()
        {
            Ok(output) if output.status.success() => {
                let rack = String::from_utf8_lossy(&output.stdout).trim().to_string();
                if rack.is_empty() {
                    tracing::warn!(
                        "rack resolver script returned empty for {bookie_host}, using default"
                    );
                    None
                } else {
                    Some(rack)
                }
            }
            Ok(output) => {
                tracing::warn!(
                    "rack resolver script failed for {bookie_host}: exit={}, stderr={}",
                    output.status,
                    String::from_utf8_lossy(&output.stderr).trim()
                );
                None
            }
            Err(e) => {
                tracing::warn!("rack resolver script exec error for {bookie_host}: {e}");
                None
            }
        }
    }

    /// Clear the cache, forcing the next resolve to re-execute the script.
    /// Matches Java's `CachedDNSToSwitchMapping.reloadCachedMappings()`.
    pub fn reload_cached_mappings(&self) {
        self.cache.lock().unwrap().clear();
    }
}

impl RackResolver for CommandBasedRackResolver {
    fn resolve(&self, bookie: &str) -> String {
        // Extract hostname from "host:port" address
        let host = bookie.split(':').next().unwrap_or(bookie);

        // Check cache first (matching Java's ConcurrentHashMap cache)
        if let Some(cached) = self.cache.lock().unwrap().get(host) {
            return cached.clone();
        }

        // Execute script and cache result
        let rack = self.execute_script(host).unwrap_or_else(|| self.default_rack.clone());
        self.cache.lock().unwrap().insert(host.to_string(), rack.clone());
        rack
    }
}

/// Rack-aware ensemble placement policy.
/// Ensures write quorums span multiple racks for fault domain isolation.
pub struct RackAwarePlacementPolicy {
    writable_bookies: Vec<String>,
    resolver: Box<dyn RackResolver>,
    min_racks_per_write_quorum: usize,
    enforce_min_racks: bool,
    bookie_info: HashMap<String, BookieInfo>,
    max_weight_multiple: u32,
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
            bookie_info: HashMap::new(),
            max_weight_multiple: 3,
        }
    }

    pub fn set_max_weight_multiple(&mut self, multiple: u32) {
        self.max_weight_multiple = multiple.max(1);
    }

    /// Get the rack for a bookie.
    fn rack_of(&self, bookie: &str) -> String {
        self.resolver.resolve(bookie)
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

        // Round-robin across racks with random selection within each rack.
        // Matches Java's topology-aware selection with randomization.
        let mut ensemble: Vec<String> = Vec::with_capacity(ensemble_size);
        let mut rack_names: Vec<String> = by_rack.keys().cloned().collect();
        let mut rng = rand::rng();
        rack_names.shuffle(&mut rng);

        // Order bookies within each rack: weighted by disk info when available,
        // otherwise random shuffle (matching Java's selectRandomFromRack).
        let mut shuffled_by_rack: HashMap<String, Vec<String>> = HashMap::new();
        for (rack, bookies) in by_rack {
            let ordered = if self.bookie_info.is_empty() {
                let mut bs = bookies;
                bs.shuffle(&mut rng);
                bs
            } else {
                let refs: Vec<&String> = bookies.iter().collect();
                let picked = weighted_select(&refs, &self.bookie_info, refs.len(), self.max_weight_multiple);
                picked.into_iter().cloned().collect()
            };
            shuffled_by_rack.insert(rack, ordered);
        }

        let mut rack_idx = 0;
        let mut rack_offsets: HashMap<String, usize> = HashMap::new();

        while ensemble.len() < ensemble_size {
            let rack = &rack_names[rack_idx % rack_names.len()];
            let offset = rack_offsets.entry(rack.clone()).or_insert(0);
            if let Some(bookies_in_rack) = shuffled_by_rack.get(rack) {
                if *offset < bookies_in_rack.len() {
                    let bookie = &bookies_in_rack[*offset];
                    if !ensemble.contains(bookie) {
                        ensemble.push(bookie.clone());
                    }
                    *offset += 1;
                }
            }
            rack_idx += 1;

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

        let adherence = if self.check_write_quorum_coverage(&ensemble, write_quorum_size) {
            PolicyAdherence::MeetsStrict
        } else if num_racks < self.min_racks_per_write_quorum {
            PolicyAdherence::MeetsSoft
        } else {
            PolicyAdherence::Fail
        };

        Ok(PlacementResult {
            bookies: ensemble,
            adherence,
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

        // Last resort: any available bookie. Even here, prefer one in
        // a rack that's already absent from the current ensemble — that
        // preserves SOME rack diversity instead of compounding the
        // failure by picking a same-rack peer of the failed bookie.
        // Pre-fix this loop returned the first writable_bookies entry,
        // which (with deterministic ordering) tended to cluster
        // replacements onto a single rack under repeated failures.
        let current_racks: HashSet<String> = current_ensemble
            .iter()
            .map(|b| self.rack_of(b))
            .collect();
        for b in &self.writable_bookies {
            if !current_set.contains(b.as_str())
                && !exclude.contains(b)
                && !current_racks.contains(&self.rack_of(b))
            {
                return Ok(b.clone());
            }
        }
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

    fn update_bookie_info(&mut self, info: &HashMap<String, BookieInfo>) {
        self.bookie_info = info.clone();
    }

    /// Check if acked bookies span enough distinct racks.
    /// Matches Java's RackawareEnsemblePlacementPolicy.areAckedBookiesAdheringToPlacementPolicy:
    /// returns true if the set of acked bookies covers >= minNumRacksPerWriteQuorum racks.
    fn are_acked_bookies_adhering(
        &self,
        acked_bookies: &HashSet<String>,
        _write_quorum_size: usize,
        _ack_quorum_size: usize,
    ) -> bool {
        let mut racks: HashSet<String> = HashSet::new();
        for bookie in acked_bookies {
            racks.insert(self.rack_of(bookie));
        }
        racks.len() >= self.min_racks_per_write_quorum
    }

    /// Trait override for rack-aware adherence.
    ///
    /// Without this override the trait method falls through to the
    /// default `MeetsStrict`, silently disabling the auditor's
    /// `RackawareEnsemblePlacementPolicyImpl.isEnsembleAdheringToPlacementPolicy`
    /// equivalent — every rack-aware violation passed audit.
    fn is_ensemble_adhering(
        &self,
        ensemble: &[String],
        write_quorum_size: usize,
        _ack_quorum_size: usize,
    ) -> PolicyAdherence {
        if self.check_write_quorum_coverage(ensemble, write_quorum_size) {
            PolicyAdherence::MeetsStrict
        } else {
            // Distinct racks regardless of write-quorum coverage:
            // 2+ racks but failing per-quorum coverage is "soft", a
            // single rack is "fail". Mirrors Java's tri-state.
            let mut racks: HashSet<String> = HashSet::new();
            for b in ensemble {
                racks.insert(self.rack_of(b));
            }
            if racks.len() >= 2 {
                PolicyAdherence::MeetsSoft
            } else {
                PolicyAdherence::Fail
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Region-aware policy: spread bookies across regions, then racks within regions.
// Matches Java's RegionAwareEnsemblePlacementPolicy.
// ---------------------------------------------------------------------------

/// Extract region from a rack path. Java uses REGIONID_DISTANCE_FROM_LEAVES = 2,
/// meaning the first component of "/region/rack" is the region.
fn extract_region(rack_path: &str) -> &str {
    let parts: Vec<&str> = rack_path.split('/').filter(|s| !s.is_empty()).collect();
    if parts.is_empty() { "UnknownRegion" } else { parts[0] }
}

/// Region-aware ensemble placement policy.
/// Matches Java's `RegionAwareEnsemblePlacementPolicy`.
pub struct RegionAwarePlacementPolicy {
    writable_bookies: Vec<String>,
    resolver: Box<dyn RackResolver>,
    min_regions_for_durability: usize,
    enforce_durability: bool,
}

impl RegionAwarePlacementPolicy {
    pub fn new(resolver: Box<dyn RackResolver>, min_regions: usize, enforce: bool) -> Self {
        Self { writable_bookies: Vec::new(), resolver, min_regions_for_durability: min_regions.max(1), enforce_durability: enforce }
    }

    fn region_of(&self, bookie: &str) -> String {
        extract_region(&self.resolver.resolve(bookie)).to_string()
    }

    fn bookies_by_region(&self, exclude: &HashSet<String>) -> HashMap<String, Vec<String>> {
        let mut m: HashMap<String, Vec<String>> = HashMap::new();
        for b in &self.writable_bookies {
            if !exclude.contains(b) { m.entry(self.region_of(b)).or_default().push(b.clone()); }
        }
        m
    }

    /// Check if an ensemble adheres to the placement policy.
    pub fn is_ensemble_adhering(&self, ensemble: &[String]) -> PolicyAdherence {
        let regions: HashSet<String> = ensemble.iter().map(|b| self.region_of(b)).collect();
        if regions.len() >= self.min_regions_for_durability { PolicyAdherence::MeetsStrict }
        else if regions.len() >= 2 { PolicyAdherence::MeetsSoft }
        else { PolicyAdherence::Fail }
    }
}

impl EnsemblePlacementPolicy for RegionAwarePlacementPolicy {
    fn new_ensemble(&self, ensemble_size: usize, _wq: usize, _aq: usize, exclude: &HashSet<String>) -> Result<PlacementResult, PlacementError> {
        let by_region = self.bookies_by_region(exclude);
        let total: usize = by_region.values().map(|v| v.len()).sum();
        if total < ensemble_size { return Err(PlacementError::NotEnoughBookies { needed: ensemble_size, available: total }); }
        if by_region.len() < self.min_regions_for_durability && self.enforce_durability {
            return Err(PlacementError::NotEnoughBookies { needed: ensemble_size, available: total });
        }
        let mut ensemble = Vec::with_capacity(ensemble_size);
        let mut names: Vec<String> = by_region.keys().cloned().collect();
        let mut rng = rand::rng();
        names.shuffle(&mut rng);
        let mut shuffled: HashMap<String, Vec<String>> = HashMap::new();
        for (r, mut bs) in by_region { bs.shuffle(&mut rng); shuffled.insert(r, bs); }
        let mut idx = 0;
        let mut offs: HashMap<String, usize> = HashMap::new();
        while ensemble.len() < ensemble_size {
            let r = &names[idx % names.len()];
            let o = offs.entry(r.clone()).or_insert(0);
            if let Some(bs) = shuffled.get(r) {
                if *o < bs.len() && !ensemble.contains(&bs[*o]) { ensemble.push(bs[*o].clone()); }
                *o += 1;
            }
            idx += 1;
            if idx > ensemble_size * names.len() * 2 { break; }
        }
        if ensemble.len() < ensemble_size { return Err(PlacementError::NotEnoughBookies { needed: ensemble_size, available: ensemble.len() }); }
        let regions_used: HashSet<String> = ensemble.iter().map(|b| self.region_of(b)).collect();
        let adherence = if regions_used.len() >= self.min_regions_for_durability { PolicyAdherence::MeetsStrict } else { PolicyAdherence::MeetsSoft };
        Ok(PlacementResult { bookies: ensemble, adherence })
    }

    fn replace_bookie(&self, _es: usize, _wq: usize, _aq: usize, current: &[String], to_replace: &str, exclude: &HashSet<String>) -> Result<String, PlacementError> {
        let current_set: HashSet<&str> = current.iter().map(|s| s.as_str()).collect();
        let rr = self.region_of(to_replace);
        for b in &self.writable_bookies { if !current_set.contains(b.as_str()) && !exclude.contains(b) && self.region_of(b) == rr { return Ok(b.clone()); } }
        for b in &self.writable_bookies { if !current_set.contains(b.as_str()) && !exclude.contains(b) { return Ok(b.clone()); } }
        Err(PlacementError::NotEnoughBookies { needed: 1, available: 0 })
    }

    fn on_cluster_changed(&mut self, writable: &[String], _readonly: &[String]) { self.writable_bookies = writable.to_vec(); }

    /// Trait override for region-aware adherence.
    ///
    /// The inherent `is_ensemble_adhering(&[String])` (above) is what
    /// the implementation is built around, but until this override
    /// existed the trait dispatch fell through to the default no-op
    /// `MeetsStrict` — silently disabling the auditor's region-aware
    /// adherence check.
    fn is_ensemble_adhering(
        &self,
        ensemble: &[String],
        _write_quorum_size: usize,
        _ack_quorum_size: usize,
    ) -> PolicyAdherence {
        Self::is_ensemble_adhering(self, ensemble)
    }
}

/// Zone-aware placement policy — alias for RegionAware.
///
/// **KNOWN DIVERGENCE FROM JAVA**: Java's `ZoneawareEnsemblePlacementPolicyImpl`
/// is a *distinct* implementation from `RackawareEnsemblePlacementPolicyImpl`
/// with stronger guarantees:
/// - Enforces `desiredNumZonesPerWriteQuorum` AND `minimumNumZonesPerWriteQuorum`
///   at **each write-quorum window** (sliding across the ensemble), not just
///   globally across the ensemble.
/// - Enforces zone diversity at the **ack-quorum** level: a write is only
///   considered durable once it has been acked by bookies spanning the
///   minimum number of zones.
/// - Has a distinct `StabilizeNetworkTopology` mechanism that delays
///   removing zones from the topology to avoid flapping.
///
/// The Rust implementation aliases `ZoneAwarePlacementPolicy` to the
/// rack-aware implementation, which provides global rack/zone diversity
/// but does NOT enforce per-write-quorum-window zone diversity or
/// per-ack-quorum zone diversity.  Clusters that rely on strict zone
/// guarantees should run Java bookies for the client side until a full
/// port is added.
pub type ZoneAwarePlacementPolicy = RegionAwarePlacementPolicy;
