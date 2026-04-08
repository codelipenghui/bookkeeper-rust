//! Unit tests for `bookkeeper_client::placement`.
//!
//! These exercise the placement policies (`DefaultPlacementPolicy`,
//! `RackAwarePlacementPolicy`) and the `StaticRackResolver` directly,
//! without spinning up bookies or ZK. The rack-aware policy is the
//! highest-risk subject — recent fix commit 04f83710 patched
//! "randomization, 3-level adherence, proper adherence levels".
//!
//! Tests are written against invariants that hold across runs (rack
//! count, exclusion, "no duplicates") rather than specific orderings,
//! so they survive the policy's `rand::thread_rng()` shuffling.

use std::collections::{HashMap, HashSet};

use bookkeeper_client::placement::{
    DefaultPlacementPolicy, EnsemblePlacementPolicy, PlacementError, PolicyAdherence,
    RackAwarePlacementPolicy, RackResolver, StaticRackResolver,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn bookies(prefix: &str, n: usize) -> Vec<String> {
    (0..n).map(|i| format!("{prefix}{i}:3181")).collect()
}

fn empty_exclude() -> HashSet<String> {
    HashSet::new()
}

/// Build a `StaticRackResolver` from `&[(bookie, rack)]` for terse setup.
fn rack_resolver(pairs: &[(&str, &str)]) -> StaticRackResolver {
    let mut map = HashMap::new();
    for (bookie, rack) in pairs {
        map.insert((*bookie).to_string(), (*rack).to_string());
    }
    StaticRackResolver::new(map)
}

/// Count distinct racks in a result, given the same resolver layout used by
/// the policy. Reimplements the policy's internal helper so tests don't
/// depend on private API.
fn racks_in(result: &[String], resolver: &dyn RackResolver) -> HashSet<String> {
    result.iter().map(|b| resolver.resolve(b)).collect()
}

// ---------------------------------------------------------------------------
// DefaultPlacementPolicy
// ---------------------------------------------------------------------------

#[test]
fn default_policy_new_ensemble_returns_requested_size() {
    let mut p = DefaultPlacementPolicy::new();
    p.on_cluster_changed(&bookies("b", 5), &[]);

    let r = p.new_ensemble(3, 2, 2, &empty_exclude()).unwrap();
    assert_eq!(r.bookies.len(), 3);
    assert_eq!(r.adherence, PolicyAdherence::MeetsStrict);

    // No duplicates
    let unique: HashSet<_> = r.bookies.iter().collect();
    assert_eq!(unique.len(), 3);
}

#[test]
fn default_policy_new_ensemble_errors_when_insufficient_bookies() {
    let mut p = DefaultPlacementPolicy::new();
    p.on_cluster_changed(&bookies("b", 2), &[]);

    let result = p.new_ensemble(5, 2, 2, &empty_exclude());
    match result {
        Err(PlacementError::NotEnoughBookies { needed, available }) => {
            assert_eq!(needed, 5);
            assert_eq!(available, 2);
        }
        Ok(_) => panic!("expected NotEnoughBookies error"),
    }
}

#[test]
fn default_policy_new_ensemble_respects_exclude_set() {
    let mut p = DefaultPlacementPolicy::new();
    p.on_cluster_changed(&bookies("b", 5), &[]);

    let mut exclude = HashSet::new();
    exclude.insert("b0:3181".to_string());
    exclude.insert("b1:3181".to_string());

    let r = p.new_ensemble(3, 2, 2, &exclude).unwrap();
    assert_eq!(r.bookies.len(), 3);
    for excluded in &exclude {
        assert!(
            !r.bookies.contains(excluded),
            "result should not contain excluded bookie {excluded}"
        );
    }
}

#[test]
fn default_policy_replace_bookie_returns_non_current_bookie() {
    let mut p = DefaultPlacementPolicy::new();
    p.on_cluster_changed(&bookies("b", 5), &[]);

    let current = vec!["b0:3181".to_string(), "b1:3181".to_string(), "b2:3181".to_string()];
    let replacement = p
        .replace_bookie(3, 2, 2, &current, "b1:3181", &empty_exclude())
        .unwrap();
    assert!(!current.contains(&replacement));
}

#[test]
fn default_policy_replace_bookie_respects_exclude() {
    let mut p = DefaultPlacementPolicy::new();
    p.on_cluster_changed(&bookies("b", 5), &[]);

    let current = vec!["b0:3181".to_string(), "b1:3181".to_string(), "b2:3181".to_string()];
    let mut exclude = HashSet::new();
    exclude.insert("b3:3181".to_string());

    let replacement = p
        .replace_bookie(3, 2, 2, &current, "b1:3181", &exclude)
        .unwrap();
    assert_eq!(replacement, "b4:3181");
}

#[test]
fn default_policy_replace_bookie_errors_when_no_candidates() {
    let mut p = DefaultPlacementPolicy::new();
    p.on_cluster_changed(&bookies("b", 3), &[]);

    let current = vec!["b0:3181".to_string(), "b1:3181".to_string(), "b2:3181".to_string()];
    let result = p.replace_bookie(3, 2, 2, &current, "b1:3181", &empty_exclude());
    assert!(result.is_err(), "should error when all bookies are in ensemble");
}

#[test]
fn default_policy_on_cluster_changed_replaces_internal_state() {
    let mut p = DefaultPlacementPolicy::new();
    p.on_cluster_changed(&bookies("a", 5), &[]);
    p.on_cluster_changed(&bookies("b", 3), &[]);

    let r = p.new_ensemble(3, 2, 2, &empty_exclude()).unwrap();
    for bookie in &r.bookies {
        assert!(
            bookie.starts_with("b"),
            "should only see bookies from the latest cluster, got {bookie}"
        );
    }

    // The old "a" cluster has 5 bookies but we should fail to make a 5-bookie
    // ensemble because we're now bound to the 3-bookie "b" cluster.
    let err = p.new_ensemble(5, 2, 2, &empty_exclude());
    assert!(err.is_err());
}

// ---------------------------------------------------------------------------
// StaticRackResolver
// ---------------------------------------------------------------------------

#[test]
fn static_rack_resolver_returns_mapped_rack_for_known_bookies() {
    let resolver = rack_resolver(&[
        ("b0:3181", "/region-a/rack-1"),
        ("b1:3181", "/region-a/rack-2"),
    ]);
    assert_eq!(resolver.resolve("b0:3181"), "/region-a/rack-1");
    assert_eq!(resolver.resolve("b1:3181"), "/region-a/rack-2");
}

#[test]
fn static_rack_resolver_returns_default_rack_for_unknown_bookies() {
    let resolver = rack_resolver(&[("b0:3181", "/r1")]);
    let result = resolver.resolve("unknown:3181");
    assert!(
        result.contains("default"),
        "unknown bookies should fall back to default rack, got {result}"
    );
}

// ---------------------------------------------------------------------------
// RackAwarePlacementPolicy — new_ensemble
// ---------------------------------------------------------------------------

/// Build a 6-bookie / 3-rack scenario: b0,b1 in r1; b2,b3 in r2; b4,b5 in r3.
fn three_rack_resolver() -> StaticRackResolver {
    rack_resolver(&[
        ("b0:3181", "/r1"),
        ("b1:3181", "/r1"),
        ("b2:3181", "/r2"),
        ("b3:3181", "/r2"),
        ("b4:3181", "/r3"),
        ("b5:3181", "/r3"),
    ])
}

#[test]
fn rack_aware_new_ensemble_three_racks_strict() {
    let mut p = RackAwarePlacementPolicy::new(
        Box::new(three_rack_resolver()),
        2, // min racks per write quorum
        true,
    );
    p.on_cluster_changed(&bookies("b", 6), &[]);

    let r = p.new_ensemble(3, 3, 3, &empty_exclude()).unwrap();
    assert_eq!(r.bookies.len(), 3);

    // No duplicates
    let unique: HashSet<_> = r.bookies.iter().collect();
    assert_eq!(unique.len(), 3, "no duplicates allowed in ensemble");

    // Should span 3 racks (one per rack — round-robin)
    let resolver = three_rack_resolver();
    assert_eq!(
        racks_in(&r.bookies, &resolver).len(),
        3,
        "ensemble should span all 3 racks; got {:?}",
        r.bookies
    );

    assert_eq!(r.adherence, PolicyAdherence::MeetsStrict);
}

#[test]
fn rack_aware_new_ensemble_six_bookies_three_racks_uses_all_racks() {
    let mut p = RackAwarePlacementPolicy::new(
        Box::new(three_rack_resolver()),
        2,
        true,
    );
    p.on_cluster_changed(&bookies("b", 6), &[]);

    // Run multiple times to be more confident the round-robin actually
    // distributes — we can't depend on a specific permutation due to rng.
    for _ in 0..20 {
        let r = p.new_ensemble(6, 3, 3, &empty_exclude()).unwrap();
        let resolver = three_rack_resolver();
        assert_eq!(
            racks_in(&r.bookies, &resolver).len(),
            3,
            "6-bookie ensemble must span all 3 racks; got {:?}",
            r.bookies
        );
        // Each rack should contribute exactly 2 bookies
        let mut per_rack: HashMap<String, usize> = HashMap::new();
        for b in &r.bookies {
            *per_rack.entry(resolver.resolve(b)).or_default() += 1;
        }
        for (rack, count) in &per_rack {
            assert_eq!(*count, 2, "rack {rack} should contribute 2 bookies");
        }
        assert_eq!(r.adherence, PolicyAdherence::MeetsStrict);
    }
}

#[test]
fn rack_aware_new_ensemble_single_rack_enforce_rejects() {
    // All bookies in one rack, min_racks=2, enforce=true → must reject.
    let resolver = rack_resolver(&[
        ("b0:3181", "/r1"),
        ("b1:3181", "/r1"),
        ("b2:3181", "/r1"),
    ]);
    let mut p = RackAwarePlacementPolicy::new(Box::new(resolver), 2, true);
    p.on_cluster_changed(&bookies("b", 3), &[]);

    let err = p.new_ensemble(3, 2, 2, &empty_exclude());
    assert!(
        err.is_err(),
        "with enforce_min_racks=true and only 1 rack, ensemble creation must fail"
    );
}

#[test]
fn rack_aware_new_ensemble_single_rack_soft_succeeds_meets_soft() {
    // All bookies in one rack, min_racks=2, enforce=false → soft success.
    let resolver = rack_resolver(&[
        ("b0:3181", "/r1"),
        ("b1:3181", "/r1"),
        ("b2:3181", "/r1"),
    ]);
    let mut p = RackAwarePlacementPolicy::new(Box::new(resolver), 2, false);
    p.on_cluster_changed(&bookies("b", 3), &[]);

    let r = p.new_ensemble(3, 2, 2, &empty_exclude()).unwrap();
    assert_eq!(r.bookies.len(), 3);
    assert_eq!(
        r.adherence,
        PolicyAdherence::MeetsSoft,
        "single-rack non-enforced result should be MeetsSoft, got {:?}",
        r.adherence
    );
}

#[test]
fn rack_aware_new_ensemble_insufficient_bookies_errors() {
    let mut p = RackAwarePlacementPolicy::new(
        Box::new(three_rack_resolver()),
        2,
        true,
    );
    p.on_cluster_changed(&bookies("b", 6), &[]);

    let result = p.new_ensemble(10, 3, 3, &empty_exclude());
    match result {
        Err(PlacementError::NotEnoughBookies { needed, available }) => {
            assert_eq!(needed, 10);
            assert_eq!(available, 6);
        }
        Ok(_) => panic!("expected NotEnoughBookies error"),
    }
}

#[test]
fn rack_aware_new_ensemble_excludes_specified_bookies() {
    let mut p = RackAwarePlacementPolicy::new(
        Box::new(three_rack_resolver()),
        2,
        true,
    );
    p.on_cluster_changed(&bookies("b", 6), &[]);

    let mut exclude = HashSet::new();
    exclude.insert("b0:3181".to_string());
    exclude.insert("b1:3181".to_string()); // exclude all of r1

    // Now only r2 and r3 are available. min_racks=2, enforce=true; this
    // should still succeed because we have 2 racks.
    let r = p.new_ensemble(3, 2, 2, &exclude).unwrap();
    let resolver = three_rack_resolver();
    let racks = racks_in(&r.bookies, &resolver);
    assert!(!racks.contains("/r1"), "r1 should be excluded");
    for excluded in &exclude {
        assert!(
            !r.bookies.contains(excluded),
            "result should not contain excluded bookie {excluded}"
        );
    }
}

#[test]
fn rack_aware_new_ensemble_excluding_to_one_rack_with_enforce_rejects() {
    let mut p = RackAwarePlacementPolicy::new(
        Box::new(three_rack_resolver()),
        2,
        true,
    );
    p.on_cluster_changed(&bookies("b", 6), &[]);

    // Exclude all of r1 and r2 — only r3 (b4, b5) remains. Single rack +
    // enforce=true must reject.
    let mut exclude = HashSet::new();
    exclude.insert("b0:3181".to_string());
    exclude.insert("b1:3181".to_string());
    exclude.insert("b2:3181".to_string());
    exclude.insert("b3:3181".to_string());

    let err = p.new_ensemble(2, 2, 2, &exclude);
    assert!(
        err.is_err(),
        "with only one rack remaining after exclusion, must fail under enforce"
    );
}

#[test]
fn rack_aware_new_ensemble_two_racks_strict_window_coverage() {
    // 4 bookies, 2 racks of 2 each. E=4 W=2 min_racks=2. Every WQ window of
    // size 2 must span 2 racks. Round-robin r0/r1/r0/r1 satisfies this.
    let resolver = rack_resolver(&[
        ("b0:3181", "/r0"),
        ("b1:3181", "/r0"),
        ("b2:3181", "/r1"),
        ("b3:3181", "/r1"),
    ]);
    let mut p = RackAwarePlacementPolicy::new(Box::new(resolver.clone()), 2, true);
    p.on_cluster_changed(&bookies("b", 4), &[]);

    for _ in 0..20 {
        let r = p.new_ensemble(4, 2, 2, &empty_exclude()).unwrap();
        assert_eq!(r.adherence, PolicyAdherence::MeetsStrict);

        // Verify the WQ-window-coverage invariant directly.
        for start in 0..r.bookies.len() {
            let mut window_racks = HashSet::new();
            for i in 0..2 {
                let idx = (start + i) % r.bookies.len();
                window_racks.insert(resolver.resolve(&r.bookies[idx]));
            }
            assert_eq!(
                window_racks.len(),
                2,
                "window starting at {start} should span 2 racks; ensemble={:?}",
                r.bookies
            );
        }
    }
}

// ---------------------------------------------------------------------------
// RackAwarePlacementPolicy — replace_bookie
// ---------------------------------------------------------------------------

#[test]
fn rack_aware_replace_bookie_prefers_different_rack_than_failed() {
    // 4 racks of 2 bookies each (8 total). Ensemble uses one from each of
    // r0, r1, r2. Replace the r0 bookie — replacement should land in r3 to
    // avoid both the failed rack AND the neighbor racks (r1, r2 within W=3).
    let resolver = rack_resolver(&[
        ("b0:3181", "/r0"),
        ("b1:3181", "/r0"),
        ("b2:3181", "/r1"),
        ("b3:3181", "/r1"),
        ("b4:3181", "/r2"),
        ("b5:3181", "/r2"),
        ("b6:3181", "/r3"),
        ("b7:3181", "/r3"),
    ]);
    let mut p = RackAwarePlacementPolicy::new(Box::new(resolver.clone()), 2, true);
    p.on_cluster_changed(&bookies("b", 8), &[]);

    let current = vec!["b0:3181".to_string(), "b2:3181".to_string(), "b4:3181".to_string()];
    let replacement = p
        .replace_bookie(3, 3, 3, &current, "b0:3181", &empty_exclude())
        .unwrap();

    // Must not be in current ensemble
    assert!(!current.contains(&replacement));
    // Must not be in /r0 (the failed rack)
    assert_ne!(resolver.resolve(&replacement), "/r0");
    // Strictest: should be /r3 because /r1 and /r2 are neighbor racks
    assert_eq!(
        resolver.resolve(&replacement),
        "/r3",
        "preferred path: replacement should be in r3 (only non-neighbor non-failed rack); got {replacement}"
    );
}

#[test]
fn rack_aware_replace_bookie_falls_back_when_only_failed_rack_alternative_available() {
    // 2 racks total: r0 (b0, b1, b2) and r1 (b3, b4). Ensemble = [b0, b3].
    // Replace b0 — neighbor racks for W=2 are {r1}. Strict candidate filter:
    // not r0, not r1 → empty. Fallback: not r0 → b4 (r1).
    let resolver = rack_resolver(&[
        ("b0:3181", "/r0"),
        ("b1:3181", "/r0"),
        ("b2:3181", "/r0"),
        ("b3:3181", "/r1"),
        ("b4:3181", "/r1"),
    ]);
    let mut p = RackAwarePlacementPolicy::new(Box::new(resolver.clone()), 2, true);
    p.on_cluster_changed(&bookies("b", 5), &[]);

    let current = vec!["b0:3181".to_string(), "b3:3181".to_string()];
    let replacement = p
        .replace_bookie(2, 2, 2, &current, "b0:3181", &empty_exclude())
        .unwrap();

    assert!(!current.contains(&replacement));
    assert_ne!(
        resolver.resolve(&replacement),
        "/r0",
        "fallback should still avoid the failed rack; got {replacement}"
    );
    assert_eq!(replacement, "b4:3181");
}

#[test]
fn rack_aware_replace_bookie_falls_back_to_any_when_no_diverse_option() {
    // Single rack, W=2. Ensemble [b0, b1]. Replace b0. Strict and
    // different-rack fallback both fail (only one rack). Final fallback:
    // any non-current bookie → b2.
    let resolver = rack_resolver(&[
        ("b0:3181", "/r0"),
        ("b1:3181", "/r0"),
        ("b2:3181", "/r0"),
    ]);
    let mut p = RackAwarePlacementPolicy::new(Box::new(resolver), 2, false);
    p.on_cluster_changed(&bookies("b", 3), &[]);

    let current = vec!["b0:3181".to_string(), "b1:3181".to_string()];
    let replacement = p
        .replace_bookie(2, 2, 2, &current, "b0:3181", &empty_exclude())
        .unwrap();
    assert_eq!(replacement, "b2:3181");
}

#[test]
fn rack_aware_replace_bookie_excludes_current_ensemble_members() {
    // 4 racks of 1 bookie each. Ensemble uses 3 of them. The replacement
    // must be the 4th bookie, never one of the current 3, even though some
    // are in different racks than the failed bookie.
    let resolver = rack_resolver(&[
        ("b0:3181", "/r0"),
        ("b1:3181", "/r1"),
        ("b2:3181", "/r2"),
        ("b3:3181", "/r3"),
    ]);
    let mut p = RackAwarePlacementPolicy::new(Box::new(resolver), 2, true);
    p.on_cluster_changed(&bookies("b", 4), &[]);

    let current = vec!["b0:3181".to_string(), "b1:3181".to_string(), "b2:3181".to_string()];
    let replacement = p
        .replace_bookie(3, 2, 2, &current, "b0:3181", &empty_exclude())
        .unwrap();
    assert_eq!(replacement, "b3:3181");
}

#[test]
fn rack_aware_replace_bookie_respects_explicit_exclude_set() {
    // 4 racks. Ensemble uses 3. Without exclude, replacement = b3 (only one
    // left). With b3 in exclude, no replacement is available → error.
    let resolver = rack_resolver(&[
        ("b0:3181", "/r0"),
        ("b1:3181", "/r1"),
        ("b2:3181", "/r2"),
        ("b3:3181", "/r3"),
    ]);
    let mut p = RackAwarePlacementPolicy::new(Box::new(resolver), 2, true);
    p.on_cluster_changed(&bookies("b", 4), &[]);

    let current = vec!["b0:3181".to_string(), "b1:3181".to_string(), "b2:3181".to_string()];
    let mut exclude = HashSet::new();
    exclude.insert("b3:3181".to_string());

    let result = p.replace_bookie(3, 2, 2, &current, "b0:3181", &exclude);
    assert!(
        result.is_err(),
        "no replacement should be possible when the only candidate is excluded"
    );
}

#[test]
fn rack_aware_replace_bookie_errors_when_no_candidates_at_all() {
    // 3 bookies, all in the ensemble — nothing left to choose from.
    let resolver = rack_resolver(&[
        ("b0:3181", "/r0"),
        ("b1:3181", "/r1"),
        ("b2:3181", "/r2"),
    ]);
    let mut p = RackAwarePlacementPolicy::new(Box::new(resolver), 2, true);
    p.on_cluster_changed(&bookies("b", 3), &[]);

    let current = vec!["b0:3181".to_string(), "b1:3181".to_string(), "b2:3181".to_string()];
    let result = p.replace_bookie(3, 2, 2, &current, "b0:3181", &empty_exclude());
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// on_cluster_changed semantics
// ---------------------------------------------------------------------------

#[test]
fn rack_aware_on_cluster_changed_replaces_internal_state() {
    let mut p = RackAwarePlacementPolicy::new(
        Box::new(three_rack_resolver()),
        2,
        true,
    );

    // Initially empty — should fail to make any ensemble
    let err = p.new_ensemble(3, 2, 2, &empty_exclude());
    assert!(err.is_err());

    // After cluster update, should succeed
    p.on_cluster_changed(&bookies("b", 6), &[]);
    let r = p.new_ensemble(3, 2, 2, &empty_exclude()).unwrap();
    assert_eq!(r.bookies.len(), 3);

    // Replacing the cluster with a smaller list should reduce capacity
    p.on_cluster_changed(&["b0:3181".to_string(), "b1:3181".to_string()], &[]);
    let err = p.new_ensemble(3, 2, 2, &empty_exclude());
    assert!(
        err.is_err(),
        "should fail when cluster shrinks below ensemble size"
    );
}
