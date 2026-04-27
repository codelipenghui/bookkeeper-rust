//! Standalone auto-recovery entry point.
//!
//! Mirrors Java's `AutoRecoveryMain` — a daemon that bundles the
//! `AuditorElector` and `ReplicationWorker` together and monitors both so
//! that if either task exits, the whole daemon shuts down. Java uses a
//! separate `AutoRecoveryDeathWatcher` thread; here we rely on
//! `JoinSet`/`select!` for the same semantics in async form.
//!
//! Typical deployment: run this inside a dedicated "autorecovery" pod
//! alongside a ZK cluster, separate from bookies. Callers that already
//! host the auditor+worker inside their bookie process (the default
//! `autoRecoveryDaemonEnabled = true` path) can continue to use
//! `server::start_auto_recovery()` instead.

use std::sync::Arc;
use tokio::sync::watch;
use zookeeper_client as zk;

use bookkeeper_client::placement::EnsemblePlacementPolicy;
use bookkeeper_metadata::traits::LedgerManager;
use bookkeeper_metadata::zk_path::ZkPaths;

use crate::auditor_election::AuditorElector;
use crate::underreplication::ZkUnderreplicationManager;
use crate::worker::ReplicationWorker;
use crate::ReplicationError;

/// Configuration bundle for [`AutoRecoveryMain::run`]. Field names track
/// Java's `ServerConfiguration` keys used by `AutoRecoveryMain`.
#[derive(Clone)]
pub struct AutoRecoveryConfig {
    pub bookie_id: String,
    pub lost_bookie_recovery_delay_secs: u64,
    pub rw_rereplicate_backoff_ms: u64,
    pub auditor_periodic_check_interval_secs: u64,
    pub auditor_periodic_placement_check_interval_secs: u64,
    pub auditor_periodic_replicas_check_interval_secs: u64,
    pub open_ledger_grace_period_ms: u64,
    /// Max replication-worker claim attempts per second (0 = unbounded).
    pub worker_max_claims_per_second: u32,
}

/// Runs the auditor election and replication worker until `shutdown_rx`
/// flips to `true` or either sub-task exits with an unrecoverable error.
///
/// The "death watcher" semantics come from `tokio::select!` — if the
/// auditor task fails, we cancel the worker and return; and vice versa.
/// That matches Java's `AutoRecoveryDeathWatcher` which watches both and
/// shuts down the peer when one exits.
pub async fn run(
    zk_client: zk::Client,
    paths: ZkPaths,
    ledger_manager: Arc<dyn LedgerManager>,
    placement_policy: Option<Arc<dyn EnsemblePlacementPolicy>>,
    cfg: AutoRecoveryConfig,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<(), ReplicationError> {
    let ur_manager = Arc::new(
        ZkUnderreplicationManager::new(zk_client.clone(), paths.clone(), cfg.bookie_id.clone()).await?,
    );

    // Build independent shutdown channels so we can cancel one task
    // without racing on the shared parent channel.
    let (auditor_shutdown_tx, auditor_shutdown_rx) = watch::channel(false);
    let (worker_shutdown_tx, worker_shutdown_rx) = watch::channel(false);

    // Auditor
    let a_zk = zk_client.clone();
    let a_paths = paths.clone();
    let a_ur = ur_manager.clone();
    let a_lm = ledger_manager.clone();
    let a_bookie_id = cfg.bookie_id.clone();
    let a_cfg = cfg.clone();
    let a_policy = placement_policy.clone();
    let auditor_task = tokio::spawn(async move {
        let mut elector = AuditorElector::new(a_zk.clone(), &a_paths, a_bookie_id);
        elector.run(
            a_ur,
            a_lm,
            a_zk,
            a_paths,
            a_cfg.lost_bookie_recovery_delay_secs,
            a_cfg.auditor_periodic_check_interval_secs,
            a_cfg.auditor_periodic_placement_check_interval_secs,
            a_cfg.auditor_periodic_replicas_check_interval_secs,
            a_policy,
            auditor_shutdown_rx,
        ).await
    });

    // Worker
    let w_ur = ur_manager.clone();
    let w_lm = ledger_manager.clone();
    let w_cfg = cfg.clone();
    let w_policy = placement_policy.clone();
    let worker_task = tokio::spawn(async move {
        let mut worker = ReplicationWorker::new(
            w_ur,
            w_lm,
            w_cfg.bookie_id,
            w_cfg.rw_rereplicate_backoff_ms,
            worker_shutdown_rx,
        );
        worker.set_open_ledger_grace_period_ms(w_cfg.open_ledger_grace_period_ms);
        if w_cfg.worker_max_claims_per_second > 0 {
            worker.set_max_claims_per_second(w_cfg.worker_max_claims_per_second);
        }
        if let Some(policy) = w_policy {
            worker.set_placement_policy(policy);
        }
        worker.run().await
    });

    // Death watcher: whichever task exits first, cancel the other and
    // propagate the outcome. A parent shutdown_rx flip drains both.
    tokio::pin!(auditor_task);
    tokio::pin!(worker_task);
    let outcome = tokio::select! {
        res = &mut auditor_task => {
            tracing::warn!("AutoRecoveryMain: auditor exited first, stopping worker");
            let _ = worker_shutdown_tx.send(true);
            let _ = (&mut worker_task).await;
            match res {
                Ok(r) => r,
                Err(e) => Err(ReplicationError::Failed(format!("auditor task panic: {e}"))),
            }
        }
        res = &mut worker_task => {
            tracing::warn!("AutoRecoveryMain: worker exited first, stopping auditor");
            let _ = auditor_shutdown_tx.send(true);
            let _ = (&mut auditor_task).await;
            match res {
                Ok(r) => r,
                Err(e) => Err(ReplicationError::Failed(format!("worker task panic: {e}"))),
            }
        }
        _ = async {
            // Wait for shutdown_rx to actually go *true*. A bare
            // `shutdown_rx.changed()` arm exits the supervisor on ANY
            // change (e.g. a watcher init that flips false→false→true)
            // and the auditor + worker subtasks would then run unsupervised.
            loop {
                if *shutdown_rx.borrow() { break; }
                if shutdown_rx.changed().await.is_err() { break; }
            }
        } => {
            tracing::info!("AutoRecoveryMain: shutdown requested");
            let _ = auditor_shutdown_tx.send(true);
            let _ = worker_shutdown_tx.send(true);
            let _ = auditor_task.await;
            let _ = worker_task.await;
            Ok(())
        }
    };

    outcome
}
