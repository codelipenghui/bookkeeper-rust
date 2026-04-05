use std::sync::Arc;
use tokio::sync::watch;
use zookeeper_client as zk;
use bookkeeper_metadata::zk_path::ZkPaths;
use bookkeeper_metadata::traits::LedgerManager;
use bookkeeper_proto::AuditorVoteFormat;
use crate::auditor::Auditor;
use crate::underreplication::ZkUnderreplicationManager;
use crate::ReplicationError;

const ELECTION_ZNODE: &str = "auditorelection";
const VOTE_PREFIX: &str = "V_";

/// ZK-based leader election for the Auditor.
///
/// Matches Java's AuditorElector + ZkLedgerAuditorManager:
/// - Each bookie creates an ephemeral sequential znode under `{underreplication}/auditorelection/V_`
/// - The bookie with the smallest sequence number becomes the auditor
/// - Others watch their predecessor and take over when it disappears
pub struct AuditorElector {
    client: zk::Client,
    election_path: String,
    bookie_id: String,
    my_vote: Option<String>,
}

impl AuditorElector {
    pub fn new(client: zk::Client, paths: &ZkPaths, bookie_id: String) -> Self {
        let election_path = format!("{}/{}", paths.underreplication(), ELECTION_ZNODE);
        Self {
            client,
            election_path,
            bookie_id,
            my_vote: None,
        }
    }

    /// Run the election loop. When elected, starts the Auditor. When the auditor
    /// loses leadership (e.g., ZK session expires), re-enters election.
    /// This method blocks until shutdown is signaled.
    pub async fn run(
        &mut self,
        ur_manager: Arc<ZkUnderreplicationManager>,
        ledger_manager: Arc<dyn LedgerManager>,
        zk_for_auditor: zk::Client,
        paths: ZkPaths,
        lost_bookie_recovery_delay_secs: u64,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> Result<(), ReplicationError> {
        // Ensure election path exists
        self.ensure_election_path().await?;

        loop {
            if *shutdown_rx.borrow() {
                break;
            }

            // Create our vote if needed
            self.create_vote().await?;

            // Try to become auditor
            match self.try_become_auditor().await? {
                ElectionResult::Elected => {
                    tracing::info!("Auditor elected: {}", self.bookie_id);

                    // Update election znode data with our bookie ID (for debugging)
                    let vote_data = crate::text_format::serialize_auditor_vote(&AuditorVoteFormat {
                        bookie_id: Some(self.bookie_id.clone()),
                    });
                    let _ = self
                        .client
                        .set_data(&self.election_path, &vote_data, None)
                        .await;

                    // Run auditor until shutdown or leadership lost
                    let auditor_shutdown = shutdown_rx.clone();
                    let mut auditor = Auditor::new(
                        zk_for_auditor.clone(),
                        paths.clone(),
                        ur_manager.clone(),
                        ledger_manager.clone(),
                        lost_bookie_recovery_delay_secs,
                        auditor_shutdown,
                    );
                    if let Err(e) = auditor.run().await {
                        tracing::warn!("Auditor exited: {e}");
                    }
                    // If auditor exits (shutdown or error), break
                    break;
                }
                ElectionResult::Waiting(predecessor) => {
                    // Watch predecessor — wait for deletion or shutdown
                    tokio::select! {
                        _ = self.wait_for_deletion(&predecessor) => {
                            // Predecessor gone — re-enter election
                            continue;
                        }
                        _ = shutdown_rx.changed() => {
                            if *shutdown_rx.borrow() {
                                break;
                            }
                        }
                    }
                }
            }
        }

        // Cleanup: delete our vote
        self.delete_vote().await;
        Ok(())
    }

    async fn ensure_election_path(&self) -> Result<(), ReplicationError> {
        let options = zk::CreateMode::Persistent.with_acls(zk::Acls::anyone_all());
        match self.client.create(&self.election_path, &[], &options).await {
            Ok(_) | Err(zk::Error::NodeExists) => Ok(()),
            Err(e) => Err(ReplicationError::Zk(format!(
                "create election path: {e}"
            ))),
        }
    }

    async fn create_vote(&mut self) -> Result<(), ReplicationError> {
        if let Some(ref vote) = self.my_vote {
            // Check if still exists
            if self.client.check_stat(vote).await.unwrap_or(None).is_some() {
                return Ok(());
            }
        }

        let prefix = format!("{}/{}", self.election_path, VOTE_PREFIX);
        let vote_data = crate::text_format::serialize_auditor_vote(&AuditorVoteFormat {
            bookie_id: Some(self.bookie_id.clone()),
        });
        let options = zk::CreateMode::EphemeralSequential.with_acls(zk::Acls::anyone_all());
        let (_stat, seq) = self
            .client
            .create(&prefix, &vote_data, &options)
            .await
            .map_err(|e| ReplicationError::Zk(format!("create vote: {e}")))?;

        self.my_vote = Some(format!("{}{}", prefix, seq));
        Ok(())
    }

    async fn try_become_auditor(&self) -> Result<ElectionResult, ReplicationError> {
        let (children, _) = self
            .client
            .get_children(&self.election_path)
            .await
            .map_err(|e| ReplicationError::Zk(format!("list votes: {e}")))?;

        if children.is_empty() {
            return Err(ReplicationError::Zk(
                "no votes found in election".into(),
            ));
        }

        // Sort by sequential suffix (V_0000000001, V_0000000002, ...)
        let mut sorted: Vec<String> = children
            .into_iter()
            .filter(|c| c.starts_with(VOTE_PREFIX))
            .collect();
        sorted.sort();

        // Find our vote's name
        let my_vote_path = self
            .my_vote
            .as_ref()
            .ok_or_else(|| ReplicationError::Zk("no vote created".into()))?;
        let my_name = my_vote_path
            .rsplit('/')
            .next()
            .unwrap_or("");

        if sorted.is_empty() {
            return Err(ReplicationError::Zk("no votes after filtering".into()));
        }

        if sorted[0] == my_name {
            // We're the smallest — elected!
            Ok(ElectionResult::Elected)
        } else {
            // Watch our predecessor
            let my_idx = sorted.iter().position(|s| s == my_name).unwrap_or(0);
            let predecessor_idx = if my_idx > 0 { my_idx - 1 } else { 0 };
            let predecessor_path = format!(
                "{}/{}",
                self.election_path, sorted[predecessor_idx]
            );
            Ok(ElectionResult::Waiting(predecessor_path))
        }
    }

    async fn wait_for_deletion(&self, path: &str) {
        // Poll for deletion (zookeeper-client doesn't expose a simple exists-watch API directly).
        // We check every second until the znode disappears.
        loop {
            match self.client.check_stat(path).await {
                Ok(Some(_)) => {
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }
                _ => return, // gone or error — re-enter election
            }
        }
    }

    async fn delete_vote(&self) {
        if let Some(ref vote) = self.my_vote {
            let _ = self.client.delete(vote, None).await;
        }
    }
}

enum ElectionResult {
    Elected,
    Waiting(String), // predecessor path to watch
}
