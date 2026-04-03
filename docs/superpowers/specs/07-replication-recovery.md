# Sub-Spec 07: Replication & Recovery

**Target**: Auditor, replication worker, underreplication manager, fencing/LAC protocols in detail.

## 1. Overview

BookKeeper auto-recovery is a distributed system that detects and repairs underreplicated ledgers. It consists of three components:

| Component               | Role                           | Instances                                   |
| ----------------------- | ------------------------------ | ------------------------------------------- |
| Auditor                 | Detect underreplicated ledgers | Exactly 1 (leader-elected)                  |
| ReplicationWorker       | Repair underreplicated ledgers | One per bookie (with auto-recovery enabled) |
| UnderreplicationManager | Track underreplicated ledgers  | Shared state in ZK                          |

Auto-recovery runs as a daemon alongside each bookie (or standalone). Only one auditor is active at a time (ZK leader election).

## 2. Auditor

### 2.1 Leader Election

* All bookies with auto-recovery enabled participate in auditor election

* ZK-based leader election: only one auditor active cluster-wide

* On leader failure: another bookie takes over

### 2.2 Audit Tasks

The auditor runs four periodic check tasks:

#### 2.2.1 Bookie Check (AuditorBookieCheckTask)

**Trigger**: Bookie failure detected (ephemeral znode disappears from ZK)

```
onBookieFailure(failedBookies):
  if lostBookieRecoveryDelay == 0:
    // Immediate audit
    startBookieCheck()
    return

  if |failedBookies| > 1:
    // Multiple simultaneous failures → immediate audit
    startBookieCheck()
    return

  // Single failure → delayed audit (transient failure handling)
  schedule(startBookieCheck, lostBookieRecoveryDelay seconds)
```

**Bookie check algorithm**:

```
startBookieCheck():
  1. Get current available bookies from ZK
  2. Compute newly failed bookies = known - available
  3. For each failed bookie:
     a. Enumerate all ledgers (via metadata scan)
     b. For each ledger:
        - Read metadata
        - Check if any ensemble segment contains the failed bookie
        - If yes: mark ledger as underreplicated with failed bookie address
```

#### 2.2.2 Ledger Check (AuditorCheckAllLedgersTask)

**Trigger**: Periodic, every `auditorPeriodicCheckInterval` seconds (default 604800 = 7 days)

```
checkAllLedgers():
  1. Iterate all ledgers via ledgerManager.asyncProcessLedgers()
  2. For each ledger:
     a. Read metadata
     b. If state == CLOSED or state == IN_RECOVERY:
        - For each ensemble segment:
          - Check that all bookies in the ensemble are available
          - Optionally: read entries and verify quorum (if auditorLedgerVerificationPercentage > 0)
        - If any bookie unavailable: mark underreplicated
```

#### 2.2.3 Placement Policy Check (AuditorPlacementPolicyCheckTask)

**Trigger**: Periodic

```
checkPlacementPolicy():
  1. For each ledger:
     a. For each ensemble segment:
        - Verify ensemble satisfies placement policy constraints
        - (e.g., bookies across minimum number of racks)
     b. If violated: mark underreplicated
```

#### 2.2.4 Replicas Check (AuditorReplicasCheckTask)

**Trigger**: Periodic

```
checkReplicas():
  1. For each ledger:
     a. For each ensemble segment:
        - Query each bookie for entries it holds (GET_LIST_OF_ENTRIES)
        - Cross-validate: each entry should exist on writeQuorumSize bookies
     b. If any entry has fewer replicas than expected: mark underreplicated
```

### 2.3 Lost Bookie Recovery Delay

* Default: 1800 seconds (30 minutes) — configurable via `lostBookieRecoveryDelay`

* Purpose: avoid unnecessary replication for transient failures (restarts, network blips)

* Single bookie failure: wait the delay before triggering audit

* Multiple simultaneous failures: trigger immediately (more likely to be a real problem)

* Delay can be dynamically adjusted via admin HTTP API

### 2.4 Bookie Availability Monitoring

```
The auditor watches ZK path: {root}/available/

On change notification:
  1. List current children → available bookies
  2. Compare with known bookies:
     - New bookies: add to known set
     - Missing bookies: add to failed set
  3. Submit audit task for failed bookies
  4. Re-set ZK watcher
```

## 3. Underreplication Manager

### 3.1 Marking Underreplicated

```
markLedgerUnderreplicated(ledgerId, missingReplicas: List<string>):
  1. Construct ZK path: {root}/underreplication/ledgers/{hex4}/{hex4}/{hex4}/{hex4}/urL{ledgerId}
  2. Serialize UnderreplicatedLedgerFormat:
     - replica: [list of missing bookie addresses]
     - ctime: current timestamp
  3. Create or update znode (PERSISTENT)
  4. If znode already exists: merge replica lists (union)
```

### 3.2 Claiming a Ledger for Replication

```
getLedgerToRereplicate() → int64:
  1. Scan underreplication ledger znodes
  2. For each underreplicated ledger:
     a. Try to acquire lock: create {root}/underreplication/locks/urL{ledgerId}
        - Content: LockDataFormat with bookie ID
        - If lock already exists: skip (another worker has it)
     b. If lock acquired: return ledgerId
  3. If no ledgers available: block until notified
```

### 3.3 Completing Replication

```
markLedgerReplicated(ledgerId):
  1. Delete marker: {root}/underreplication/ledgers/.../urL{ledgerId}
  2. Release lock: delete {root}/underreplication/locks/urL{ledgerId}
```

### 3.4 Global Enable/Disable

A special znode controls whether replication is enabled cluster-wide:

```
isLedgerReplicationEnabled() → bool:
  Check existence of {root}/underreplication/disable znode
  If exists: replication disabled
  If not exists: replication enabled
```

## 4. Replication Worker

### 4.1 Main Loop

```
while running:
  try:
    ledgerId = underreplicationManager.getLedgerToRereplicate()  // blocks
    success = rereplicate(ledgerId)
    if success:
      underreplicationManager.markLedgerReplicated(ledgerId)
    else:
      // Release lock, will be retried later
      releaseLock(ledgerId)
      sleep(backoffTime)
  catch InterruptedException:
    shutdown()
  catch BKException:
    sleep(backoffTime)
```

### 4.2 Per-Ledger Replication

```
rereplicate(ledgerId):
  1. Read ledger metadata
  2. If ledger is OPEN or IN_RECOVERY:
     a. Open for recovery (fences the ledger)
     b. This also closes the ledger, setting lastEntryId
  3. If ledger is CLOSED:
     a. Open read-only

  4. For each ensemble segment:
     a. Identify which bookies are missing/unavailable
     b. For each missing bookie:
        - Select replacement via placement policy
     c. For each entry in this segment:
        - Determine write set
        - If write set contains missing bookie:
          - Read entry from available bookie
          - Write entry to replacement bookie (FLAG_RECOVERY_ADD)

  5. Update ledger metadata with new ensemble
     (CAS update — retry on conflict)

  6. Return success
```

### 4.3 Throttling

* Replication rate can be limited to avoid overwhelming the cluster

* Configurable backoff on failures

* Exponential backoff with max retries (`NUM_OF_EXPONENTIAL_BACKOFF_RETRIALS`)

### 4.4 Failure Handling

```
If read fails for an entry:
  1. Add to unableToReadEntries cache
  2. Continue with other entries
  3. After processing all entries: retry failed entries
  4. If still failing: release lock, ledger stays underreplicated for later retry
```

## 5. Fencing Protocol (Detailed)

### 5.1 Purpose

Prevent two writers from writing to the same ledger simultaneously ("split-brain" prevention).

### 5.2 Protocol Steps

**Initiator** (new client or recovery process):

```
1. Read ledger metadata from ZK → get ensemble, quorum sizes
2. For EACH bookie in the LAST ensemble segment:
   Send READ_ENTRY(
     ledgerId,
     entryId = LAST_ADD_CONFIRMED (-1),
     flags = FLAG_DO_FENCING,
     masterKey = ledger password derived key
   )
3. Wait for writeQuorumSize responses
```

**Bookie** (on receiving fencing read):

```
1. Extract masterKey from request
2. Validate masterKey against stored masterKey:
   - If mismatch: return EUA
   - If no stored key (new ledger on this bookie): store the key
3. Set fenced = true for this ledger (persistent, in RocksDB)
4. Return:
   - Last entry data (or ENOENTRY if no entries)
   - Current LAC for this ledger
```

**Initiator** (on receiving responses):

```
1. Collect responses from fencing quorum (writeQuorumSize bookies)
2. recoveryLAC = max(all returned LAC values, lastEnsembleKey - 1)
3. Proceed to forward read from recoveryLAC + 1
```

### 5.3 Effect on Old Writer

After fencing quorum is reached:

* The old writer's subsequent `ADD_ENTRY` requests (without `FLAG_RECOVERY_ADD`) receive `EFENCED`

* The old writer detects the fence and:

  * Stops writing

  * Fails pending writes to the application with `BKLedgerFencedException`

### 5.4 Recovery Add

Entries written during recovery use `FLAG_RECOVERY_ADD`:

* These are accepted even on fenced ledgers

* This allows the recovery process to re-replicate entries and close the ledger

## 6. LAC Protocol (Detailed)

### 6.1 Piggyback LAC

Every interaction carries LAC information:

**In ADD\_ENTRY**: The entry itself contains the writer's current LAC at offset 16:

```
Entry: [ledgerId(8) | entryId(8) | LAC(8) | length(8) | digest | payload]
```

**In READ\_ENTRY response**: The bookie includes its known LAC for the ledger:

* v2: LAC is embedded in the returned entry data (at offset 16)

* v3: `maxLAC` field in `ReadResponse`, plus LAC in entry body

### 6.2 Explicit LAC

For readers that need timely LAC updates without reading entries:

**Writer → Bookies** (WRITE\_LAC):

```
Periodically (every explicitLacIntervalMs):
  1. Construct LAC entry:
     [ledgerId(8) | lac(8) | digest]
  2. Send WRITE_LAC to all bookies in current ensemble
  3. Bookie stores explicit LAC value
```

**Reader → Bookies** (READ\_LAC):

```
1. Send READ_LAC to bookies in ensemble
2. Bookie returns:
   - Explicit LAC (from WRITE_LAC, if set)
   - LAC from last entry (piggyback)
3. Reader takes max of both values
```

### 6.3 Long-Poll LAC

For tailing reads, the reader can long-poll for LAC advancement:

```
Send READ_ENTRY or READ_LAC with:
  - previousLAC: the reader's current known LAC
  - timeOut: max wait time in milliseconds

Bookie behavior:
  - If current LAC > previousLAC: return immediately
  - If current LAC <= previousLAC: wait until LAC advances or timeout
  - On LAC advancement: return new LAC
  - On timeout: return current state
```

### 6.4 LAC in Recovery

During recovery, the LAC determines the boundary between committed and uncommitted entries:

```
recoveryLAC = max(
  LAC values from fencing reads across quorum,
  firstEntryId of last ensemble segment - 1
)
```

Entries above the recoveryLAC are found by forward reading — they exist but weren't confirmed. The recovery process re-replicates them to ensure quorum, then closes the ledger with the correct lastEntryId.

## 7. Decommissioning a Bookie

Safe removal of a bookie from the cluster:

```
decommissionBookie(bookieId):
  1. For each ledger on the bookie:
     a. Mark as underreplicated
  2. Wait for all ledgers to be replicated to other bookies
  3. Stop the bookie
  4. Remove cookie from ZK
  5. The bookie's ephemeral registration znode disappears automatically
```

This is an administrative operation triggered via CLI or HTTP API.
