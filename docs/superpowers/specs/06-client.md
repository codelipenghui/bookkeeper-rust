# Sub-Spec 06: Client Library

**Target**: Client write/read paths, ensemble placement, distribution scheduling, recovery, digest, and connection management.

## 1. Client Initialization

```
BookKeeper client initialization:
1. Parse metadata service URI (e.g., "zk+hierarchical://zk1:2181/ledgers")
2. Connect to metadata service (ZooKeeper)
3. Initialize MetadataClientDriver
4. Create LedgerManager (for metadata operations)
5. Create BookieClient (TCP connection pool to bookies)
6. Initialize EnsemblePlacementPolicy:
   a. Fetch current bookie list from ZK
   b. Build network topology (rack/region mapping)
   c. Set watchers for bookie changes
7. Start scheduler threads (ordered executor for callbacks)
```

## 2. Ledger Creation

```
1. Allocate ledger ID via LedgerIdGenerator (ZK sequential znode)
2. Select initial ensemble:
   a. Call placementPolicy.newEnsemble(ensembleSize, writeQuorum, ackQuorum, excludeBookies)
   b. Returns ordered list of ensembleSize bookies
3. Build LedgerMetadata:
   - state=OPEN
   - ensemble segment 0: (firstEntryId=0, selectedBookies)
   - digestType, password, quorum sizes
4. Store metadata in ZK via ledgerManager.createLedgerMetadata()
5. Return WriteHandle to caller
```

## 3. Write Path

### 3.1 Entry Append

```
append(payload):
  1. Assign entryId = lastAddPushed + 1 (monotonically increasing)
  2. Track cumulative length: currentLength += payload.size
  3. Compute entry bytes:
     a. Metadata: ledgerId(8) + entryId(8) + LAC(8) + currentLength(8) = 32 bytes
     b. Digest: computed over metadata + payload
     c. Full entry: [metadata | digest | payload]
  4. Determine write set from DistributionSchedule:
     writeSet = [(entryId + i) % ensembleSize for i in 0..writeQuorumSize]
  5. Create PendingAddOp with AckSet (bit set tracking acks)
  6. Add to pendingAddsQueue
  7. Send ADD_ENTRY to each bookie in writeSet (parallel)
  8. Return Future<entryId>
```

### 3.2 Ack Handling

```
onWriteComplete(bookieIndex, status):
  1. If status != EOK:
     a. Mark bookie as failed in AckSet
     b. If failures > (writeQuorumSize - ackQuorumSize):
        → write cannot succeed, trigger ensemble change
     c. Else: wait for more responses
  2. If status == EOK:
     a. Mark bookie as succeeded in AckSet
     b. If succeeded count >= ackQuorumSize:
        → write confirmed
        → advance LAC to this entryId
        → invoke success callback
```

### 3.3 LAC Advancement

LAC advances **in order** — `sendAddSuccessCallbacks()` processes the `pendingAddsQueue` sequentially:

```
while pendingAddsQueue is not empty:
  head = pendingAddsQueue.peek()
  if head is completed:
    pendingAddsQueue.remove()
    lastAddConfirmed = head.entryId
    head.callback.success()
  else:
    break  // cannot advance past incomplete entry
```

This ensures LAC is monotonically increasing and gaps are never exposed to readers.

### 3.4 Write Timeout

* Timer runs at fixed interval, checks each PendingAddOp

* If elapsed time > `addEntryQuorumTimeoutNanos`:

  * Check if ack quorum is still achievable

  * If not: fail the entry, trigger ensemble change or error

## 4. Ensemble Change

Triggered when a bookie fails during writes.

### 4.1 Algorithm

```
handleBookieFailure(failedBookies: Map<bookieIndex, bookieId>):
  synchronized(metadataLock):
    if changingEnsemble:
      // Queue for next iteration
      delayedWriteFailedBookies.putAll(failedBookies)
      return
    changingEnsemble = true

  // Metadata CAS loop
  loop:
    currentMetadata = readCurrentMetadata()
    if currentMetadata.state != OPEN:
      break  // ledger no longer writable

    currentEnsemble = currentMetadata.lastEnsemble
    newEnsemble = copy(currentEnsemble)

    for each (bookieIndex, failedBookie) in failedBookies:
      replacement = placementPolicy.replaceBookie(
        ensembleSize, writeQuorum, ackQuorum,
        newEnsemble, bookieIndex, excludeBookies
      )
      newEnsemble[bookieIndex] = replacement

    newSegmentStartEntry = lastAddConfirmed + 1
    if lastEnsembleKey == newSegmentStartEntry:
      // Replace the last segment (no entries written with old ensemble)
      updatedMetadata = metadata.replaceLastEnsemble(newEnsemble)
    else:
      // Add new segment
      updatedMetadata = metadata.addEnsembleSegment(newSegmentStartEntry, newEnsemble)

    try:
      writeLedgerMetadata(updatedMetadata, currentVersion)  // CAS
      break  // success
    catch MetadataVersionException:
      // Concurrent modification — re-read and retry
      continue
```

### 4.2 In-Flight Write Handling

After ensemble change succeeds:

```
for each pendingAdd in pendingAddsQueue:
  if pendingAdd.writeSet contains failedBookie:
    unset success for failed bookie
    resend to replacement bookie
```

### 4.3 Exclusive Lock

Only one ensemble change can be in progress at a time per ledger handle. If another failure occurs during an ensemble change, it's queued in `delayedWriteFailedBookies` and processed in the next iteration.

## 5. Read Path

### 5.1 Single Entry Read

```
read(firstEntry, lastEntry):
  for entryId in firstEntry..lastEntry:
    1. Determine ensemble for this entry:
       - Find the ensemble segment where firstEntryId <= entryId
    2. Determine read set from DistributionSchedule:
       readSet = [(entryId + i) % ensembleSize for i in 0..writeQuorumSize]
    3. Send READ_ENTRY to first bookie in read set
    4. On response:
       a. If EOK: verify digest, return entry
       b. If error: try next bookie in read set
    5. If all bookies exhausted: return error
```

### 5.2 Speculative Reads

```
1. Send read to primary bookie (first in read set)
2. Start timer: speculativeReadTimeoutMs
3. If timer fires before response:
   a. Send duplicate read to next bookie in read set
4. Use first successful response, cancel outstanding
```

Configurable via `speculativeReadTimeout` (default: disabled).

### 5.3 Digest Verification

On every read response:

```
verifyDigestAndReturnData(entryBody):
  1. Extract metadata: ledgerId(8) + entryId(8) + LAC(8) + length(8) from offset 0
  2. Extract digest from offset 32 (METADATA_LENGTH)
  3. Recompute digest over: metadata + payload
  4. Compare with received digest
  5. If mismatch: throw BKDigestMatchException
  6. Verify ledgerId and entryId match expected values
  7. Return payload (strip metadata + digest)
```

## 6. Distribution Schedule

### 6.1 Round-Robin Algorithm

```
class RoundRobinDistributionSchedule:
  ensembleSize: int
  writeQuorumSize: int
  ackQuorumSize: int

  getWriteSet(entryId: int64) → List<int>:
    return [(entryId + i) % ensembleSize for i in 0..writeQuorumSize]

  getAckSet() → AckSet:
    return new AckSet(writeQuorumSize, ackQuorumSize)
```

### 6.2 AckSet

Tracks which bookies have acknowledged a write:

```
class AckSet:
  ackBits: BitSet(writeQuorumSize)
  failBits: BitSet(writeQuorumSize)

  completeBookieAndCheck(bookieIndex: int) → bool:
    ackBits.set(bookieIndex)
    return ackBits.cardinality() >= ackQuorumSize

  failBookieAndCheck(bookieIndex: int) → bool:
    failBits.set(bookieIndex)
    failCount = failBits.cardinality()
    return failCount > (writeQuorumSize - ackQuorumSize)
    // True means ack quorum is now impossible
```

### 6.3 Segment Awareness

The distribution schedule operates on ensemble **indices** (0 to ensembleSize-1), not bookie addresses. When looking up which bookie to contact:

```
1. Find ensemble segment for entryId
2. Get ensemble list for that segment
3. Compute write/read set indices
4. Map indices to bookie addresses: ensemble[index]
```

## 7. Ensemble Placement Policies

### 7.1 Interface

```
interface EnsemblePlacementPolicy:
    newEnsemble(ensembleSize, writeQuorum, ackQuorum, excludeBookies)
        → List<BookieId>

    replaceBookie(ensembleSize, writeQuorum, ackQuorum, 
                  currentEnsemble, bookieIndex, excludeBookies)
        → BookieId

    onClusterChanged(writableBookies, readOnlyBookies)

    reorderReadSequence(ensemble, bookiesHealthInfo, writeSet)
        → List<int>  // reordered indices for read preference
```

### 7.2 RackAwareEnsemblePlacementPolicy (Default)

* Distributes bookies across network racks for fault tolerance

* Ensures minimum number of racks per write quorum: `minNumRacksPerWriteQuorum`

* Uses `DNSToSwitchMapping` to resolve bookie → rack mapping

* `newEnsemble()`: selects bookies from different racks, weighted by available capacity

* `replaceBookie()`: finds replacement in a different rack from remaining ensemble members

* `reorderReadSequence()`: prefers local rack, then bookies with lower latency

### 7.3 RegionAwareEnsemblePlacementPolicy

* Extends rack-aware with region awareness

* Distributes across regions first, then racks within regions

* Configurable: `minNumRacksPerWriteQuorum` at region level

### 7.4 ZoneAwareEnsemblePlacementPolicy

* Zone-based placement (e.g., availability zones)

* Similar to rack-aware but with zone topology

## 8. Ledger Recovery (Client-Initiated)

When a client opens a ledger for recovery (e.g., previous writer crashed):

### 8.1 Phase 1: Fencing

```
1. For each bookie in the current ensemble:
   Send READ_ENTRY(ledgerId, entryId=LAST_ADD_CONFIRMED, flags=FLAG_DO_FENCING, masterKey)
2. Wait for writeQuorumSize responses (fencing quorum)
3. Each bookie:
   a. Validates masterKey
   b. Sets fenced=true for this ledger
   c. Returns its LAC and last entry data
4. Recovery LAC = max(all returned LACs)
```

### 8.2 Phase 2: Forward Read

```
1. startEntry = recoveryLAC + 1
2. Batch read entries starting from startEntry:
   - Read from any available bookie in each entry's write set
   - Continue until ENOENTRY (no more entries)
3. For each entry found:
   a. Verify digest
   b. Track as "needs re-replication"
```

### 8.3 Phase 3: Re-replication

```
For each entry found in Phase 2:
  1. Send ADD_ENTRY with FLAG_RECOVERY_ADD to bookies that are missing the entry
  2. Wait for ackQuorumSize acks
  3. This ensures all entries up to the true last entry meet quorum
```

### 8.4 Phase 4: Close

```
1. Update metadata:
   - state = CLOSED
   - lastEntryId = highest entry found (or recoveryLAC if no new entries)
   - length = cumulative bytes
2. CAS update to metadata store
```

## 9. Ledger Close (Normal)

```
1. Stop accepting new appends
2. Wait for all PendingAddOps to complete (ack quorum reached)
3. Optionally send FORCE_LEDGER to all bookies in ensemble
4. Update metadata:
   - state = CLOSED
   - lastEntryId = lastAddConfirmed
   - length = cumulative bytes
5. CAS update to metadata store
```

## 10. Connection Management

### 10.1 PerChannelBookieClient

One instance per bookie, manages a single TCP connection:

```
States: DISCONNECTED → CONNECTING → CONNECTED → CLOSED
        (also START_TLS as transient state)

connect():
  1. If DISCONNECTED: transition to CONNECTING
  2. Create Netty channel to bookie address
  3. On connect success: transition to CONNECTED
  4. On connect failure: back to DISCONNECTED, schedule retry

disconnect():
  1. Close channel
  2. Fail all pending operations
  3. Transition to DISCONNECTED
```

### 10.2 Request/Response Correlation

* **v2 protocol**: Correlation key = `(ledgerId, entryId, opType)` tuple

* **v3 protocol**: Correlation key = `txnId` (monotonically increasing uint64, `AtomicLong`)

* Pending operations stored in `ConcurrentHashMap<CompletionKey, CompletionValue>`

* On response: lookup by key, invoke callback, remove from map

* On timeout: fail pending operation, mark bookie as potentially unavailable

### 10.3 Connection Pool

`BookieClient` maintains a pool of `PerChannelBookieClient` instances:

* Lazy creation: connections established on first request to a bookie

* Configurable number of connections per bookie (`numChannelsPerBookie`)

* Round-robin across connections for the same bookie

## 11. Explicit LAC (Client Side)

### 11.1 Writer

The writer periodically broadcasts its LAC to all bookies in the current ensemble:

```
Every explicitLacIntervalMs:
  1. Compute LAC entry: ledgerId(8) + lac(8) + digest
  2. Send WRITE_LAC to all bookies in current ensemble
```

### 11.2 Reader (Tailing)

For tailing reads (following a live writer):

```
1. Send READ_LAC to bookies
2. Take max of:
   a. Explicit LAC from WRITE_LAC
   b. Piggyback LAC from entry data
3. Read entries up to the known LAC
4. Optionally long-poll for LAC advancement
```

## 12. Ledger Deletion

```
1. Call ledgerManager.removeLedgerMetadata(ledgerId, version)
2. Metadata removed from ZK
3. Bookies are NOT notified directly
4. Bookie GC thread eventually:
   a. Discovers ledger metadata is gone
   b. Marks entries as dead
   c. Compaction reclaims space
```

