# Sub-Spec 04: Bookie Server

**Target**: Request processing pipeline, write/read paths, fencing, state machine, and disk management.

## 1. Server Lifecycle

### 1.1 Startup Sequence

1. Load and validate configuration
2. Initialize disk directories (journal, ledger, index)
3. Validate cookie against ZK and local disk (see 02-metadata.md §4)
4. Initialize Journal
5. Initialize LedgerStorage (DbLedgerStorage)
6. Replay journal from lastMark (crash recovery)
7. Start TCP server (Netty)
8. Register with metadata service (ZK)
9. Start auto-recovery daemon (if enabled)
10. Start GC/compaction thread
11. Start disk monitoring thread

### 1.2 Shutdown Sequence

1. Deregister from metadata service
2. Stop accepting new TCP connections
3. Reject new requests
4. Drain in-flight requests (wait for completion)
5. Stop auto-recovery daemon
6. Stop GC/compaction
7. Flush ledger storage
8. Checkpoint (flush journal → ledger storage)
9. Close journal
10. Close TCP server

## 2. Request Processing Pipeline

### 2.1 Netty Pipeline

```
TCP Connection
  → LengthFieldBasedFrameDecoder (4-byte length prefix, max frame size)
  → BookieProtoEncoding.RequestDecoder (decode v2 binary or v3 protobuf)
  → BookieRequestHandler (dispatch to processor)
```

### 2.2 Thread Pools

| Pool | Config Parameter | Default | Purpose |
|------|-----------------|---------|---------|
| I/O threads | `serverNumIOThreads` | 2 × CPU cores | Netty I/O event loops |
| Add workers | `numAddWorkerThreads` | 1 | Process ADD_ENTRY requests |
| Read workers | `numReadWorkerThreads` | 8 | Process READ_ENTRY requests |
| Long-poll workers | `numLongPollWorkerThreads` | 0 (uses read pool) | Process long-poll READ_LAC |
| High-priority workers | `numHighPriorityWorkerThreads` | 8 | Process FLAG_HIGH_PRIORITY requests |

### 2.3 Request Dispatch

The `BookieRequestProcessor` dispatches each request to the appropriate handler:

| Operation | Handler | Thread Pool |
|-----------|---------|-------------|
| ADD_ENTRY | WriteEntryProcessor | Add workers |
| READ_ENTRY | ReadEntryProcessor | Read workers (or high-priority) |
| BATCH_READ_ENTRY | BatchReadEntryProcessor | Read workers |
| WRITE_LAC | WriteLacProcessor | Add workers |
| READ_LAC | ReadLacProcessor | Read workers / Long-poll workers |
| FORCE_LEDGER | ForceLedgerProcessor | Add workers |
| GET_BOOKIE_INFO | GetBookieInfoProcessor | Read workers |
| GET_LIST_OF_ENTRIES | GetListOfEntriesProcessor | Read workers |
| AUTH | AuthHandler | I/O thread |
| START_TLS | StartTLSHandler | I/O thread |

Requests with `FLAG_HIGH_PRIORITY` are routed to the high-priority thread pool.

## 3. Write Path (ADD_ENTRY)

### 3.1 Normal Write

```
1. Decode request → extract ledgerId, entryId, masterKey, entryBody, flags
2. Validate request:
   a. Check bookie is writable (not read-only, not shutting down)
   b. Check rate limits (ETOOMANYREQUESTS if exceeded)
3. Check if ledger is fenced:
   a. If fenced AND not FLAG_RECOVERY_ADD → reject with EFENCED
4. Validate master key:
   a. First write to this ledger: store masterKey in LedgerMetadataIndex
   b. Subsequent writes: compare with stored masterKey → reject with EUA on mismatch
5. Write to Journal:
   a. Add entry to journal write queue
   b. Journal batches and fsyncs (group commit)
   c. On fsync complete: invoke callback
6. Journal callback:
   a. Add entry to LedgerStorage (WriteCache)
   b. Send success response to client (EOK)
```

### 3.2 Recovery Write (FLAG_RECOVERY_ADD)

Same as normal write, except:
- Step 3: fencing check is skipped — recovery writes are always accepted
- The bookie also sets the ledger as fenced (if not already)

### 3.3 Deferred Sync (DEFERRED_SYNC write flag)

When the client sets `DEFERRED_SYNC` in the write flags:
- Step 5: entry is written to journal buffer but NOT fsynced immediately
- Ack is sent before fsync
- Client must later send `FORCE_LEDGER` to ensure durability
- Used for higher throughput at the cost of explicit flush management

## 4. Read Path (READ_ENTRY)

### 4.1 Normal Read

```
1. Decode request → extract ledgerId, entryId, flags
2. If FLAG_DO_FENCING:
   a. Validate masterKey against stored key
   b. Set ledger as fenced in LedgerStorage
   c. If fencing fails (wrong key): reject with EUA
3. If entryId == LAST_ADD_CONFIRMED (-1):
   a. Return the current LAC for this ledger (no entry data)
4. Read entry from LedgerStorage:
   a. ReadCache → WriteCache → RocksDB index → EntryLog file
5. Piggyback current LAC in the response
6. Return entry data (or ENOENTRY if not found)
```

### 4.2 Long-Poll Read

When `previousLAC` and `timeOut` are provided in a read request:
- If the requested entry is available: return immediately
- If not: register a watcher on the ledger's LAC
- When LAC advances past the requested entry: trigger the read and respond
- If timeout expires: return ENOENTRY

This is used for tailing reads where the reader follows the writer in near-real-time.

### 4.3 Batch Read

```
1. Decode request → extract ledgerId, startEntryId, maxCount, maxSize
2. If FLAG_DO_FENCING: fence as in single read
3. Read entries sequentially starting from startEntryId:
   a. Stop when: count reaches maxCount OR total bytes reaches maxSize OR ENOENTRY
4. Include current LAC in response
5. Return list of entries
```

## 5. Fencing

### 5.1 Fencing a Ledger

```
fenceLedger(ledgerId, masterKey):
  1. Read stored masterKey for this ledger
  2. Compare with provided masterKey:
     - Mismatch → return error (EUA)
  3. Set fenced=true in LedgerMetadataIndex (RocksDB)
  4. Return success with current LAC
```

### 5.2 Fencing Persistence

- The `fenced` flag is stored in the RocksDB ledger metadata index
- It survives bookie restarts
- Once fenced, a ledger stays fenced until deleted

### 5.3 Fencing Effects

After a ledger is fenced on a bookie:
- Normal `ADD_ENTRY` (without `FLAG_RECOVERY_ADD`) → `EFENCED`
- `ADD_ENTRY` with `FLAG_RECOVERY_ADD` → accepted (recovery in progress)
- `READ_ENTRY` → unaffected (reads always work)

## 6. Explicit LAC

### 6.1 WRITE_LAC

```
1. Validate masterKey
2. Verify LAC digest (same digest algorithm as entries, over ledgerId + lac)
3. Store explicit LAC value in LedgerStorage for this ledger
4. Notify any long-poll waiters that LAC has advanced
5. Return success
```

### 6.2 READ_LAC

```
1. Read explicit LAC from LedgerStorage
2. Read LAC from last entry (piggyback LAC)
3. Return both:
   - lacBody: the explicit LAC entry (if set)
   - lastEntryBody: the last entry bytes (contains piggyback LAC)
```

Readers take the maximum of explicit LAC and piggyback LAC.

## 7. FORCE_LEDGER

```
1. Force flush all journal entries for this ledger to durable storage
2. Force flush ledger storage
3. Return success
```

Used after DEFERRED_SYNC writes to ensure all entries are persistent.

## 8. GET_BOOKIE_INFO

```
1. Read requested info flags from request
2. If FREE_DISK_SPACE requested: compute free space across all ledger directories
3. If TOTAL_DISK_SPACE requested: compute total space across all ledger directories
4. Return info
```

## 9. GET_LIST_OF_ENTRIES_OF_LEDGER

```
1. Query LedgerStorage for all entry IDs belonging to this ledger
2. Encode as a roaring bitmap (or equivalent compact representation)
3. Return the encoded availability map
```

## 10. Bookie State Machine

```
         ┌──────────┐
         │ STARTING │
         └────┬─────┘
              │ initialization complete
              ▼
         ┌──────────┐
    ┌───▶│ RUNNING  │◄───┐
    │    └────┬─────┘    │
    │         │          │ disk space recovered
    │         │ disk full│
    │         ▼          │
    │    ┌──────────┐    │
    │    │READ_ONLY │────┘
    │    └────┬─────┘
    │         │
    │         │ (cannot recover from shutdown)
    │         ▼
    │    ┌──────────────┐
    └────│SHUTTING_DOWN │
         └──────────────┘
```

### 10.1 State Transitions

- **STARTING → RUNNING**: All initialization complete, registered with ZK
- **RUNNING → READ_ONLY**: Any ledger directory exceeds `diskUsageThreshold`
  - Deregisters from writable bookies
  - Registers as read-only bookie
  - Rejects writes with `EREADONLY`
- **READ_ONLY → RUNNING**: All directories below `diskUsageLwmThreshold`
  - Only if `readOnlyModeEnabled=true`
  - Re-registers as writable
- **Any → SHUTTING_DOWN**: Shutdown requested
  - Deregisters from ZK
  - Drains in-flight requests
  - Flushes and closes storage

### 10.2 Disk Monitoring

- Runs every `diskCheckInterval` ms (default 10000)
- Checks all configured directories (journal, ledger, index)
- Tracks per-directory usage percentages
- `LedgerDirsManager` maintains lists of writable vs full directories

## 11. Rate Limiting

- Configurable per-bookie rate limits for read and write operations
- When exceeded: reject with `ETOOMANYREQUESTS`
- Prevents a single client from overwhelming the bookie

## 12. Bookie Identity

The bookie's identity (`bookieId`) is determined by:
1. `advertisedAddress` if configured (highest priority)
2. Hostname of `listeningInterface` if configured
3. System hostname (or short hostname if `useShortHostName=true`)
4. Combined with port: `{hostname}:{bookiePort}`

If `useHostNameAsBookieID=true`, hostname is used; otherwise IP address.
