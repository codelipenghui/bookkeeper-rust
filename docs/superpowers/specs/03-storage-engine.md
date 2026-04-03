# Sub-Spec 03: Storage Engine

**Target**: DbLedgerStorage (RocksDB-based), Journal (WAL), entry logs, caches, compaction, and checkpoint.

## 1. Overview

The bookie storage engine has two write paths:

1. **Journal**: Write-ahead log for durability. Entries are written here first and acknowledged to the client.
2. **Ledger Storage (DbLedgerStorage)**: Indexed storage for reads. Entries are flushed here asynchronously from an in-memory cache.

```
Write:  Client → Journal (fsync) → Ack → WriteCache → (async flush) → EntryLog + RocksDB index
Read:   Client → ReadCache → WriteCache → RocksDB index → EntryLog file
```

## 2. Journal (Write-Ahead Log)

### 2.1 File Naming

Journal files are named `{journalId}.txn` where `journalId` is formatted as a hex string.

Example: `0.txn`, `1.txn`, `a.txn`

Stored in the configured journal directory.

### 2.2 Journal File Format

**Header (V5+)**: 512 bytes (`SECTOR_SIZE`)

```
Offset  Size    Field
0       4       Magic: "BKLG" (ASCII, 4 bytes)
4       4       Format version (int32, big-endian)
8       504     Zero padding (to fill 512-byte sector)
```

**Header (V1-V4)**: 8 bytes

```
Offset  Size    Field
0       4       Magic: "BKLG"
4       4       Format version (int32)
```

**V1**: No header at all (entries start at offset 0).

### 2.3 Format Versions

| Version | Changes                                    |
| ------- | ------------------------------------------ |
| V1      | No header, raw entries                     |
| V2      | 8-byte header (magic + version)            |
| V3      | Added ledger key support                   |
| V4      | Added fencing key support                  |
| V5      | 512-byte header, padding alignment         |
| V6      | Added explicit LAC entry support (current) |

Current: `CURRENT_JOURNAL_FORMAT_VERSION = 6`
Minimum compatible: `MIN_COMPAT_JOURNAL_FORMAT_VERSION = 1`

### 2.4 Entry Layout in Journal

Each entry in the journal is preceded by its size:

```
┌──────────┬────────────────────────┐
│ Size     │ Entry payload          │
│ (4 bytes)│ (Size bytes)           │
└──────────┴────────────────────────┘
```

The entry payload is the raw entry bytes (same as wire format entry body from ADD\_ENTRY).

### 2.5 Padding (V5+)

After each flush, padding is added to align to `journalAlignmentSize` (default 4096 bytes):

```
┌──────────────────┬──────────────────┬──────────────┐
│ PADDING_MASK     │ Padding Length   │ Zero bytes   │
│ 0xFFFFFF00 (4B)  │ (4 bytes, int32) │ (N bytes)    │
└──────────────────┴──────────────────┴──────────────┘
```

* `PADDING_MASK = 0xFFFFFF00` — sentinel value distinguishing padding from entry data

* Padding length includes the 8-byte header

* Minimum padding: 8 bytes (just the mask + length)

* Target alignment: `(position & ~(journalAlignmentSize - 1)) + journalAlignmentSize`

On read: if the size field equals `PADDING_MASK`, skip `paddingLength` bytes.

### 2.6 Journal Position

A journal position is encoded as a 64-bit value:

```
position = (journalFileId << 32) | byteOffset
```

* Upper 32 bits: journal file ID

* Lower 32 bits: byte offset within the file

### 2.7 Last Mark (Checkpoint)

The last mark records the journal position up to which all entries have been safely flushed to ledger storage.

**File**: `.lastMark` in each ledger directory

**Format** (16 bytes):

```
Offset  Size    Field
0       8       Journal file ID (int64, big-endian)
8       8       Byte offset within journal file (int64, big-endian)
```

On startup: replay all journal entries after the last mark position.

### 2.8 Journal Rotation

* New journal file created when current file exceeds `journalMaxSizeMB` (default 2048 MB)

* Old journal files deleted after all their entries are checkpointed (i.e., flushed to ledger storage and the last mark has advanced past them)

### 2.9 Group Commit

For throughput, the journal batches multiple writes before calling fsync:

* Entries accumulate in a write buffer

* Flush triggered by:

  * Buffer exceeds `journalBufferedWritesThreshold`

  * Entry count exceeds `journalBufferedEntriesThreshold`

  * Wait time exceeds `journalMaxGroupWaitMSec` (default 1ms)

  * Adaptive group writes: adjusts dynamically based on queue depth

* Single fsync covers all buffered entries

* All waiting writers are unblocked after fsync completes

## 3. DbLedgerStorage

### 3.1 Architecture

```
DbLedgerStorage
├── SingleDirectoryDbLedgerStorage[0] (ledger dir 0)
│   ├── WriteCache (double-buffered)
│   ├── ReadCache (ring-buffer)
│   ├── EntryLocationIndex (RocksDB: "locations/")
│   ├── LedgerMetadataIndex (RocksDB: "ledgers/")
│   └── EntryLogger (entry log files)
├── SingleDirectoryDbLedgerStorage[1] (ledger dir 1)
│   └── ... (same structure)
└── ...
```

**Sharding**: One `SingleDirectoryDbLedgerStorage` per configured ledger directory. Ledgers are assigned to directories by hash: `ledgerId % numDirectories`.

### 3.2 Write Path

1. Entry arrives from journal callback (after journal fsync)
2. Route to correct `SingleDirectoryDbLedgerStorage` via `ledgerId % numDirs`
3. Add to active `WriteCache`:

   * Store entry bytes in direct memory segment

   * Index entry in `ConcurrentLongLongPairHashMap`: key=(ledgerId, entryId), value=(offset, size)

   * Update `lastEntryMap`: ledgerId → max(entryId)
4. If WriteCache full:

   * Swap active/flush caches (double-buffer rotation)

   * Trigger async flush of the now-inactive cache

### 3.3 Read Path

1. Check `ReadCache`: hash lookup by (ledgerId, entryId) → direct memory offset
2. Check `WriteCache` (both active and flush buffers)
3. Lookup in RocksDB `EntryLocationIndex`: key=(ledgerId, entryId) → location
4. Decode location: `entryLogId = location >>> 32`, `offset = location & 0xFFFFFFFF`
5. Read entry from entry log file at the decoded offset
6. Add to `ReadCache` for future reads

### 3.4 Flush (WriteCache → Persistent Storage)

1. Sort all entries in WriteCache by (ledgerId, entryId)
2. For each entry in sorted order:
   a. Write entry to `EntryLogger` → returns location (logId, offset)
   b. Batch-put to RocksDB `EntryLocationIndex`: (ledgerId, entryId) → location
3. Flush EntryLogger
4. Flush RocksDB

## 4. WriteCache

### 4.1 Data Structure

```
WriteCache:
  cacheSegments: ByteBuf[]          // Direct memory segments (default 1GB each)
  index: ConcurrentLongLongPairHashMap  // (ledgerId, entryId) → (offset, size)
  lastEntryMap: ConcurrentLongLongHashMap  // ledgerId → lastEntryId
  cacheOffset: AtomicLong           // Current write position
  cacheSize: AtomicLong             // Total bytes stored
  cacheCount: LongAdder             // Number of entries
```

### 4.2 Memory Layout

* Entries stored in pre-allocated direct memory segments

* Each entry aligned to 64 bytes: `alignedSize = (size + 63) & ~63`

* Entries do not cross segment boundaries — if an entry doesn't fit in the current segment, try the next

### 4.3 Double Buffering

Two WriteCache instances exist per directory:

* **Active cache**: receiving new writes

* **Flush cache**: being flushed to disk

* On swap: active becomes flush, flush becomes active (after clearing)

* If flush is still in progress when active fills: writers block until flush completes

### 4.4 Default Sizing

* Max cache size per directory: 25% of estimated max direct memory / numDirs

* Each cache divided into segments (default max 1 GB per segment)

## 5. ReadCache

### 5.1 Data Structure

```
ReadCache:
  cacheSegments: List<ByteBuf>      // Ring-buffer of direct memory segments
  cacheIndexes: List<ConcurrentLongLongPairHashMap>  // One index per segment
  currentSegmentIdx: int            // Active segment
  currentSegmentOffset: AtomicInteger
```

### 5.2 Ring-Buffer Eviction

* Minimum 2 segments (enforced)

* When current segment is full: rotate to next segment, clear the old one

* On read: search from current segment backward to oldest

* Segment size: `maxCacheSize / numSegments`

### 5.3 Default Sizing

* Max cache size per directory: 25% of estimated max direct memory / numDirs

## 6. RocksDB Storage

### 6.1 Entry Location Index

**Path**: `{ledgerDir}/locations/`

**Key format** (16 bytes, big-endian):

```
Offset  Size    Field
0       8       ledgerId (int64, big-endian)
8       8       entryId (int64, big-endian)
```

**Value format** (8 bytes):

```
Offset  Size    Field
0       8       location (int64, big-endian)
                  Upper 32 bits: entry log file ID
                  Lower 32 bits: byte offset in entry log
```

**RocksDB configuration**:

* Compression: LZ4 (configurable)

* Bloom filter: 10 bits per key

* Block size: 64 KB

* Write buffer: 64 MB

* Format version: 5

* Checksum: xxHash

* Level compaction with dynamic level bytes

### 6.2 Ledger Metadata Index

**Path**: `{ledgerDir}/ledgers/`

**Key format** (8 bytes):

```
Offset  Size    Field
0       8       ledgerId (int64, big-endian)
```

**Value format** (protobuf):

```protobuf
message DbLedgerStorageDataFormats.LedgerData {
    required bool exists = 1;
    required bool fenced = 2;
    optional bytes masterKey = 3;
    optional int64 lac = 4;         // Last Add Confirmed
    optional int64 ctime = 5;       // Creation time (reserved)
}
```

## 7. Entry Log Files

### 7.1 File Naming

Entry log files: `{entryLogId}.log` where entryLogId is formatted as hex.

### 7.2 File Format

**Header** (1024 bytes = `LOGFILE_HEADER_SIZE`):

```
Offset  Size    Field
0       4       Magic: "BKLO" (ASCII)
4       4       Header version (int32): HEADER_V0=0 or HEADER_V1=1
8       8       Ledgers map offset (int64): byte position of ledger map in file
16      4       Ledgers count (int32): number of distinct ledgers in this file
20      1004    Zero padding
```

**Entry layout** (repeated, after header):

```
┌──────────┬────────────────────────┐
│ Size     │ Entry bytes            │
│ (4 bytes)│ (Size bytes)           │
└──────────┴────────────────────────┘
```

The `Size` field stores `entry.readableBytes()` — the number of entry bytes that follow (NOT including the 4-byte size field itself).

### 7.3 Entry Location

An entry's location in the log is the byte offset of the **entry bytes** (after the size field):

```
position = offset_of_size_field + 4
```

To read an entry at a given position:

1. Seek to `position - 4` to read the 4-byte size
2. Read `size` bytes starting at `position`

### 7.4 Ledger Map (end of file)

When an entry log file is closed, a ledger map is appended. It consists of one or more batches:

**Batch format**:

```
Offset  Size    Field
0       4       Batch content size (int32) — bytes following, excluding this field
4       8       INVALID_LID = -1 (int64, marker)
12      8       LEDGERS_MAP_ENTRY_ID = -2 (int64, marker)
20      4       Number of entries in batch (int32)
24      16*N    Ledger entries (N entries):
                  Per entry:
                    8 bytes: ledgerId (int64)
                    8 bytes: totalSize (int64) — bytes written for this ledger
```

The ledger map offset is recorded in the file header at offset 8.

### 7.5 Entry Log Per Ledger (Optional)

When `entryLogPerLedgerEnabled=true`:

* Each ledger gets its own entry log file

* Simplifies GC (delete entire file when ledger is deleted)

* Same file format, just one ledger per file

## 8. Compaction & Garbage Collection

### 8.1 Garbage Collection Thread

Runs periodically (default `gcWaitTime=900000ms` = 15 minutes):

1. **Scan** all entry log files
2. For each file, compute **usage ratio**: (live bytes) / (total bytes)

   * Live bytes = bytes belonging to ledgers that still exist

   * Dead bytes = bytes belonging to deleted ledgers
3. **Minor compaction**: files with usage < `minorCompactionThreshold` (default 0.1 = 10%)

   * Runs every `minorCompactionInterval` (default 3600s = 1 hour)
4. **Major compaction**: files with usage < `majorCompactionThreshold` (default 0.5 = 50%)

   * Runs every `majorCompactionInterval` (default 604800s = 7 days)

### 8.2 Compaction Process (Transactional)

1. Create a new entry log file for compacted entries
2. Scan source entry log:

   * For each entry: check if its ledger still exists

   * If yes: write entry to new log file, record new location
3. Flush new log file
4. Batch-update RocksDB entry location index with new locations
5. Flush RocksDB
6. Delete old entry log file

**Throttling**: Configurable via `compactionRateByBytes` (default 1MB/s) to limit I/O impact.

### 8.3 Deleted Ledger Cleanup

When a ledger is deleted:

1. Remove from RocksDB ledger metadata index
2. Entry log entries become "dead" — reclaimed during next compaction cycle
3. If `entryLogPerLedgerEnabled`: can delete the entire entry log file immediately

## 9. Checkpoint Flow

Checkpointing coordinates the journal and ledger storage to determine which journal entries can be safely discarded.

```
1. SyncThread timer fires (or journal triggers checkpoint)
2. Get current journal position as checkpoint mark
3. Call ledgerStorage.flush():
   a. Rotate WriteCache (swap active/flush)
   b. Flush old cache to entry logs + RocksDB
   c. Flush RocksDB
4. Call journal.checkpointComplete(checkpoint):
   a. Update lastMark to checkpoint position
   b. Persist lastMark to disk (.lastMark file)
   c. Delete journal files older than lastMark
```

### 9.1 Crash Recovery

On startup:

1. Read `.lastMark` from all ledger directories, take the latest
2. Open journal files at or after the lastMark position
3. Replay all journal entries from lastMark forward:

   * For each entry: write to ledger storage (same as normal write path)
4. Flush ledger storage
5. Update lastMark
6. Resume normal operation

## 10. Disk Management

### 10.1 Multiple Directories

BookKeeper supports multiple ledger and journal directories for I/O parallelism:

* Journal directories: entries written round-robin or to least-loaded

* Ledger directories: sharded by ledgerId hash

### 10.2 Disk Usage Monitoring

* `diskUsageThreshold` (default 0.95 = 95%): trigger read-only mode

* `diskUsageWarnThreshold` (default 0.90 = 90%): log warnings

* `diskUsageLwmThreshold` (default 0.90 = 90%): recover from read-only when below this

* `diskCheckInterval` (default 10000ms = 10s): monitoring interval

### 10.3 Read-Only Mode Transition

When any ledger directory exceeds `diskUsageThreshold`:

1. Bookie transitions to read-only mode
2. Deregisters from writable bookies in ZK
3. Registers under `{root}/available/readonly/{bookieId}`
4. Rejects all write operations with `EREADONLY`
5. Continues serving reads
6. Can recover to writable when disk usage drops below `diskUsageLwmThreshold`

