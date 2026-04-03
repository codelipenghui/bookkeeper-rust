# Sub-Spec 02: Metadata

**Target**: Ledger metadata schema, ZooKeeper paths, ID generation, cookies, and the pluggable MetadataDriver interface.

## 1. Ledger Metadata

### 1.1 Fields

| Field | Type | Required | Default | Constraints |
|-------|------|----------|---------|-------------|
| `ledgerId` | int64 | yes | — | >= 0 |
| `metadataFormatVersion` | int32 | no | 3 | 1, 2, or 3 |
| `ensembleSize` | int32 | no | 3 | >= writeQuorumSize |
| `writeQuorumSize` | int32 | no | 3 | >= ackQuorumSize |
| `ackQuorumSize` | int32 | no | 2 | >= 1 |
| `state` | enum | no | OPEN | OPEN, IN_RECOVERY, CLOSED |
| `lastEntryId` | int64 | conditional | -1 | Required if CLOSED |
| `length` | int64 | conditional | 0 | Required if CLOSED |
| `digestType` | enum | no | null | CRC32, CRC32C, HMAC, DUMMY |
| `password` | bytes | no | empty | Must pair with digestType |
| `ctime` | int64 | no | -1 | Creation timestamp ms |
| `storeCtime` | bool | no | false | Whether to persist ctime |
| `customMetadata` | map<string, bytes> | no | {} | Application metadata |
| `cToken` | int64 | no | 0 | Creator token (v3+) |
| `ensembles` | map<int64, list<BookieId>> | yes | — | >= 1 segment |

**State constraints**:
- OPEN: `lastEntryId` and `length` not set
- CLOSED: both `lastEntryId` and `length` must be set
- IN_RECOVERY: transitional state, can move to CLOSED

**Ensemble constraints**:
- At least one segment
- All segments have exactly `ensembleSize` members
- Segments ordered by `firstEntryId`

### 1.2 Protobuf Schema (DataFormats.proto)

```protobuf
message LedgerMetadataFormat {
    required int32 quorumSize = 1;       // writeQuorumSize
    required int32 ensembleSize = 2;
    required int64 length = 3;
    optional int64 lastEntryId = 4;

    enum State {
        OPEN = 1;
        IN_RECOVERY = 2;
        CLOSED = 3;
    }
    required State state = 5 [default = OPEN];

    message Segment {
        repeated string ensembleMember = 1;
        required int64 firstEntryId = 2;
    }
    repeated Segment segment = 6;

    enum DigestType {
        HMAC = 1;
        CRC32 = 2;
        CRC32C = 3;
        DUMMY = 4;
    }
    optional DigestType digestType = 7;
    optional bytes password = 8;
    optional int32 ackQuorumSize = 9;
    optional int64 ctime = 10;

    message cMetadataMapEntry {
        optional string key = 1;
        optional bytes value = 2;
    }
    repeated cMetadataMapEntry customMetadata = 11;
    optional int64 cToken = 12;
}
```

### 1.3 Serialization Formats

Three serialization versions exist. The format is determined by `metadataFormatVersion`.

**Common header** (all versions):
```
"BookieMetadataFormatVersion\t{version}\n"
```
The header is UTF-8 text, terminated by newline. The version is a decimal integer (up to 10 digits).

**Version 1** (text, legacy v4.0.x):
```
BookieMetadataFormatVersion\t1\n
{writeQuorumSize}\n
{ensembleSize}\n
{length}\n
{firstEntryId}\t{bookieAddr1}\t{bookieAddr2}\t...\n
...
{lastEntryId}\t{state}
```
State is `CLOSED` or `IN_RECOVERY`. If OPEN, no trailing line.

**Version 2** (protobuf text format, v4.2.x+):
```
BookieMetadataFormatVersion\t2\n
{protobuf TextFormat of LedgerMetadataFormat}
```

**Version 3** (protobuf binary, v4.9.x+, current default):
```
BookieMetadataFormatVersion\t3\n
{protobuf binary via writeDelimitedTo()}
```
Note: `writeDelimitedTo` writes a varint-encoded size prefix followed by the protobuf bytes.

### 1.4 Versioned Metadata

All metadata operations use optimistic concurrency control:
- `Versioned<LedgerMetadata>` wraps metadata with a version object
- The version is opaque — for ZK, it's the znode stat version (int32)
- Updates require passing the current version — if it doesn't match, the operation fails with `BKMetadataVersionException`
- This provides compare-and-swap semantics

## 2. ZooKeeper Path Layout

### 2.1 Base Paths

All paths are relative to the **ledger root** (default: `/ledgers`).

| Path | Znode Type | Purpose |
|------|-----------|---------|
| `{root}/` | PERSISTENT | Ledger root |
| `{root}/available/` | PERSISTENT | Parent for active bookies |
| `{root}/available/{bookieId}` | EPHEMERAL | Writable bookie registration |
| `{root}/available/readonly/` | PERSISTENT | Parent for read-only bookies |
| `{root}/available/readonly/{bookieId}` | EPHEMERAL | Read-only bookie registration |
| `{root}/cookies/` | PERSISTENT | Parent for bookie cookies |
| `{root}/cookies/{bookieId}` | PERSISTENT | Bookie identity cookie |
| `{root}/LAYOUT` | PERSISTENT | Metadata layout version |
| `{root}/instanceid` | PERSISTENT | Cluster UUID |
| `{root}/idgen/` | PERSISTENT | Legacy ledger ID generation |
| `{root}/idgen-long/` | PERSISTENT | Long ledger ID generation |
| `{root}/underreplication/` | PERSISTENT | Underreplication tracking root |

### 2.2 Ledger Metadata Paths

BookKeeper uses a hierarchical path structure to avoid having too many children in a single znode.

**HierarchicalLedgerManager** uses a hybrid strategy:
- For `ledgerId < Integer.MAX_VALUE` (2^31 - 1): 3-level (legacy) hierarchy
- For `ledgerId >= Integer.MAX_VALUE`: 5-level (long) hierarchy

**3-Level (Legacy) Hierarchy**:
```
Format ledger ID as 10-digit decimal: %010d
Split into 3 parts: [0:2] / [2:6] / [6:10]
Path: {root}/{part1}/{part2}/L{part3}

Example: ledgerId=1 → "0000000001" → {root}/00/0000/L0001
Example: ledgerId=12345678 → "0012345678" → {root}/00/1234/L5678
```

**5-Level (Long) Hierarchy**:
```
Format ledger ID as 19-digit decimal: %019d
Split into 5 parts: [0:3] / [3:7] / [7:11] / [11:15] / [15:19]
Path: {root}/{part1}/{part2}/{part3}/{part4}/L{part5}

Example: ledgerId=1 → "0000000000000000001" → {root}/000/0000/0000/0000/L0001
```

Each ledger znode is PERSISTENT and contains serialized `LedgerMetadata` (see Section 1.3).

## 3. Ledger ID Generation

**Mechanism**: ZK sequential ephemeral znodes.

**Algorithm**:
1. Create EPHEMERAL_SEQUENTIAL znode at `{root}/{idGenZnode}/ID-`
2. ZK appends a 10-digit zero-padded sequential number (e.g., `ID-0000000042`)
3. Extract the number from the returned path
4. Delete the ephemeral znode immediately
5. Return the extracted number as the new ledger ID

**Path**: `{root}/idgen-long/ID-` for long IDs, `{root}/idgen/ID-` for legacy

**Limit**: ~9 billion IDs (ZK sequential counter is a signed 32-bit integer internally, but the path allows up to 10 digits)

## 4. Cookie Mechanism

### 4.1 Purpose

Cookies prevent a bookie from accidentally starting with mismatched data directories. They are stored in both ZK and on local disk, and validated on every startup.

### 4.2 Cookie Format

**Current layout version**: 5

**Fields**:
| Field | Type | Description |
|-------|------|-------------|
| `layoutVersion` | int | Always 5 (current) |
| `bookieId` | string | `{hostname}:{port}` or custom |
| `journalDirs` | string | Comma-separated journal directories |
| `ledgerDirs` | string | Tab-encoded: `{count}\t{dir1}\t{dir2}\t...` |
| `instanceId` | string | Cluster UUID (optional) |
| `indexDirs` | string | Tab-encoded like ledgerDirs (optional, defaults to ledgerDirs) |

**Serialization** (v5): Protobuf text format
```
5
bookie_host: "hostname:3181"
journal_dir: "/data/journal"
ledger_dirs: "2\t/data/ledgers1\t/data/ledgers2"
instanceId: "550e8400-e29b-41d4-a716-446655440000"
indexDirs: "1\t/data/index"
```

First line is the version number, followed by a protobuf text-format `CookieFormat` message.

### 4.3 Storage

- **ZK**: `{root}/cookies/{bookieId}` — PERSISTENT znode, content is serialized cookie
- **Local disk**: `{journalDir}/COOKIE` — file containing serialized cookie

### 4.4 Validation on Startup

1. Read cookie from ZK
2. Read cookie from local disk
3. Compare:
   - `layoutVersion` must match
   - `bookieId` must match
   - `journalDirs` must match
   - `ledgerDirs` must match (or be a superset, for directory expansion)
   - `indexDirs` must match (or be a superset)
   - `instanceId` must match if both are non-null
4. On mismatch: refuse to start (prevents data corruption)

## 5. LedgerManager Interface

The LedgerManager is the primary interface for metadata operations:

```
interface LedgerManager:
    createLedgerMetadata(ledgerId: int64, metadata: LedgerMetadata)
        → Future<Versioned<LedgerMetadata>>

    readLedgerMetadata(ledgerId: int64)
        → Future<Versioned<LedgerMetadata>>

    writeLedgerMetadata(ledgerId: int64, metadata: LedgerMetadata, currentVersion: Version)
        → Future<Versioned<LedgerMetadata>>

    removeLedgerMetadata(ledgerId: int64, version: Version)
        → Future<Void>

    registerLedgerMetadataListener(ledgerId: int64, listener: LedgerMetadataListener)
        → void

    asyncProcessLedgers(processor: Processor<int64>, finalCb, ctx, successCode, failureCode)
        → void

    getLedgerRanges(zkOpTimeOutMs: int64)
        → LedgerRangeIterator
```

All write operations use compare-and-swap via the `Version` parameter.

## 6. MetadataDriver Interface

The pluggable driver interface supports alternative metadata backends.

```
interface MetadataBookieDriver:
    // Server-side driver
    initialize(conf: ServerConfiguration, supplier: Supplier<BookieId>)
    getRegistrationManager() → RegistrationManager
    getLayoutManager() → LayoutManager
    close()

interface MetadataClientDriver:
    // Client-side driver
    initialize(conf: ClientConfiguration, scheduler, statsLogger, uri)
    getSessionStateListener() → Optional<MetadataEventListener>
    getLedgerManagerFactory() → LedgerManagerFactory
    getLayoutManager() → LayoutManager
    close()
```

The metadata service URI format: `zk+hierarchical://{zkServers}{ledgerRoot}`
Example: `zk+hierarchical://zk1:2181,zk2:2181/ledgers`

## 7. Layout Manager

The layout znode stores which LedgerManager implementation is in use.

**Path**: `{root}/LAYOUT`

**Operations**:
```
interface LayoutManager:
    readLedgerLayout() → Optional<LedgerLayout>
    storeLedgerLayout(layout: LedgerLayout)  // throws if already exists
    deleteLedgerLayout()
```

**LedgerLayout** contains:
- Manager factory class name (e.g., `"HierarchicalLedgerManagerFactory"`)
- Layout format version

This ensures all clients/bookies agree on the metadata format.

## 8. Underreplication Tracking (ZK Paths)

### 8.1 Path Structure

```
{root}/underreplication/
  LAYOUT                         — format version
  ledgers/
    {hex4}/{hex4}/{hex4}/{hex4}/ — 4-level hex hierarchy from ledger ID
      urL{ledgerId}             — underreplicated ledger marker
  locks/
    urL{ledgerId}               — replication lock
```

**Hex hierarchy construction** (from 64-bit ledger ID):
```
subdir1 = format("%04x", (ledgerId >> 48) & 0xFFFF)
subdir2 = format("%04x", (ledgerId >> 32) & 0xFFFF)
subdir3 = format("%04x", (ledgerId >> 16) & 0xFFFF)
subdir4 = format("%04x", ledgerId & 0xFFFF)
path = {urLedgerPath}/{subdir1}/{subdir2}/{subdir3}/{subdir4}/urL{ledgerId}
```

### 8.2 Znode Content

**Underreplicated ledger marker** (protobuf text format):
```protobuf
message UnderreplicatedLedgerFormat {
    repeated string replica = 1;    // Missing replica bookie addresses
    optional int64 ctime = 2;       // When it was marked underreplicated
}
```

**Lock znode** (protobuf text format):
```protobuf
message LockDataFormat {
    optional string bookieId = 1;   // Bookie holding the lock (informational)
}
```

### 8.3 Operations

- **Mark**: Create or update `urL{ledgerId}` znode with replica list
- **Lock**: Create PERSISTENT `locks/urL{ledgerId}` (acts as a mutex)
- **Process**: Read ledger marker, perform replication
- **Release**: Delete lock znode
- **Clear**: Delete ledger marker znode after successful replication
- **Global disable**: A special znode can disable all replication cluster-wide
