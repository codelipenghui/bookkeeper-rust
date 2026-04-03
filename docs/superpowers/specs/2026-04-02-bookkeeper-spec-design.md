# Apache BookKeeper Specification — Master Architecture Document

**Version**: 4.18.0-SNAPSHOT | **Protocol**: v1, v2, v3 | **Target**: Language-agnostic reimplementation with full interoperability

## 1. Overview

Apache BookKeeper is a distributed log storage service providing durable, replicated, low-latency append-only storage. The fundamental abstraction is the **ledger** — an ordered sequence of entries replicated across multiple storage nodes called **bookies**.

Key properties:
- **Strong durability**: entries are persisted to a write-ahead log (journal) and acknowledged only after fsync
- **Replication**: each entry is replicated to a configurable write quorum of bookies
- **Single-writer semantics**: a ledger has exactly one writer at a time, enforced via fencing
- **Last Add Confirmed (LAC)**: a consistency boundary — readers can only see entries up to the LAC
- **Append-only**: entries cannot be modified or deleted individually; the entire ledger is deleted as a unit

## 2. Cluster Topology

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Client     │     │   Client     │     │   Client     │
│  (library)   │     │  (library)   │     │  (library)   │
└──────┬───────┘     └──────┬───────┘     └──────┬───────┘
       │                    │                    │
       │         TCP (BookKeeper Protocol)       │
       │                    │                    │
       ▼                    ▼                    ▼
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Bookie 1   │     │   Bookie 2   │     │   Bookie 3   │
│  (storage)   │     │  (storage)   │     │  (storage)   │
└──────┬───────┘     └──────┬───────┘     └──────┬───────┘
       │                    │                    │
       └────────────────────┼────────────────────┘
                            │
                     ┌──────┴───────┐
                     │  ZooKeeper    │
                     │  (metadata)   │
                     └──────────────┘
```

**Bookies**: Storage servers that persist ledger entries. Each bookie runs a TCP server accepting client requests and stores data in a journal (WAL) + entry logs backed by RocksDB indexing.

**Metadata Service (ZooKeeper)**: Stores ledger metadata (ensemble membership, state, quorum sizes), bookie registration (which bookies are alive), and coordination data (underreplication tracking, auditor election). Clients and bookies both connect to ZK.

**Clients**: Libraries that create, write to, read from, and manage ledgers. Clients talk directly to bookies for data operations and to ZK for metadata. There is no central broker or proxy — the client is the coordinator.

## 3. Core Data Model

### 3.1 Ledger

A ledger is identified by a 64-bit integer (`ledgerId`). It has:

| Field | Type | Description |
|-------|------|-------------|
| `ledgerId` | int64 | Unique identifier, allocated by ZK |
| `ensembleSize` | int32 | Number of bookies in the ensemble |
| `writeQuorumSize` | int32 | Number of bookies each entry is written to |
| `ackQuorumSize` | int32 | Number of acks required before confirming a write |
| `state` | enum | OPEN, IN_RECOVERY, CLOSED |
| `digestType` | enum | CRC32, CRC32C, HMAC_SHA1, DUMMY |
| `password` | bytes | Ledger access credential |
| `lastEntryId` | int64 | Last entry ID (set on close) |
| `length` | int64 | Total bytes written (set on close) |
| `ctime` | int64 | Creation timestamp |
| `customMetadata` | map<string, bytes> | Application-defined metadata |
| `ensembles` | map<int64, list<BookieId>> | Ensemble segments (firstEntryId → bookie list) |

**Invariants**:
- `ensembleSize >= writeQuorumSize >= ackQuorumSize >= 1`
- At least one ensemble segment starting at entry 0
- Each ensemble segment has exactly `ensembleSize` bookies

### 3.2 Entry

An entry is identified by `(ledgerId, entryId)`. Entry IDs are sequential starting from 0.

**Wire/storage format** (32 + macCodeLength + payload bytes):
```
Offset  Size    Field
0       8       ledgerId (int64, big-endian)
8       8       entryId (int64, big-endian)
16      8       lastAddConfirmed (int64, big-endian)
24      8       length (int64, big-endian) — total bytes written so far
32      N       digest (8 bytes for CRC32/CRC32C stored as int64; 20 bytes for HMAC; 0 for DUMMY)
32+N    M       payload (application data)
```

The digest covers bytes [0, 32) concatenated with the payload.

### 3.3 Ensemble Segments

A ledger's bookie set can change during its lifetime. When a bookie in the write set fails, the client replaces it, creating a new **ensemble segment**:

```
Segment 0: firstEntryId=0,   ensemble=[B1, B2, B3, B4, B5]
Segment 1: firstEntryId=100, ensemble=[B1, B6, B3, B4, B5]  // B2 replaced by B6
```

The distribution schedule determines which bookies in the ensemble receive each entry (see Sub-Spec 06).

### 3.4 Distribution Schedule (Round-Robin)

For a given entry with `entryId=e`, ensemble size `E`, and write quorum `W`:

```
writeSet(e) = [(e + 0) % E, (e + 1) % E, ..., (e + W - 1) % E]
```

This determines which `W` bookies out of the `E`-member ensemble store entry `e`.

## 4. Ledger Lifecycle

```
         ┌──────┐
         │CREATE│
         └──┬───┘
            │ allocate ID, store metadata, select ensemble
            ▼
         ┌──────┐
         │ OPEN │◄──────────────────────────────┐
         └──┬───┘                               │
            │ append entries                    │
            │ (ensemble change on failure)      │
            ▼                                   │
    ┌───────────────┐                           │
    │ Close or Fence│                           │
    └───────┬───────┘                           │
            │                                   │
     ┌──────┴──────┐                            │
     ▼             ▼                            │
┌────────┐  ┌─────────────┐                     │
│ CLOSED │  │ IN_RECOVERY │─── recovery ────────┘
└────┬───┘  └─────────────┘    completes → CLOSED
     │
     ▼
  ┌──────┐
  │DELETE│
  └──────┘
```

- **Create**: Client allocates a ledger ID, selects an initial ensemble via placement policy, stores metadata in ZK.
- **Write**: Client appends entries. Each entry goes to `writeQuorumSize` bookies. After `ackQuorumSize` acks, the entry is confirmed and LAC advances.
- **Close**: Client flushes all pending writes, updates metadata with lastEntryId and length, sets state=CLOSED.
- **Recovery**: A new client (or auditor) fences the ledger on all bookies, reads forward from LAC to find the true last entry, re-replicates as needed, then closes.
- **Delete**: Client removes metadata from ZK. Bookies eventually garbage-collect the data.

## 5. Consistency Model

### 5.1 Single Writer

At any time, at most one client can write to a ledger. This is enforced by:
1. The creating client holds the write handle
2. If another client attempts recovery, it **fences** the ledger — all bookies reject further non-recovery writes with `EFENCED`

### 5.2 Last Add Confirmed (LAC)

The LAC is the highest entry ID for which `ackQuorumSize` bookies have acknowledged the write. It defines the **consistency boundary**:
- Entries with `entryId <= LAC` are guaranteed durable and readable
- Entries with `entryId > LAC` may or may not exist

LAC propagation:
- **Piggyback**: Every ADD_ENTRY carries the writer's current LAC; every READ_ENTRY response includes the bookie's known LAC
- **Explicit LAC**: Writer periodically sends WRITE_LAC to bookies; readers can READ_LAC with long-polling for tailing reads

### 5.3 Fencing Protocol

When a client opens a ledger for recovery:
1. Send READ_ENTRY with `FLAG_DO_FENCING` + master key to ALL bookies in the current ensemble
2. Each bookie validates the master key, sets `fenced=true` for the ledger, returns its last entry + LAC
3. Once a fencing quorum (writeQuorumSize) is reached, the old writer is effectively locked out
4. Recovery writer uses `FLAG_RECOVERY_ADD` for re-replication (accepted even on fenced ledgers)

## 6. Sub-Spec Index

| # | Sub-Spec | Description |
|---|----------|-------------|
| 01 | [Wire Protocol](./01-wire-protocol.md) | TCP framing, protocol v1/v2/v3, all operations, entry format, status codes |
| 02 | [Metadata](./02-metadata.md) | Ledger metadata schema, ZK paths, ID generation, cookies, MetadataDriver interface |
| 03 | [Storage Engine](./03-storage-engine.md) | DbLedgerStorage, journal (WAL), entry logs, RocksDB, caches, compaction/GC |
| 04 | [Bookie Server](./04-bookie-server.md) | Request processing, write/read paths, fencing, state machine, disk management |
| 05 | [Bookie Registration](./05-bookie-registration.md) | Discovery, registration, cookies, service info, layout manager |
| 06 | [Client Library](./06-client.md) | Write/read paths, ensemble placement, distribution schedule, recovery, digest |
| 07 | [Replication & Recovery](./07-replication-recovery.md) | Auditor, replication worker, underreplication manager, fencing/LAC protocols |
| 08 | [Configuration](./08-configuration.md) | All configuration parameters with types, defaults, semantics |
| 09 | [Admin HTTP API](./09-admin-http-api.md) | REST endpoints for monitoring and administration |
| 10 | [CLI Tools](./10-cli-tools.md) | Shell commands for administration and debugging |

The sub-specs are ordered by implementation dependency — each builds on the ones before it.
