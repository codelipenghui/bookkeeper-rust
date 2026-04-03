# Sub-Spec 08: Configuration

**Target**: All configuration parameters with types, defaults, and semantics.

## 1. Configuration Sources

* **File**: `bk_server.conf` (server), client config file

* **Programmatic**: `ServerConfiguration` / `ClientConfiguration` classes

* **Command-line**: Override individual parameters

Configuration is key-value based (Java Properties format).

## 2. Server Identity

| Parameter                  | Type   | Default | Description                                       |
| -------------------------- | ------ | ------- | ------------------------------------------------- |
| `bookiePort`               | int    | 3181    | TCP port for bookie protocol                      |
| `advertisedAddress`        | string | (none)  | Advertised hostname/IP (overrides auto-detection) |
| `useHostNameAsBookieID`    | bool   | false   | Use hostname (true) or IP (false) for bookie ID   |
| `useShortHostName`         | bool   | false   | Use short hostname instead of FQDN                |
| `listeningInterface`       | string | (none)  | Network interface to bind to                      |
| `allowLoopback`            | bool   | false   | Allow binding to loopback interface               |
| `allowEphemeralPorts`      | bool   | false   | Allow port 0 (OS-assigned)                        |
| `enableLocalTransport`     | bool   | false   | Enable in-process transport (testing)             |
| `bookieDeathWatchInterval` | int    | 1000    | Interval (ms) to check bookie death               |

## 3. Storage Directories

| Parameter               | Type   | Default           | Description                                       |
| ----------------------- | ------ | ----------------- | ------------------------------------------------- |
| `journalDirectory`      | string | `/tmp/bk-txn`     | Primary journal directory                         |
| `journalDirectories`    | string | (none)            | Multiple journal dirs (pipe-separated)            |
| `ledgerDirectories`     | string | `/tmp/bk-data`    | Ledger storage directories (comma-separated)      |
| `indexDirectories`      | string | (none)            | Index directories (defaults to ledgerDirectories) |
| `ledgerStorageClass`    | string | `DbLedgerStorage` | Storage implementation class                      |
| `allowStorageExpansion` | bool   | true              | Allow adding new directories after initial setup  |

## 4. Journal

| Parameter                         | Type | Default | Description                                    |
| --------------------------------- | ---- | ------- | ---------------------------------------------- |
| `journalMaxSizeMB`                | int  | 2048    | Max journal file size before rotation          |
| `journalMaxBackups`               | int  | 5       | Max number of old journal files to keep        |
| `journalSyncData`                 | bool | false   | fsync data (not just metadata) on each flush   |
| `journalWriteData`                | bool | true    | Write data to journal (false = no durability)  |
| `journalAdaptiveGroupWrites`      | bool | true    | Adaptive group commit sizing                   |
| `journalMaxGroupWaitMSec`         | int  | 1       | Max time (ms) to wait before group commit      |
| `journalBufferedWritesThreshold`  | int  | 524288  | Buffer size threshold to trigger flush (bytes) |
| `journalBufferedEntriesThreshold` | int  | 0       | Entry count threshold (0=disabled)             |
| `journalPreAllocSizeMB`           | int  | 32      | Pre-allocate journal file size (MB)            |
| `journalWriteBufferSizeKB`        | int  | 64      | Write buffer size (KB)                         |
| `journalAlignmentSize`            | int  | 4096    | Alignment for padding (V5+)                    |
| `journalFormatVersionToWrite`     | int  | 6       | Journal format version                         |
| `journalFlushWhenQueueEmpty`      | bool | false   | Flush when queue drains (vs. wait for batch)   |

## 5. Entry Logs

| Parameter                          | Type | Default          | Description                  |
| ---------------------------------- | ---- | ---------------- | ---------------------------- |
| `logSizeLimit`                     | long | 1073741823 (1GB) | Max entry log file size      |
| `entryLogFilePreallocationEnabled` | bool | true             | Pre-allocate entry log files |
| `entryLogPerLedgerEnabled`         | bool | false            | One entry log per ledger     |
| `readBufferSizeBytes`              | int  | 4096             | Read buffer for entry logs   |
| `writeBufferSizeBytes`             | int  | 65536            | Write buffer for entry logs  |

## 6. DbLedgerStorage

| Parameter                                 | Type | Default                 | Description                       |
| ----------------------------------------- | ---- | ----------------------- | --------------------------------- |
| `dbStorage_writeCacheMaxSizeMb`           | int  | (25% direct mem / dirs) | Write cache max size per dir (MB) |
| `dbStorage_readAheadCacheMaxSizeMb`       | int  | (25% direct mem / dirs) | Read cache max size per dir (MB)  |
| `dbStorage_readAheadCacheBatchSize`       | int  | 100                     | Read-ahead batch size             |
| `dbStorage_rocksDB_blockSize`             | int  | 65536                   | RocksDB block size (bytes)        |
| `dbStorage_rocksDB_writeBufferSizeMB`     | int  | 64                      | RocksDB write buffer (MB)         |
| `dbStorage_rocksDB_sstSizeInMB`           | int  | 64                      | RocksDB SST file size (MB)        |
| `dbStorage_rocksDB_blockCacheSize`        | long | (10% direct mem / dirs) | RocksDB block cache (bytes)       |
| `dbStorage_rocksDB_numLevels`             | int  | -1                      | RocksDB compaction levels         |
| `dbStorage_rocksDB_numFilesInLevel0`      | int  | 4                       | RocksDB L0 file count trigger     |
| `dbStorage_rocksDB_maxSizeInLevel1MB`     | int  | 256                     | RocksDB L1 max size (MB)          |
| `dbStorage_rocksDB_format_version`        | int  | 5                       | RocksDB format version            |
| `dbStorage_rocksDB_lz4CompressionEnabled` | bool | true                    | Enable LZ4 compression            |
| `dbStorage_rocksDB_bloomFilterBitsPerKey` | int  | 10                      | Bloom filter bits per key         |

## 7. Network & Threading

| Parameter                         | Type | Default       | Description                                |
| --------------------------------- | ---- | ------------- | ------------------------------------------ |
| `serverTcpNoDelay`                | bool | true          | TCP\_NODELAY                               |
| `serverSockKeepalive`             | bool | true          | SO\_KEEPALIVE                              |
| `serverTcpLinger`                 | int  | -1            | SO\_LINGER (-1=disabled)                   |
| `serverNumIOThreads`              | int  | 2 × CPU       | Netty I/O threads                          |
| `numAddWorkerThreads`             | int  | 1             | ADD\_ENTRY worker threads                  |
| `numReadWorkerThreads`            | int  | 8             | READ\_ENTRY worker threads                 |
| `numLongPollWorkerThreads`        | int  | 0             | Long-poll worker threads (0=use read pool) |
| `numHighPriorityWorkerThreads`    | int  | 8             | High-priority worker threads               |
| `maxPendingAddRequestsPerThread`  | int  | 10000         | Max queued add requests per thread         |
| `maxPendingReadRequestsPerThread` | int  | 10000         | Max queued read requests per thread        |
| `nettyMaxFrameSizeBytes`          | int  | 5242880 (5MB) | Max Netty frame size                       |

## 8. Disk Management

| Parameter                          | Type  | Default | Description                          |
| ---------------------------------- | ----- | ------- | ------------------------------------ |
| `diskUsageThreshold`               | float | 0.95    | Trigger read-only mode (fraction)    |
| `diskUsageWarnThreshold`           | float | 0.90    | Log warnings (fraction)              |
| `diskUsageLwmThreshold`            | float | 0.90    | Recover from read-only (fraction)    |
| `diskCheckInterval`                | int   | 10000   | Disk check interval (ms)             |
| `readOnlyModeEnabled`              | bool  | true    | Enable read-only mode on disk full   |
| `readOnlyModeOnAnyDiskFullEnabled` | bool  | false   | Read-only if any single disk is full |
| `isForceGCAllowWhenNoSpace`        | bool  | false   | Force GC when disk full              |

## 9. Compaction & Garbage Collection

| Parameter                        | Type  | Default   | Description                                 |
| -------------------------------- | ----- | --------- | ------------------------------------------- |
| `minorCompactionInterval`        | long  | 3600      | Minor compaction interval (seconds)         |
| `minorCompactionThreshold`       | float | 0.1       | Minor compaction usage threshold            |
| `majorCompactionInterval`        | long  | 604800    | Major compaction interval (seconds, 7 days) |
| `majorCompactionThreshold`       | float | 0.5       | Major compaction usage threshold            |
| `compactionRate`                 | int   | 1000000   | Compaction rate (entries/s)                 |
| `compactionRateByBytes`          | int   | 1000000   | Compaction rate (bytes/s)                   |
| `isThrottleByBytes`              | bool  | true      | Throttle by bytes (true) or entries (false) |
| `gcWaitTime`                     | long  | 900000    | GC thread sleep time between runs (ms)      |
| `gcOverreplicatedLedgerWaitTime` | long  | 604800000 | Wait time before GC over-replicated (ms)    |

## 10. Replication & Auto-Recovery

| Parameter                                                 | Type   | Default | Description                                          |
| --------------------------------------------------------- | ------ | ------- | ---------------------------------------------------- |
| `autoRecoveryDaemonEnabled`                               | bool   | true    | Enable auto-recovery daemon                          |
| `auditorPeriodicCheckInterval`                            | long   | 604800  | Full ledger check interval (seconds)                 |
| `auditorPeriodicBookieCheckInterval`                      | long   | 3600    | Bookie check interval (seconds)                      |
| `auditorLedgerVerificationPercentage`                     | long   | 0       | % of ledgers to deeply verify (0=metadata only)      |
| `lostBookieRecoveryDelay`                                 | int    | 1800    | Delay before recovery after bookie failure (seconds) |
| `rwRereplicateBackoffMs`                                  | int    | 5000    | Replication worker backoff on failure (ms)           |
| `openLedgerRereplicationGracePeriod`                      | string | 30s     | Grace period for open ledger replication             |
| `auditorMaxNumberOfConcurrentOpenLedgerOperations`        | int    | 0       | Max concurrent open operations (0=unlimited)         |
| `auditorAcquireConcurrentOpenLedgerOperationsTimeoutMSec` | long   | 120000  | Timeout for acquiring open operation slot            |

## 11. Client Configuration

| Parameter                       | Type   | Default                            | Description                                  |
| ------------------------------- | ------ | ---------------------------------- | -------------------------------------------- |
| `metadataServiceUri`            | string | (required)                         | Metadata service URI                         |
| `zkTimeout`                     | int    | 10000                              | ZooKeeper session timeout (ms)               |
| `useV2WireProtocol`             | bool   | false                              | Use v2 binary (true) or v3 protobuf (false)  |
| `readTimeout`                   | int    | 5                                  | Read timeout (seconds)                       |
| `addEntryTimeout`               | int    | 5                                  | Write timeout (seconds)                      |
| `addEntryQuorumTimeout`         | int    | (addEntryTimeout)                  | Quorum timeout (seconds)                     |
| `speculativeReadTimeout`        | int    | -1                                 | Speculative read delay (ms, -1=disabled)     |
| `numChannelsPerBookie`          | int    | 1                                  | TCP connections per bookie                   |
| `enableDigestTypeAutodetection` | bool   | true                               | Auto-detect digest type on open              |
| `ensemblePlacementPolicy`       | string | `RackawareEnsemblePlacementPolicy` | Placement policy class                       |
| `stickyReadBookieIndex`         | bool   | false                              | Use same bookie for sequential reads         |
| `explicitLacIntervalMs`         | int    | 0                                  | Explicit LAC broadcast interval (0=disabled) |

## 12. Placement Policy

| Parameter                               | Type   | Default            | Description                               |
| --------------------------------------- | ------ | ------------------ | ----------------------------------------- |
| `reppDnsResolverClass`                  | string | (default resolver) | DNS-to-rack resolver class                |
| `enforceMinNumRacksPerWriteQuorum`      | bool   | false              | Enforce rack diversity                    |
| `minNumRacksPerWriteQuorum`             | int    | 2                  | Min racks per write quorum                |
| `enforceMinNumFaultDomainsForWrite`     | bool   | false              | Enforce fault domain diversity for acks   |
| `networkTopologyStabilizePeriodSeconds` | int    | 0                  | Delay before using new topology (seconds) |

## 13. Security / TLS

| Parameter                        | Type   | Default | Description                      |
| -------------------------------- | ------ | ------- | -------------------------------- |
| `tlsProvider`                    | string | (none)  | TLS provider (e.g., "OpenSSL")   |
| `tlsProviderFactoryClass`        | string | (none)  | TLS context factory class        |
| `tlsClientAuthentication`        | bool   | false   | Require client TLS certificate   |
| `tlsKeyStoreType`                | string | `JKS`   | Keystore type                    |
| `tlsKeyStore`                    | string | (none)  | Path to keystore                 |
| `tlsKeyStorePasswordPath`        | string | (none)  | Path to keystore password file   |
| `tlsTrustStoreType`              | string | `JKS`   | Truststore type                  |
| `tlsTrustStore`                  | string | (none)  | Path to truststore               |
| `tlsTrustStorePasswordPath`      | string | (none)  | Path to truststore password file |
| `bookieAuthProviderFactoryClass` | string | (none)  | Auth provider factory class      |

## 14. HTTP Server (Admin API)

| Parameter           | Type   | Default      | Description                      |
| ------------------- | ------ | ------------ | -------------------------------- |
| `httpServerEnabled` | bool   | false        | Enable HTTP admin API            |
| `httpServerPort`    | int    | 8080         | HTTP server port                 |
| `httpServerClass`   | string | (Vertx impl) | HTTP server implementation class |

## 15. Compatibility Notes

Parameters that affect **wire format compatibility** (must match between Java and Rust):

* `useV2WireProtocol` — determines protocol version

* `digestType` — digest algorithm selection

* `MASTER_KEY_LENGTH` — fixed at 20 (not configurable, hardcoded)

Parameters that affect **on-disk format compatibility** (important for data sharing):

* `journalFormatVersionToWrite` — journal file format

* `ledgerStorageClass` — storage engine (DbLedgerStorage, etc.)

* `dbStorage_rocksDB_format_version` — RocksDB format version

Parameters that affect **metadata format compatibility**:

* `metadataFormatVersion` — ledger metadata serialization

* `ledgerManagerFactoryClass` — ZK path structure

