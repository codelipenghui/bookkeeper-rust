# Sub-Spec 09: Admin HTTP API

**Target**: REST endpoints for monitoring and administration.

## 1. Overview

The bookie exposes an HTTP server for administration and monitoring. Enabled via `httpServerEnabled=true` on port `httpServerPort` (default 8080).

All responses are JSON unless otherwise noted.

## 2. Heartbeat & Readiness

### GET /heartbeat

Liveness check. Returns `200 OK` with text body `OK`.

### GET /api/v1/bookie/is\_ready

Readiness probe. Returns:

* `200 OK`: Bookie is ready to accept requests

* `503 Service Unavailable`: Bookie is not ready

## 3. Bookie State

### GET /api/v1/bookie/state

Returns the current bookie state.

**Response**:

```json
{
  "running": true,
  "readOnly": false,
  "shuttingDown": false,
  "availableForHighPriorityWrites": true
}
```

### GET /api/v1/bookie/info

Returns bookie identity and disk information.

**Response**:

```json
{
  "freeSpace": 107374182400,
  "totalSpace": 214748364800
}
```

## 4. Configuration

### GET /api/v1/config/server\_config

Dumps the full server configuration as JSON key-value pairs.

**Response**:

```json
{
  "bookiePort": "3181",
  "journalDirectory": "/data/journal",
  "ledgerDirectories": "/data/ledgers",
  ...
}
```

## 5. Ledger Operations

### GET /api/v1/ledger/metadata?ledger\_id={id}

Read metadata for a specific ledger.

**Parameters**: `ledger_id` (required, int64)

**Response** (200):

```json
{
  "ledgerId": 12345,
  "ensembleSize": 3,
  "writeQuorumSize": 2,
  "ackQuorumSize": 2,
  "state": "CLOSED",
  "lastEntryId": 999,
  "length": 1048576,
  "digestType": "CRC32C",
  "ensembles": {
    "0": ["bookie1:3181", "bookie2:3181", "bookie3:3181"]
  }
}
```

**Error** (404): Ledger not found

### DELETE /api/v1/ledger/metadata?ledger\_id={id}

Delete a ledger's metadata. **Dangerous**: data becomes orphaned.

**Parameters**: `ledger_id` (required, int64)

**Response**: `200 OK` on success

### GET /api/v1/ledger/list?print\_metadata={bool}

List all ledgers.

**Parameters**: `print_metadata` (optional, default false) — include full metadata for each ledger

**Response**:

```json
{
  "ledgerIds": [1, 2, 3, 100, 200]
}
```

Or with metadata:

```json
{
  "ledgers": [
    {
      "ledgerId": 1,
      "metadata": { ... }
    }
  ]
}
```

## 6. Auto-Recovery

### GET /api/v1/autorecovery/status

Returns whether auto-recovery (replication) is enabled.

**Response**:

```json
{
  "enabled": true
}
```

### PUT /api/v1/autorecovery/status

Enable or disable auto-recovery.

**Request body**:

```json
{
  "enabled": false
}
```

### GET /api/v1/autorecovery/auditor

Returns the current auditor's identity.

**Response**:

```json
{
  "auditor": "bookie1:3181"
}
```

### PUT /api/v1/autorecovery/lost\_bookie\_recovery\_delay

Set the lost bookie recovery delay.

**Request body**:

```json
{
  "delay_seconds": 600
}
```

### PUT /api/v1/autorecovery/decommission

Decommission a bookie. Marks all its ledgers for replication.

**Request body**:

```json
{
  "bookie_src": "bookie3:3181"
}
```

## 7. Garbage Collection

### PUT /api/v1/bookie/gc

Trigger garbage collection manually.

**Response**: `200 OK` — GC triggered

### GET /api/v1/bookie/gc

Check if GC is currently running.

**Response**:

```json
{
  "is_in_force_gc": false
}
```

### GET /api/v1/bookie/gc\_details

Detailed GC status per entry log.

**Response**:

```json
[
  {
    "logId": 0,
    "status": "ACTIVE",
    "totalSize": 1073741824,
    "usableSize": 805306368,
    "usagePercent": 75.0
  }
]
```

## 8. Metrics

### GET /metrics

Prometheus-format metrics endpoint.

**Response**: Text in Prometheus exposition format:

```
# HELP bookie_write_bytes_total Total bytes written
# TYPE bookie_write_bytes_total counter
bookie_write_bytes_total 123456789
# HELP bookie_read_bytes_total Total bytes read
# TYPE bookie_read_bytes_total counter
bookie_read_bytes_total 987654321
...
```

Key metric categories:

* **Journal**: write latency, sync latency, queue depth, flush counts

* **Storage**: add/read latency, cache hit rates, entry counts

* **Network**: connections, request rates, error rates

* **Disk**: usage percentages, I/O throughput

* **Replication**: underreplicated count, replication latency, auditor status

## 9. Implementation Priority

This sub-spec is lower priority. For a Rust reimplementation:

1. Start with `/heartbeat` and `/api/v1/bookie/is_ready` (operational basics)
2. Add `/api/v1/bookie/state` and `/metrics` (monitoring)
3. Add ledger and GC endpoints (administration)
4. Add auto-recovery endpoints (management)

