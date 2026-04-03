# Sub-Spec 01: Wire Protocol

**Target**: Full specification of the BookKeeper client-bookie binary protocol, all three versions.

## 1. Transport Layer

<br />

* **Protocol**: TCP with persistent connections

* **Framing**: Length-prefixed. Each message is preceded by a 4-byte big-endian integer representing the payload length (NOT including the 4-byte length field itself).

* **Max frame size**: Configurable via `nettyMaxFrameSizeBytes` (default: 5 MB for client, configurable for server)

* **Byte order**: Big-endian throughout (network byte order)

```
┌────────────────┬────────────────────────────────┐
│ Frame Length    │ Payload                        │
│ (4 bytes, BE)  │ (Frame Length bytes)            │
└────────────────┴────────────────────────────────┘
```

## 2. Protocol Versions

Three protocol versions exist. The version is determined by client configuration, NOT negotiated dynamically.

| Version | Encoding                | Configuration                                           |
| ------- | ----------------------- | ------------------------------------------------------- |
| v1      | Binary, fixed header    | Legacy, `useV2WireProtocol=true`                        |
| v2      | Binary, extended header | `useV2WireProtocol=true` (CURRENT\_PROTOCOL\_VERSION=2) |
| v3      | Protobuf                | `useV2WireProtocol=false` (default)                     |

**Version selection logic**:

* Default: v3 (protobuf)

* If `useV2WireProtocol=true`: v2 binary, except:

  * If TLS enabled: first message (START\_TLS) uses v3, then downgrades to v2

  * Fallback: if protobuf parse fails, fall back to v2 decode

**Constants**:

```
CURRENT_PROTOCOL_VERSION = 2
LOWEST_COMPAT_PROTOCOL_VERSION = 0
```

## 3. Pre-v3 Binary Protocol (v1/v2)

### 3.1 Packet Header (4 bytes)

The first 4 bytes of every pre-v3 message encode version, opcode, and flags:

```
Bits:  [31..24] [23..16] [15..0]
       version   opCode   flags

Encoding: int32 = (version << 24) | (opCode << 16) | (flags & 0xFFFF)

Special case: if version == 0, the entire int32 is treated as the opCode (backward compat)
```

### 3.2 Operation Codes (Pre-v3)

| OpCode             | Value | Description              |
| ------------------ | ----- | ------------------------ |
| ADDENTRY           | 1     | Write entry              |
| READENTRY          | 2     | Read entry               |
| AUTH               | 3     | Authentication           |
| READ\_LAC          | 4     | Read Last Add Confirmed  |
| WRITE\_LAC         | 5     | Write Last Add Confirmed |
| GET\_BOOKIE\_INFO  | 6     | Get bookie info          |
| BATCH\_READ\_ENTRY | 7     | Batch read entries       |

### 3.3 Flags (16-bit)

| Flag                 | Value  | Used With |
| -------------------- | ------ | --------- |
| FLAG\_NONE           | 0x0000 | Any       |
| FLAG\_DO\_FENCING    | 0x0001 | READENTRY |
| FLAG\_RECOVERY\_ADD  | 0x0002 | ADDENTRY  |
| FLAG\_HIGH\_PRIORITY | 0x0004 | Any       |

### 3.4 Request Formats (Pre-v3)

**ADD\_ENTRY Request**:

```
Offset  Size    Field
0       4       Packet Header (version=2, opCode=1, flags)
4       20      Master Key (MASTER_KEY_LENGTH=20, fixed)
24      8       Ledger ID (int64)
32      8       Entry ID (int64)
40      ...     Entry data (see Entry Wire Format, Section 6)
```

**READ\_ENTRY Request**:

```
Offset  Size    Field
0       4       Packet Header (version=2, opCode=2, flags)
4       8       Ledger ID (int64)
8       8       Entry ID (int64)
16      20      Master Key (only if FLAG_DO_FENCING set)
```

* Special entry ID: `-1` (LAST\_ADD\_CONFIRMED) means "read the LAC"

**AUTH Request**:

```
Offset  Size    Field
0       4       Packet Header (version=2, opCode=3, flags=0)
4       N       AuthMessage (protobuf serialized)
```

**WRITE\_LAC Request**:

```
Offset  Size    Field
0       4       Packet Header (version=2, opCode=5, flags=0)
4       8       Ledger ID (int64)
12      8       LAC value (int64)
20      20      Master Key (MASTER_KEY_LENGTH=20)
40      N       LAC digest data (ledgerId + lac + digest)
```

**READ\_LAC Request**:

```
Offset  Size    Field
0       4       Packet Header (version=2, opCode=4, flags=0)
4       8       Ledger ID (int64)
```

**GET\_BOOKIE\_INFO Request**:

```
Offset  Size    Field
0       4       Packet Header (version=2, opCode=6, flags)
4       8       Requested info mask (int64)
```

Flags in the mask: `FREE_DISK_SPACE=1<<0`, `TOTAL_DISK_SPACE=1<<1`

**BATCH\_READ\_ENTRY Request**:

```
Offset  Size    Field
0       4       Packet Header (version=2, opCode=7, flags)
4       8       Ledger ID (int64)
12      8       Start Entry ID (int64)
20      4       Max count (int32)
24      8       Max size (int64)
```

### 3.5 Response Formats (Pre-v3)

All pre-v3 responses share a common 24-byte header:

```
Offset  Size    Field
0       4       Packet Header (version|opCode|flags)
4       4       Error Code (int32)
8       8       Ledger ID (int64)
16      8       Entry ID (int64)
```

**ADD\_ENTRY Response** (24 bytes total):

```
[24-byte common header]
No additional payload.
```

**READ\_ENTRY Response**:

```
[24-byte common header]
24      N       Entry data (if status == EOK)
```

**AUTH Response**:

```
[24-byte common header]  (ledgerId=0, entryId=0)
24      N       AuthMessage (protobuf)
```

**WRITE\_LAC Response** (24 bytes total):

```
[24-byte common header]
```

**READ\_LAC Response**:

```
[24-byte common header]
24      N1      Last LAC data (explicit LAC bytes)
24+N1   N2      LAC from last entry (entry bytes with piggyback LAC)
```

**GET\_BOOKIE\_INFO Response**:

```
[24-byte common header]
24      8       Free disk space (int64)
32      8       Total disk space (int64)
```

**BATCH\_READ\_ENTRY Response**:

```
[24-byte common header]
24      8       Request ID (int64)
For each entry:
  +0    4       Entry size (int32)
  +4    N       Entry data
```

## 4. Protocol v3 (Protobuf)

v3 uses protocol buffers for all messages. The protobuf definitions are in `BookkeeperProtocol.proto`.

### 4.1 Top-Level Messages

```protobuf
message BKPacketHeader {
    required ProtocolVersion version = 1;
    required OperationType operation = 2;
    required uint64 txnId = 3;
    optional uint32 priority = 4;
}

message Request {
    required BKPacketHeader header = 1;
    optional ReadRequest readRequest = 100;
    optional AddRequest addRequest = 101;
    optional AuthMessage authRequest = 102;
    optional WriteLacRequest writeLacRequest = 103;
    optional ReadLacRequest readLacRequest = 104;
    optional GetBookieInfoRequest getBookieInfoRequest = 105;
    optional StartTLSRequest startTLSRequest = 106;
    optional ForceLedgerRequest forceLedgerRequest = 107;
    optional GetListOfEntriesOfLedgerRequest getListOfEntriesOfLedgerRequest = 108;
    optional BatchReadRequest batchReadRequest = 109;
}

message Response {
    required BKPacketHeader header = 1;
    required StatusCode status = 2;
    optional ReadResponse readResponse = 100;
    optional AddResponse addResponse = 101;
    optional AuthMessage authResponse = 102;
    optional WriteLacResponse writeLacResponse = 103;
    optional ReadLacResponse readLacResponse = 104;
    optional GetBookieInfoResponse getBookieInfoResponse = 105;
    optional StartTLSResponse startTLSResponse = 106;
    optional ForceLedgerResponse forceLedgerResponse = 107;
    optional GetListOfEntriesOfLedgerResponse getListOfEntriesOfLedgerResponse = 108;
    optional BatchReadResponse batchReadResponse = 109;
}
```

### 4.2 Operation Types (v3)

```protobuf
enum OperationType {
    READ_ENTRY = 1;
    ADD_ENTRY = 2;
    RANGE_READ_ENTRY = 3;                  // Not supported
    RANGE_ADD_ENTRY = 4;                   // Not supported
    AUTH = 5;
    WRITE_LAC = 6;
    READ_LAC = 7;
    GET_BOOKIE_INFO = 8;
    START_TLS = 9;
    FORCE_LEDGER = 10;
    GET_LIST_OF_ENTRIES_OF_LEDGER = 11;
    BATCH_READ_ENTRY = 12;
}
```

### 4.3 Request/Response Definitions

```protobuf
message ReadRequest {
    enum Flag {
        FENCE_LEDGER = 1;
        ENTRY_PIGGYBACK = 2;
    }
    optional Flag flag = 100;
    required int64 ledgerId = 1;
    required int64 entryId = 2;
    optional bytes masterKey = 3;
    optional int64 previousLAC = 4;
    optional int64 timeOut = 5;
}

message ReadResponse {
    required int64 ledgerId = 1;
    required int64 entryId = 2;
    optional bytes body = 3;
    optional int64 maxLAC = 4;
    optional int64 lacUpdateTimestamp = 5;
}

message AddRequest {
    enum Flag {
        RECOVERY_ADD = 1;
    }
    optional Flag flag = 100;
    required int64 ledgerId = 1;
    required int64 entryId = 2;
    required bytes masterKey = 3;
    required bytes body = 4;
    optional int32 writeFlags = 5;
}

message AddResponse {
    required int64 ledgerId = 1;
    required int64 entryId = 2;
}

message BatchReadRequest {
    enum Flag {
        FENCE_LEDGER = 1;
    }
    optional Flag flag = 100;
    required int64 ledgerId = 1;
    required int64 startEntryId = 2;
    required int32 maxCount = 3;
    required int64 maxSize = 4;
    optional bytes masterKey = 5;
    optional int64 previousLAC = 6;
    optional int64 timeOut = 7;
}

message BatchReadResponse {
    required int64 ledgerId = 1;
    required int64 startEntryId = 2;
    optional int64 maxLAC = 3;
    optional int64 lacUpdateTimestamp = 4;
    repeated ReadResponse entries = 100;
}

message WriteLacRequest {
    required int64 ledgerId = 1;
    required int64 lac = 2;
    required bytes masterKey = 3;
    required bytes body = 4;
}

message WriteLacResponse {
    required int64 ledgerId = 1;
}

message ReadLacRequest {
    required int64 ledgerId = 1;
}

message ReadLacResponse {
    required int64 ledgerId = 1;
    optional bytes lacBody = 2;
    optional bytes lastEntryBody = 3;
}

message ForceLedgerRequest {
    required int64 ledgerId = 1;
}

message ForceLedgerResponse {
    required int64 ledgerId = 1;
}

message GetBookieInfoRequest {
    optional int64 requested = 1;
}

message GetBookieInfoResponse {
    optional int64 totalDiskCapacity = 1;
    optional int64 freeDiskSpace = 2;
}

message GetListOfEntriesOfLedgerRequest {
    required int64 ledgerId = 1;
}

message GetListOfEntriesOfLedgerResponse {
    required int64 ledgerId = 1;
    optional bytes availabilityOfEntriesOfLedger = 2;
}

message StartTLSRequest {}
message StartTLSResponse {}

message AuthMessage {
    required string authPluginName = 1;
    required bytes payload = 2;
}
```

### 4.4 Status Codes

```protobuf
enum StatusCode {
    EOK = 0;
    ENOLEDGER = 402;
    ENOENTRY = 403;
    EBADREQ = 404;
    EIO = 501;
    EUA = 502;
    EBADVERSION = 503;
    EFENCED = 504;
    EREADONLY = 505;
    ETOOMANYREQUESTS = 506;
    EUNKNOWNLEDGERSTATE = 507;
}
```

Note: Pre-v3 uses different error code values:

```
EOK = 0
ENOLEDGER = 1
ENOENTRY = 2
EBADREQ = 100
EIO = 101
EUA = 102
EBADVERSION = 103
EFENCED = 104
EREADONLY = 105
ETOOMANYREQUESTS = 106
```

## 5. TLS Handshake (START\_TLS)

1. Client sends `StartTLSRequest` (v3 protobuf, even if v2 is configured)
2. Server responds with `StartTLSResponse` + status `EOK`
3. Both sides initiate TLS handshake on the existing TCP connection
4. If v2 was configured, protocol downgrades to v2 binary after TLS is established
5. All subsequent messages are TLS-encrypted

## 6. Entry Wire Format

The entry as transmitted in ADD\_ENTRY body and returned in READ\_ENTRY body:

```
Offset  Size    Field
0       8       ledgerId (int64, big-endian)
8       8       entryId (int64, big-endian)
16      8       lastAddConfirmed (int64, big-endian)
24      8       length (int64, big-endian) — cumulative bytes written to ledger
32      N       digest
32+N    M       payload (application data)
```

**Digest sizes**:

| Type       | MacCodeLength | Storage Size              | Notes                      |
| ---------- | ------------- | ------------------------- | -------------------------- |
| CRC32      | 4 bytes       | 8 bytes (stored as int64) | Java CRC32                 |
| CRC32C     | 4 bytes       | 8 bytes (stored as int64) | Hardware-accelerated       |
| HMAC\_SHA1 | 20 bytes      | 20 bytes                  | Keyed with ledger password |
| DUMMY      | 0 bytes       | 0 bytes                   | No integrity check         |

**Digest computation**:

1. Initialize digest with algorithm-specific state
2. Update with metadata bytes: `ledgerId(8) + entryId(8) + LAC(8) + length(8)` = 32 bytes
3. Update with payload bytes
4. Write digest value at offset 32

**METADATA\_LENGTH** = 32 bytes (the 4 int64 fields before digest)

## 7. Master Key

* **Length**: Fixed at 20 bytes (`MASTER_KEY_LENGTH = 20`)

* **Derivation**: Generated by `MacDigestManager` regardless of actual digest type

* **Purpose**: Used for entry-level authentication — bookie validates master key on writes

* **Empty password optimization**: An all-zero or static master key is used for empty passwords

## 8. Authentication Flow

1. Server creates a `BookieAuthProvider` instance per connection
2. Client sends AUTH message with `authPluginName` and initial `payload`
3. Server validates plugin name matches configured provider
4. Server processes payload, may respond with AUTH message containing next challenge
5. Multi-round exchange continues until provider signals completion
6. Once `authenticated=true`, normal operations proceed
7. Non-AUTH requests before authentication completes receive `EUA` (502)

## 9. Connection Lifecycle

### 9.1 Client Side (PerChannelBookieClient)

* States: `DISCONNECTED → CONNECTING → CONNECTED → CLOSED` (also `START_TLS` transient state)

* Lazy connection: established on first request

* Request/response correlation:

  * v2: `(ledgerId, entryId, opType)` tuple

  * v3: `txnId` (monotonically increasing uint64)

* Reconnection on channel failure with pending ops re-queued

### 9.2 Special Values

```
INVALID_ENTRY_ID = -1
LAST_ADD_CONFIRMED = -1  // Special entryId in READ_ENTRY: returns current LAC
SMALL_ENTRY_SIZE_THRESHOLD = 16384  // 16KB: optimization boundary for buffer allocation
```

## 10. Write Flags (v3 AddRequest)

```protobuf
enum WriteFlag {
    DEFERRED_SYNC = 0x1;  // Entry will be synced later via FORCE_LEDGER
}
```

When `DEFERRED_SYNC` is set, the bookie may acknowledge the write before fsync. The client must later send `FORCE_LEDGER` to ensure durability.
