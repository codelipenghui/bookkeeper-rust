# Java Client Compatibility Tests — Design Spec

## Goal

Verify that the Rust bookie server is 100% wire-compatible with the Java BookKeeper client by running the full BookKeeper client (with real ZooKeeper) against the Rust bookie.

## Scope

Two deliverables:

1. **V3 protobuf protocol support** in the Rust bookie server (auto-detect V2 binary vs V3 protobuf)
2. **Java integration test project** using the full `BookKeeper` client with ZK discovery

## Part 1: V3 Protocol Support in Rust Server

### Auto-Detection

When a TCP frame arrives, the server must auto-detect whether it's V2 binary or V3 protobuf:

- **V2 detection**: The first 4 bytes are a `PacketHeader` where bits 24-31 contain the version (0-2). If version byte is 0, 1, or 2 → treat as V2 binary.
- **V3 detection**: If V2 decode fails, attempt protobuf decode of `bookkeeper_proto::Request`. If successful → V3.
- **Per-connection stickiness**: Once V3 is detected on a connection, all subsequent messages on that connection are V3. This matches Java's `ResponseDecoder` behavior.

### V3 Request Processing

1. Decode `bookkeeper_proto::Request` from protobuf bytes
2. Extract `BKPacketHeader` — contains `txnId`, `OperationType`, `ProtocolVersion`
3. Map `OperationType` to handler (same logic as V2 handlers)
4. Construct `bookkeeper_proto::Response` with:
   - Same `txnId` echoed back in the header
   - Appropriate `StatusCode` (V3 values: EOK=0, ENOLEDGER=402, etc.)
   - Operation-specific response fields

### Operations to Support

| V3 OperationType | Handler | Response Type |
|---|---|---|
| ADD_ENTRY | Same as V2 AddEntry | AddResponse (status, ledgerId, entryId) |
| READ_ENTRY | Same as V2 ReadEntry | ReadResponse (status, ledgerId, entryId, body, maxLAC) |
| WRITE_LAC | Same as V2 WriteLac | WriteLacResponse (status, ledgerId) |
| READ_LAC | Same as V2 ReadLac | ReadLacResponse (status, ledgerId, lacBody, lastEntryBody) |
| GET_BOOKIE_INFO | Same as V2 GetBookieInfo | GetBookieInfoResponse (status, totalDiskCapacity, freeDiskSpace) |
| FORCE_LEDGER | Flush storage | ForceLedgerResponse (status, ledgerId) |
| BATCH_READ_ENTRY | Same as V2 BatchRead | BatchReadResponse (entries) |
| AUTH | Return EOK | AuthMessage |
| START_TLS | Not supported | Return EBADREQ |

### Status Code Mapping

V3 uses the protobuf `StatusCode` enum values (different from V2):
- EOK = 0
- ENOLEDGER = 402, ENOENTRY = 403, EBADREQ = 404
- EIO = 501, EUA = 502, EBADVERSION = 503, EFENCED = 504
- EREADONLY = 505, ETOOMANYREQUESTS = 506

The existing `BKError::to_v3_code()` already provides this mapping.

### Key Differences from V2

| Aspect | V2 | V3 |
|---|---|---|
| Encoding | Binary (manual) | Protobuf |
| Correlation | (ledgerId, entryId, opType) | txnId (monotonic uint64) |
| Status codes | 0, 1, 2, 100-107 | 0, 402-507 |
| Master key in AddEntry | Separate 20-byte field | bytes field in AddRequest |
| Entry body in AddEntry | Trailing bytes after header | bytes field in AddRequest.body |
| Fencing flag | FLAG_DO_FENCING (0x0001) | ReadRequest.Flag.FENCE_LEDGER |
| Recovery flag | FLAG_RECOVERY_ADD (0x0002) | AddRequest.Flag.RECOVERY_ADD |

### V3 Framing

V3 messages use the same 4-byte length-prefixed framing as V2. The frame payload is a protobuf-encoded `Request` or `Response`. The existing `BookieCodec` handles framing for both.

## Part 2: Java Integration Test Project

### Location

```
tests/java-compat/
├── pom.xml
├── src/test/java/
│   └── org/apache/bookkeeper/compat/
│       └── RustBookieCompatTest.java
└── run-tests.sh
```

### Maven Project (pom.xml)

- Depends on `org.apache.bookkeeper:bookkeeper-server:4.18.0-SNAPSHOT` (includes client + embedded ZK)
- Uses JUnit 5
- Uses Curator `TestingServer` for embedded ZK (comes with BK server dependency)
- System properties: `-Dbookie.port={port}` passed from the test script

### Prerequisites

The Java BookKeeper project must be built locally first:
```bash
cd /Users/lipenghui/Github/apache/bookkeeper
mvn install -DskipTests -pl bookkeeper-server -am
```

### Test Script (run-tests.sh)

```
1. Build Java BK if not already built (mvn install -DskipTests)
2. Build Rust bookie (cargo build -p bookkeeper-server -p bookkeeper-cli)
3. Start embedded ZK via a small Java helper (Curator TestingServer on random port)
4. Initialize BK cluster in ZK (bk initnewcluster --zk localhost:{zkPort})
5. Start Rust bookie pointing at ZK (background process, port 0)
6. Wait for bookie to register in ZK
7. Run mvn test -f tests/java-compat/pom.xml -Dzk.connect=localhost:{zkPort}
8. Capture exit code
9. Stop Rust bookie
10. Stop ZK
11. Exit with test exit code
```

### Test Cases (RustBookieCompatTest.java)

All tests use the standard `BookKeeper` client API with `ClientConfiguration`:

**Test 1: Create + Write + Read + Close (V3 default)**
```java
BookKeeper bk = new BookKeeper(clientConf);
LedgerHandle wh = bk.createLedger(1, 1, 1, DigestType.CRC32C, "test".getBytes());
for (int i = 0; i < 10; i++) {
    wh.addEntry(("entry-" + i).getBytes());
}
long lac = wh.getLastAddConfirmed();
wh.close();

LedgerHandle rh = bk.openLedgerNoRecovery(wh.getId(), DigestType.CRC32C, "test".getBytes());
Enumeration<LedgerEntry> entries = rh.readEntries(0, lac);
// verify each entry matches
rh.close();
bk.close();
```

**Test 2: V2 Wire Protocol**
Same as Test 1 but with `clientConf.setUseV2WireProtocol(true)`.

**Test 3: Multiple Digest Types**
- CRC32 (`DigestType.CRC32`)
- CRC32C (`DigestType.CRC32C`)
- MAC (`DigestType.MAC`)

Each: create ledger, write 5 entries, close, reopen, read back, verify digest.

**Test 4: Fencing**
```java
LedgerHandle wh = bk.createLedger(1, 1, 1, DigestType.CRC32C, password);
wh.addEntry("data".getBytes());
// Open with recovery (fences the writer)
LedgerHandle rh = bk.openLedger(wh.getId(), DigestType.CRC32C, password);
// Old writer should be fenced
try {
    wh.addEntry("after-fence".getBytes());
    fail("should have been fenced");
} catch (BKException.BKLedgerFencedException e) {
    // expected
}
```

**Test 5: Large Entries**
Write entries of 64KB and 256KB, read back, verify.

**Test 6: Empty Password**
Create ledger with empty password (`new byte[0]`), write, read, verify. Tests the `EMPTY_LEDGER_KEY = SHA1("ledger" + "")` path.

**Test 7: Many Entries**
Write 1000 entries, read all back, verify sequential correctness.

### Success Criteria

All 7 Java test cases pass when running against the Rust bookie. The Java client uses its default settings (V3 protobuf) and also verifies V2 compatibility.
