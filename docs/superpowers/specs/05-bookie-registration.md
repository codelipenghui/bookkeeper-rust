# Sub-Spec 05: Bookie Registration & Discovery

**Target**: Bookie registration, discovery, cookie validation, service info, and layout management.

## 1. Registration Lifecycle

### 1.1 Startup Registration

```
1. Ensure parent znodes exist:
   - {root}/available/ (PERSISTENT)
   - {root}/available/readonly/ (PERSISTENT)
2. Serialize BookieServiceInfo to protobuf bytes
3. Create registration znode:
   - Writable: {root}/available/{bookieId} (EPHEMERAL) with ServiceInfo data
   - Read-only: {root}/available/readonly/{bookieId} (EPHEMERAL) with ServiceInfo data
4. Set ZK watcher on session events for re-registration
```

### 1.2 Ephemeral Znodes

Registration znodes are **ephemeral** — they are automatically deleted by ZooKeeper when the bookie's session expires. This is the fundamental mechanism for failure detection:

* Bookie crashes → ZK session expires → registration znode deleted → watchers notified

* Other bookies/auditor detect the disappearance and trigger recovery

### 1.3 Re-registration

On ZK session reconnect (after temporary disconnection):

* Bookie re-creates its ephemeral registration znode

* If the session expired and a new session was created, the old znode is already gone

### 1.4 Read-Only Transition

```
1. Delete writable registration: {root}/available/{bookieId}
2. Create read-only registration: {root}/available/readonly/{bookieId} (EPHEMERAL)
```

Reverse for recovery from read-only:

```
1. Delete read-only registration: {root}/available/readonly/{bookieId}
2. Create writable registration: {root}/available/{bookieId} (EPHEMERAL)
```

## 2. BookieServiceInfo

### 2.1 Protobuf Schema

```protobuf
message BookieServiceInfoFormat {
    message Endpoint {
        required string id = 1;          // Unique identifier (e.g., bookieId)
        required int32 port = 2;         // Service port
        required string host = 3;        // Hostname or IP
        required string protocol = 4;    // Protocol name (e.g., "bookie-rpc")
        repeated string auth = 5;        // Supported auth methods
        repeated string extensions = 6;  // Protocol extensions
    }

    repeated Endpoint endpoints = 6;
    map<string, string> properties = 7;
}
```

### 2.2 Default Construction

For a standard bookie, the service info contains:

* One endpoint with:

  * `id` = bookieId (e.g., `"hostname:3181"`)

  * `port` = bookie port

  * `host` = bookie hostname

  * `protocol` = `"bookie-rpc"`

  * `auth` = empty

  * `extensions` = empty

* `properties` = empty map

### 2.3 Serialization

Binary protobuf format, stored as the data of the registration znode.

## 3. RegistrationManager Interface

```
interface RegistrationManager:
    registerBookie(bookieId: string, readOnly: bool, serviceInfo: BookieServiceInfo)
    unregisterBookie(bookieId: string, readOnly: bool)
    
    getWritableBookies() → Set<BookieId>
    getReadOnlyBookies() → Set<BookieId>
    getAllBookies() → Set<BookieId>
    
    watchWritableBookies(watcher: RegistrationListener) → void
    watchReadOnlyBookies(watcher: RegistrationListener) → void
    
    // Cookie management
    readCookie(bookieId: string) → Versioned<byte[]>
    createCookie(bookieId: string, cookieData: byte[])
    removeCookie(bookieId: string, version: Version)
    
    // Cluster initialization
    prepareFormat() → bool
    format() → bool
    nukeExistingCluster() → bool
    
    // Instance ID
    getClusterInstanceId() → string
```

## 4. Discovery (Client Side)

### 4.1 Watching for Changes

Clients watch ZK znodes for bookie availability changes:

```
1. List children of {root}/available/ → writable bookies
2. List children of {root}/available/readonly/ → read-only bookies
3. Set ZK watchers on both paths
4. On change notification:
   a. Re-list children
   b. Compute added/removed bookies
   c. Notify EnsemblePlacementPolicy of topology change
   d. Re-set watchers
```

### 4.2 BookieId Format

A `BookieId` is a string, typically in the format `{hostname}:{port}`.

For historical reasons, some code paths use `BookieSocketAddress` which wraps a hostname and port, but the canonical form is the string representation.

### 4.3 Address Resolution

The client resolves `BookieId` strings to network addresses for TCP connections:

* Default: parse as `{host}:{port}`, resolve hostname to IP

* Custom: pluggable `BookieAddressResolver` for service mesh or DNS-based discovery

## 5. Cookie Validation (Detailed)

### 5.1 First-Time Bookie

```
1. Check ZK for cookie at {root}/cookies/{bookieId}
2. If not found:
   a. Create cookie with current config (bookieId, journalDirs, ledgerDirs, etc.)
   b. Store cookie in ZK: {root}/cookies/{bookieId} (PERSISTENT)
   c. Store cookie on disk: {journalDir}/COOKIE
   d. If instanceId exists in ZK ({root}/instanceid): include in cookie
3. Start normally
```

### 5.2 Existing Bookie

```
1. Read cookie from ZK
2. Read cookie from local disk
3. Compare fields:
   - bookieId: must match exactly
   - journalDirs: must match exactly
   - ledgerDirs: must match or be a superset (expansion allowed)
   - indexDirs: must match or be a superset
   - instanceId: must match if both non-null
4. If mismatch:
   a. Check if this is a directory expansion (ledgerDirs/indexDirs superset)
   b. If yes: update cookie in both ZK and disk
   c. If no: REFUSE TO START (prevents data corruption)
```

### 5.3 Cookie Storage Paths

| Location   | Path                        | Znode Type   |
| ---------- | --------------------------- | ------------ |
| ZooKeeper  | `{root}/cookies/{bookieId}` | PERSISTENT   |
| Local disk | `{journalDir}/COOKIE`       | Regular file |

## 6. Layout Manager

### 6.1 Purpose

The layout znode ensures all clients and bookies in a cluster agree on the metadata format (which `LedgerManager` implementation is in use).

### 6.2 Operations

```
interface LayoutManager:
    readLedgerLayout() → Optional<LedgerLayout>
    storeLedgerLayout(layout: LedgerLayout)
    deleteLedgerLayout()
```

### 6.3 LedgerLayout

Contains:

* `managerFactoryClass`: fully qualified class name (e.g., `"org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory"`)

* `managerVersion`: version number for the layout format

### 6.4 ZK Path

`{root}/LAYOUT` — PERSISTENT znode containing serialized `LedgerLayout`.

### 6.5 Validation

On client/bookie startup:

1. Read `{root}/LAYOUT`
2. If exists: verify the configured `LedgerManager` matches what's stored
3. If not exists: create with current config (first initialization)
4. Mismatch: refuse to start

## 7. Instance ID

### 7.1 Purpose

A cluster-wide UUID that uniquely identifies the BookKeeper cluster. Used in cookies to prevent a bookie from accidentally joining a different cluster.

### 7.2 ZK Path

`{root}/instanceid` — PERSISTENT znode containing a UUID string.

### 7.3 Generation

Created during cluster initialization (`initNewCluster` or `metaformat`):

```
instanceId = UUID.randomUUID().toString()
create({root}/instanceid, instanceId, PERSISTENT)
```

## 8. Cluster Initialization

### 8.1 initNewCluster

```
1. Check {root}/LAYOUT does not exist (cluster is fresh)
2. Create {root}/ (root znode)
3. Create {root}/LAYOUT with configured LedgerLayout
4. Create {root}/instanceid with new UUID
5. Create {root}/available/ (PERSISTENT)
6. Create {root}/available/readonly/ (PERSISTENT)
7. Create {root}/cookies/ (PERSISTENT)
```

### 8.2 nukeExistingCluster

**Dangerous**: Destroys all metadata.

```
1. Verify instance ID matches (safety check)
2. Delete all ledger metadata znodes
3. Delete all cookie znodes
4. Delete {root}/instanceid
5. Delete {root}/LAYOUT
```

### 8.3 metaformat

Formats (reinitializes) the metadata service:

```
1. If {root}/LAYOUT exists: delete it
2. Store new LedgerLayout
3. Optionally generate new instance ID
```

