# Sub-Spec 10: CLI Tools

**Target**: Shell commands for administration, debugging, and operations.

## 1. Overview

BookKeeper provides a CLI tool (`bookkeeper shell` or `bkctl`) for cluster administration. The CLI connects to the metadata service and/or individual bookies to perform operations.

## 2. Cluster Initialization

### initnewcluster

Initialize a fresh BookKeeper cluster.

```
bookkeeper shell initnewcluster
```

**Behavior**:
1. Create ZK root path structure
2. Create LAYOUT znode
3. Generate cluster instance ID
4. Create registration parent znodes

### metaformat

Format (reinitialize) the metadata service.

```
bookkeeper shell metaformat [-nonInteractive] [-force]
```

**Behavior**:
1. Delete existing LAYOUT if present
2. Store new LedgerLayout
3. Optionally regenerate instance ID

### bookieformat

Format bookie data directories.

```
bookkeeper shell bookieformat [-nonInteractive] [-force] [-deleteCookie]
```

**Behavior**:
1. Clean journal directories
2. Clean ledger directories
3. Clean index directories
4. Optionally remove cookie from ZK

### nukeexistingcluster

**Dangerous**: Destroy all cluster metadata.

```
bookkeeper shell nukeexistingcluster -zkledgersrootpath {path} -instanceid {id} [-force]
```

**Behavior**:
1. Verify instance ID matches (safety)
2. Delete all ledger metadata
3. Delete all cookies
4. Delete instance ID and LAYOUT

## 3. Bookie Operations

### listbookies

List registered bookies.

```
bookkeeper shell listbookies [-rw] [-ro] [-a]
```

**Flags**:
- `-rw`: List writable bookies
- `-ro`: List read-only bookies
- `-a`: List all (both)

**Output**: One bookie ID per line

### bookieinfo

Show bookie disk information.

```
bookkeeper shell bookieinfo
```

**Output**: Free space and total space for each bookie

### bookiesanity

Sanity test: create a ledger, write, read, verify, delete.

```
bookkeeper shell bookiesanity [-entries N] [-timeout T]
```

**Behavior**:
1. Create a test ledger
2. Write N entries (default 10)
3. Close ledger
4. Open and read all entries
5. Verify data integrity
6. Delete ledger
7. Report success/failure

### decommissionbookie

Safely remove a bookie from the cluster.

```
bookkeeper shell decommissionbookie [-bookieid {id}]
```

**Behavior**:
1. Find all ledgers on the bookie
2. Mark each as underreplicated
3. Wait for replication to complete
4. Verify no ledgers remain on the bookie

## 4. Ledger Operations

### listledgers

List all ledgers.

```
bookkeeper shell listledgers [-meta] [-bookieid {id}]
```

**Flags**:
- `-meta`: Print metadata for each ledger
- `-bookieid`: Only list ledgers on a specific bookie

### ledgermetadata

Dump metadata for a specific ledger.

```
bookkeeper shell ledgermetadata -ledgerid {id}
```

**Output**: Full ledger metadata in human-readable format

### readledger

Read and print entries from a ledger.

```
bookkeeper shell readledger -ledgerid {id} [-firstEntry {n}] [-lastEntry {m}]
```

**Behavior**:
1. Open ledger read-only
2. Read entries in range [firstEntry, lastEntry]
3. Print entry contents (hex dump or text)

### deleteledger

Delete a ledger.

```
bookkeeper shell deleteledger -ledgerid {id} [-force]
```

**Behavior**:
1. Read metadata (verify exists)
2. Delete metadata from ZK
3. Entries remain on disk until GC

### recover

Manually trigger recovery for a bookie.

```
bookkeeper shell recover [-bookieid {id}] [-force] [-deleteCookie]
```

**Behavior**:
1. Find all ledgers on the specified bookie
2. For each ledger: open in recovery mode, replicate, close
3. Optionally delete the bookie's cookie

## 5. Debug Tools

### readjournal

Dump journal file contents.

```
bookkeeper shell readjournal [-dir {journalDir}] [-id {journalId}]
```

**Output**: For each entry in the journal:
- Entry size, ledger ID, entry ID
- Optionally: full entry data

### readlog

Dump entry log file contents.

```
bookkeeper shell readlog [-entrylog {id}] [-ledgerid {id}] [-startpos {offset}]
```

**Output**: For each entry in the log:
- Ledger ID, entry ID, size, offset

### localconsistencycheck

Verify local storage consistency.

```
bookkeeper shell localconsistencycheck
```

**Behavior**:
1. For each entry in the location index:
   - Verify the entry log file exists
   - Verify the offset is valid
   - Optionally verify entry data integrity
2. Report any inconsistencies

### triggeraudit

Force the auditor to run an immediate check.

```
bookkeeper shell triggeraudit
```

**Behavior**: Sends a signal to the auditor to run a bookie check immediately.

### listunderreplicated

List underreplicated ledgers.

```
bookkeeper shell listunderreplicated [-missingreplica {bookieId}] [-excludingmissingreplica {bookieId}]
```

**Output**: Ledger IDs and their underreplication details

### whatisinstanceid

Print the cluster instance ID.

```
bookkeeper shell whatisinstanceid
```

## 6. Implementation Priority

This sub-spec is the lowest priority. For a Rust reimplementation:

1. **Essential** (for operations):
   - `initnewcluster`
   - `bookieformat`
   - `listbookies`
   - `bookiesanity`

2. **Important** (for debugging):
   - `listledgers`
   - `ledgermetadata`
   - `readledger`
   - `readjournal`
   - `readlog`
   - `localconsistencycheck`

3. **Nice to have** (for admin):
   - `decommissionbookie`
   - `recover`
   - `listunderreplicated`
   - `triggeraudit`
   - `deleteledger`
   - `metaformat`
   - `nukeexistingcluster`
