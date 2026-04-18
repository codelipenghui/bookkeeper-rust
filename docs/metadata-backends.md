# Metadata backends

bookkeeper-rust supports two metadata backends, selected by the
`metadataServiceUri` key in `bk_server.conf` (or the equivalent
`metadata_service_uri` field in the programmatic `ServerConfig`).

## ZooKeeper (default)

```
metadataServiceUri=zk+hierarchical://host1:2181,host2:2181/ledgers
```

The older forms `zk://...` and `zk+flat://...` are also accepted for
compatibility with legacy config files.

## Oxia

```
metadataServiceUri=oxia://host:6648/namespace[/ledgers-root]
```

`host:6648` is the oxia service address. `namespace` is the oxia
namespace (commonly `default`). The optional third segment is the
ledger root path inside that namespace (default: `/ledgers`).

For example:

```
metadataServiceUri=oxia://oxia-svc:6648/default/ledgers
```

The `metadata-store:` prefix used by Apache Pulsar's broker config is
also accepted:

```
metadataServiceUri=metadata-store:oxia://oxia-svc:6648/default
```

### Pulsar interoperability

The on-wire keys produced by the oxia backend are bit-identical to
those Apache Pulsar's `pulsar-metadata/bookkeeper` driver produces
(`PulsarRegistrationManager`, `PulsarLedgerManager`,
`PulsarLedgerIdGenerator`). A bookkeeper-rust bookie can therefore
join a Pulsar+oxia cluster configured for Pulsar's driver, as long
as both use the same oxia namespace and ledger root (default
`/ledgers`).

### Feature flag

The oxia backend is gated by the `oxia` cargo feature on the
`bookkeeper-metadata` crate. The workspace enables it by default, so
ordinary builds work out of the box:

```
cargo build
```

To build without the oxia backend (ZooKeeper only):

```
cargo build --no-default-features
```

Note: disabling the `oxia` feature only drops the oxia backend. All
other functionality is unaffected.

### Session semantics

Bookie registrations under oxia are ephemeral, tied to the bookie's
oxia session. If the bookie process dies the session expires and the
registration is automatically removed. Default session timeout is 30 s.
The bookie re-registers its ephemeral node whenever the oxia session is
re-established (e.g. after a transient network partition).

### Verifying Pulsar interop

`tests/interop/pulsar_oxia.sh` is a manual runbook that boots a local
oxia + Apache Pulsar broker + bookkeeper-rust bookie, creates a ledger
through Pulsar's managed-ledger client, and lists it via the Rust
bookie. Run it before shipping a significant change to the oxia backend.
The script is NOT run in CI because it requires a Pulsar image and
takes several minutes.
