use bookkeeper_metadata::backend::MetadataBackend;
use bookkeeper_metadata::ledger_metadata::MetadataError;
use bookkeeper_metadata::service_info::BookieServiceInfo;
use bookkeeper_metadata::traits::LedgerLayout;

use super::fixtures::fake_ledger_metadata;

/// Run the full contract suite against `backend`. Panics on any failure.
pub async fn run_all(backend: &MetadataBackend) {
    layout_roundtrip(backend).await;
    ledger_crud(backend).await;
    ledger_cas_conflict_on_stale_write(backend).await;
    list_ledger_ids(backend).await;
    id_generator_monotonic(backend).await;
    cookie_crud(backend).await;
}

async fn layout_roundtrip(backend: &MetadataBackend) {
    let layout = LedgerLayout {
        manager_factory_class:
            "org.apache.bookkeeper.meta.LegacyHierarchicalLedgerManagerFactory".into(),
        manager_version: 1,
    };
    // Delete any pre-existing layout so the store assertion works.
    let _ = backend.layout_manager.delete_ledger_layout().await;
    backend
        .layout_manager
        .store_ledger_layout(&layout)
        .await
        .expect("layout_roundtrip: store");
    let got = backend
        .layout_manager
        .read_ledger_layout()
        .await
        .expect("layout_roundtrip: read")
        .expect("layout_roundtrip: expected Some");
    assert_eq!(
        got.manager_factory_class, layout.manager_factory_class,
        "layout_roundtrip: factory class mismatch"
    );
    assert_eq!(
        got.manager_version, layout.manager_version,
        "layout_roundtrip: manager version mismatch"
    );
    backend
        .layout_manager
        .delete_ledger_layout()
        .await
        .expect("layout_roundtrip: delete");
    assert!(
        backend
            .layout_manager
            .read_ledger_layout()
            .await
            .expect("layout_roundtrip: read after delete")
            .is_none(),
        "layout_roundtrip: expected None after delete"
    );
}

async fn ledger_crud(backend: &MetadataBackend) {
    let md = fake_ledger_metadata();
    let created = backend
        .ledger_manager
        .create_ledger_metadata(42, &md)
        .await
        .expect("ledger_crud: create");
    let read = backend
        .ledger_manager
        .read_ledger_metadata(42)
        .await
        .expect("ledger_crud: read");
    assert_eq!(
        read.version(),
        created.version(),
        "ledger_crud: version mismatch after create"
    );
    backend
        .ledger_manager
        .remove_ledger_metadata(42, read.version())
        .await
        .expect("ledger_crud: remove");
    assert!(
        matches!(
            backend.ledger_manager.read_ledger_metadata(42).await,
            Err(MetadataError::NotFound(42))
        ),
        "ledger_crud: expected NotFound after remove"
    );
}

async fn ledger_cas_conflict_on_stale_write(backend: &MetadataBackend) {
    let md = fake_ledger_metadata();
    let created = backend
        .ledger_manager
        .create_ledger_metadata(99, &md)
        .await
        .expect("cas_conflict: create");
    let bumped = backend
        .ledger_manager
        .write_ledger_metadata(99, &md, created.version())
        .await
        .expect("cas_conflict: write 1");
    let err = backend
        .ledger_manager
        .write_ledger_metadata(99, &md, created.version())
        .await
        .expect_err("cas_conflict: stale write should fail");
    assert!(
        matches!(err, MetadataError::VersionConflict { .. }),
        "cas_conflict: expected VersionConflict, got {err:?}"
    );
    backend
        .ledger_manager
        .remove_ledger_metadata(99, bumped.version())
        .await
        .ok();
}

async fn list_ledger_ids(backend: &MetadataBackend) {
    // Include IDs that exercise multi-segment path reconstruction:
    //   1, 7, 13    — small IDs (single path prefix digit group)
    //   12345       — crosses the 4-digit boundary (path: XX/XXXX/L{NNNN})
    //   987654321   — large ID near i32::MAX (also short hierarchy)
    let want: Vec<i64> = vec![1, 7, 13, 12345, 987654321];
    let md = fake_ledger_metadata();
    for &id in &want {
        backend
            .ledger_manager
            .create_ledger_metadata(id, &md)
            .await
            .expect("list_ledger_ids: create");
    }
    let mut ids = backend
        .ledger_manager
        .get_ledger_ranges()
        .await
        .expect("list_ledger_ids: get_ledger_ranges");
    ids.retain(|id| want.contains(id));
    ids.sort_unstable();
    assert_eq!(ids, want, "list_ledger_ids: unexpected IDs returned");
    for id in &want {
        let v = backend
            .ledger_manager
            .read_ledger_metadata(*id)
            .await
            .expect("list_ledger_ids: read")
            .version();
        backend
            .ledger_manager
            .remove_ledger_metadata(*id, v)
            .await
            .ok();
    }
}

async fn id_generator_monotonic(backend: &MetadataBackend) {
    let a = backend
        .id_generator
        .generate_ledger_id()
        .await
        .expect("id_generator: gen 1");
    let b = backend
        .id_generator
        .generate_ledger_id()
        .await
        .expect("id_generator: gen 2");
    let c = backend
        .id_generator
        .generate_ledger_id()
        .await
        .expect("id_generator: gen 3");
    assert!(
        a < b && b < c,
        "id_generator_monotonic: IDs must be monotonically increasing: {a} < {b} < {c}"
    );
}

async fn cookie_crud(backend: &MetadataBackend) {
    let bookie = "bookie-test:3181";
    let data = b"cookie-v1".to_vec();
    backend
        .registration_manager
        .create_cookie(bookie, &data)
        .await
        .expect("cookie_crud: create");
    let (read_data, v) = backend
        .registration_manager
        .read_cookie(bookie)
        .await
        .expect("cookie_crud: read")
        .expect("cookie_crud: expected Some after create");
    assert_eq!(read_data, data, "cookie_crud: data mismatch after create");
    let updated = b"cookie-v2".to_vec();
    backend
        .registration_manager
        .update_cookie(bookie, &updated, v)
        .await
        .expect("cookie_crud: update");
    let (read2, v2) = backend
        .registration_manager
        .read_cookie(bookie)
        .await
        .expect("cookie_crud: read after update")
        .expect("cookie_crud: expected Some after update");
    assert_eq!(read2, updated, "cookie_crud: data mismatch after update");
    assert_ne!(v, v2, "cookie_crud: version should change after update");
    backend
        .registration_manager
        .delete_cookie(bookie, v2)
        .await
        .expect("cookie_crud: delete");
    assert!(
        backend
            .registration_manager
            .read_cookie(bookie)
            .await
            .expect("cookie_crud: read after delete")
            .is_none(),
        "cookie_crud: expected None after delete"
    );
}

// `BookieServiceInfo` import kept so future tests using it don't have
// to re-add; remove if clippy flags it unused.
#[allow(dead_code)]
fn _keep_service_info_import(_info: BookieServiceInfo) {}
