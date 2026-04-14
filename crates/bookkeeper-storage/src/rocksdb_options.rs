//! Load RocksDB options from a Java BookKeeper-style INI file.
//!
//! Format (rocksdb's native OPTIONS file format) looks like:
//!
//! ```ini
//! [DBOptions]
//!   create_if_missing=true
//!   max_background_jobs=32
//!
//! [CFOptions "default"]
//!   compression=kLZ4Compression
//!   write_buffer_size=67108864
//!
//! [TableOptions/BlockBasedTable "default"]
//!   block_size=65536
//!   filter_policy=rocksdb.BloomFilter:10:false
//! ```
//!
//! `bookkeeper-dist` ships three such files (`default_rocksdb.conf`,
//! `entry_location_rocksdb.conf`, `ledger_metadata_rocksdb.conf`) and
//! Java's `KeyValueStorageRocksDB.initializeRocksDBWithConfFile` loads
//! them via `OptionsUtil.loadOptionsFromFile`.
//!
//! The Rust `rocksdb` crate doesn't expose `LoadOptionsFromFile`, so this
//! module parses the subset of keys Java's shipped files use and maps them
//! to the corresponding `rocksdb::Options` / `rocksdb::BlockBasedOptions`
//! setters. Unknown keys are logged at `warn` level and ignored — this
//! matches Java's `setIgnoreUnknownOptions(false)` semantics *except* that
//! we don't hard-fail, because we want to tolerate keys introduced in
//! future rocksdb releases without a code change.

use crate::StorageError;
use rocksdb::{BlockBasedOptions, Cache, ChecksumType, DBCompressionType, LogLevel, Options};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

/// Owned, parsed view of a rocksdb options file. Returned by [`load`] and
/// applied to [`Options`] + [`BlockBasedOptions`] via [`apply_to_options`].
#[derive(Debug, Default, Clone)]
pub struct RocksdbOptionsFile {
    pub db_options: HashMap<String, String>,
    pub cf_options: HashMap<String, String>,
    pub block_based_table_options: HashMap<String, String>,
}

/// Parse a rocksdb options file at `path` and return the parsed sections.
/// Does not apply anything to rocksdb structs yet — call
/// [`apply_to_options`] to build the real `Options` object.
pub fn load(path: &Path) -> Result<RocksdbOptionsFile, StorageError> {
    let text = fs::read_to_string(path).map_err(|e| {
        StorageError::RocksDb(format!("read rocksdb options file {}: {e}", path.display()))
    })?;
    parse(&text)
}

#[derive(Clone, Copy)]
enum Section {
    Db,
    Cf,
    Table,
    Ignored,
}

/// Parse from an already-read string (used by tests; callers should prefer
/// [`load`]).
pub fn parse(text: &str) -> Result<RocksdbOptionsFile, StorageError> {
    let mut result = RocksdbOptionsFile::default();
    let mut current: Option<Section> = None;

    for (lineno, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        if let Some(rest) = line.strip_prefix('[') {
            let header = rest
                .strip_suffix(']')
                .ok_or_else(|| section_error(lineno, raw))?;
            current = Some(match header {
                "DBOptions" => Section::Db,
                s if s.starts_with("CFOptions") => Section::Cf,
                s if s.starts_with("TableOptions/BlockBasedTable") => Section::Table,
                other => {
                    tracing::warn!(
                        "rocksdb options file line {}: unknown section [{}], skipping",
                        lineno + 1,
                        other
                    );
                    Section::Ignored
                }
            });
            continue;
        }

        let Some((k, v)) = line.split_once('=') else {
            return Err(StorageError::RocksDb(format!(
                "rocksdb options file line {}: expected `key = value`, got {:?}",
                lineno + 1,
                raw
            )));
        };
        let key = k.trim().to_string();
        let value = v.trim().to_string();

        match current {
            Some(Section::Db) => { result.db_options.insert(key, value); }
            Some(Section::Cf) => { result.cf_options.insert(key, value); }
            Some(Section::Table) => { result.block_based_table_options.insert(key, value); }
            Some(Section::Ignored) | None => {}
        }
    }

    Ok(result)
}

/// Build and return a fully-configured `Options` struct from the parsed
/// file. The `Cache` is returned alongside because rocksdb-rs requires the
/// Cache to outlive the DB — callers must keep it alive for the lifetime
/// of the opened `DB`.
pub fn apply_to_options(
    parsed: &RocksdbOptionsFile,
) -> Result<(Options, Option<Cache>), StorageError> {
    let mut opts = Options::default();
    let mut block_opts = BlockBasedOptions::default();
    let mut block_cache: Option<Cache> = None;

    for (k, v) in &parsed.db_options {
        match k.as_str() {
            "create_if_missing" => opts.create_if_missing(parse_bool(k, v)?),
            "info_log_level" => opts.set_log_level(parse_log_level(v)?),
            "keep_log_file_num" => opts.set_keep_log_file_num(parse_usize(k, v)?),
            "log_file_time_to_roll" => opts.set_log_file_time_to_roll(parse_usize(k, v)?),
            "max_background_jobs" => opts.set_max_background_jobs(parse_i32(k, v)?),
            "max_subcompactions" => opts.set_max_subcompactions(parse_u32(k, v)?),
            "max_total_wal_size" => opts.set_max_total_wal_size(parse_u64(k, v)?),
            "max_open_files" => opts.set_max_open_files(parse_i32(k, v)?),
            "delete_obsolete_files_period_micros" => {
                opts.set_delete_obsolete_files_period_micros(parse_u64(k, v)?)
            }
            other => {
                tracing::debug!("rocksdb options: ignoring unknown DBOption `{other}`");
            }
        }
    }

    for (k, v) in &parsed.cf_options {
        match k.as_str() {
            "compression" => opts.set_compression_type(parse_compression(v)?),
            "write_buffer_size" => opts.set_write_buffer_size(parse_usize(k, v)?),
            "max_write_buffer_number" => opts.set_max_write_buffer_number(parse_i32(k, v)?),
            "num_levels" => opts.set_num_levels(parse_i32(k, v)?),
            "level0_file_num_compaction_trigger" => {
                opts.set_level_zero_file_num_compaction_trigger(parse_i32(k, v)?)
            }
            "max_bytes_for_level_base" => opts.set_max_bytes_for_level_base(parse_u64(k, v)?),
            "target_file_size_base" => opts.set_target_file_size_base(parse_u64(k, v)?),
            "level_compaction_dynamic_level_bytes" => {
                opts.set_level_compaction_dynamic_level_bytes(parse_bool(k, v)?)
            }
            other => {
                tracing::debug!("rocksdb options: ignoring unknown CFOption `{other}`");
            }
        }
    }

    for (k, v) in &parsed.block_based_table_options {
        match k.as_str() {
            "block_size" => block_opts.set_block_size(parse_usize(k, v)?),
            "block_cache" => {
                let bytes = parse_usize(k, v)?;
                let cache = Cache::new_lru_cache(bytes);
                block_opts.set_block_cache(&cache);
                block_cache = Some(cache);
            }
            "format_version" => block_opts.set_format_version(parse_i32(k, v)?),
            "checksum" => block_opts.set_checksum_type(parse_checksum(v)?),
            "filter_policy" => {
                let (bits_per_key, block_based) = parse_filter_policy(v)?;
                block_opts.set_bloom_filter(bits_per_key, block_based);
            }
            "cache_index_and_filter_blocks" => {
                block_opts.set_cache_index_and_filter_blocks(parse_bool(k, v)?)
            }
            other => {
                tracing::debug!("rocksdb options: ignoring unknown BlockBasedTable option `{other}`");
            }
        }
    }

    opts.set_block_based_table_factory(&block_opts);
    Ok((opts, block_cache))
}

// ---------------------------------------------------------------------------
// Shipped defaults
// ---------------------------------------------------------------------------

/// Default content for `entry_location_rocksdb.conf`. Single-sourced from
/// the file shipped under `docker/rocksdb/` so a tuning change in one place
/// reaches both the in-process fallback and the on-disk default.
pub const DEFAULT_ENTRY_LOCATION_CONF: &str =
    include_str!("../../../docker/rocksdb/entry_location_rocksdb.conf");

/// Default content for `ledger_metadata_rocksdb.conf`. See
/// [`DEFAULT_ENTRY_LOCATION_CONF`] for single-sourcing rationale.
pub const DEFAULT_LEDGER_METADATA_CONF: &str =
    include_str!("../../../docker/rocksdb/ledger_metadata_rocksdb.conf");

/// Load options from `path` if provided, else parse the embedded default.
/// Typical callers want `load_or_default(cfg_path, DEFAULT_ENTRY_LOCATION_CONF)`
/// so an unset operator knob falls back to Apache BK's shipped tuning.
pub fn load_or_default(
    path: Option<&Path>,
    default: &str,
) -> Result<(Options, Option<Cache>), StorageError> {
    let parsed = match path {
        Some(p) => load(p)?,
        None => parse(default).expect("embedded default is valid"),
    };
    apply_to_options(&parsed)
}

// ---------------------------------------------------------------------------
// Value parsers
// ---------------------------------------------------------------------------

fn parse_bool(key: &str, v: &str) -> Result<bool, StorageError> {
    match v.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        other => Err(StorageError::RocksDb(format!(
            "rocksdb options: {key}: expected boolean, got {other:?}"
        ))),
    }
}

fn parse_i32(key: &str, v: &str) -> Result<i32, StorageError> {
    v.parse::<i32>().map_err(|e| {
        StorageError::RocksDb(format!("rocksdb options: {key}: expected i32, got {v:?}: {e}"))
    })
}

fn parse_u32(key: &str, v: &str) -> Result<u32, StorageError> {
    v.parse::<u32>().map_err(|e| {
        StorageError::RocksDb(format!("rocksdb options: {key}: expected u32, got {v:?}: {e}"))
    })
}

fn parse_usize(key: &str, v: &str) -> Result<usize, StorageError> {
    v.parse::<usize>().map_err(|e| {
        StorageError::RocksDb(format!("rocksdb options: {key}: expected usize, got {v:?}: {e}"))
    })
}

fn parse_u64(key: &str, v: &str) -> Result<u64, StorageError> {
    v.parse::<u64>().map_err(|e| {
        StorageError::RocksDb(format!("rocksdb options: {key}: expected u64, got {v:?}: {e}"))
    })
}

fn parse_compression(v: &str) -> Result<DBCompressionType, StorageError> {
    match v {
        "kNoCompression" => Ok(DBCompressionType::None),
        "kSnappyCompression" => Ok(DBCompressionType::Snappy),
        "kLZ4Compression" => Ok(DBCompressionType::Lz4),
        "kLZ4HCCompression" => Ok(DBCompressionType::Lz4hc),
        "kZSTD" | "kZSTDNotFinalCompression" => Ok(DBCompressionType::Zstd),
        "kZlibCompression" => Ok(DBCompressionType::Zlib),
        other => Err(StorageError::RocksDb(format!(
            "rocksdb options: unknown compression type {other:?}"
        ))),
    }
}

fn parse_checksum(v: &str) -> Result<ChecksumType, StorageError> {
    match v {
        "kNoChecksum" => Ok(ChecksumType::NoChecksum),
        "kCRC32c" => Ok(ChecksumType::CRC32c),
        "kxxHash" => Ok(ChecksumType::XXHash),
        "kxxHash64" => Ok(ChecksumType::XXHash64),
        "kXXH3" => Ok(ChecksumType::XXH3),
        other => Err(StorageError::RocksDb(format!(
            "rocksdb options: unknown checksum type {other:?}"
        ))),
    }
}

fn parse_log_level(v: &str) -> Result<LogLevel, StorageError> {
    match v {
        "DEBUG_LEVEL" => Ok(LogLevel::Debug),
        "INFO_LEVEL" => Ok(LogLevel::Info),
        "WARN_LEVEL" => Ok(LogLevel::Warn),
        "ERROR_LEVEL" => Ok(LogLevel::Error),
        "FATAL_LEVEL" => Ok(LogLevel::Fatal),
        "HEADER_LEVEL" => Ok(LogLevel::Header),
        other => Err(StorageError::RocksDb(format!(
            "rocksdb options: unknown info_log_level {other:?}"
        ))),
    }
}

/// Parse `rocksdb.BloomFilter:10:false` → `(10.0, false)`.
fn parse_filter_policy(v: &str) -> Result<(f64, bool), StorageError> {
    let mut parts = v.split(':');
    let kind = parts
        .next()
        .ok_or_else(|| StorageError::RocksDb("rocksdb options: empty filter_policy".into()))?;
    if kind != "rocksdb.BloomFilter" {
        return Err(StorageError::RocksDb(format!(
            "rocksdb options: only rocksdb.BloomFilter is supported, got {kind:?}"
        )));
    }
    let bits = parts
        .next()
        .ok_or_else(|| StorageError::RocksDb("filter_policy: missing bits_per_key".into()))?
        .parse::<f64>()
        .map_err(|e| StorageError::RocksDb(format!("filter_policy bits_per_key: {e}")))?;
    let block_based = parts
        .next()
        .map(|s| parse_bool("filter_policy use_block_based_builder", s))
        .transpose()?
        .unwrap_or(false);
    Ok((bits, block_based))
}

fn section_error(lineno: usize, raw: &str) -> StorageError {
    StorageError::RocksDb(format!(
        "rocksdb options file line {}: bad section header {:?}",
        lineno + 1,
        raw
    ))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_java_entry_location_file() {
        let parsed = parse(DEFAULT_ENTRY_LOCATION_CONF).expect("parse");
        assert_eq!(parsed.db_options.get("create_if_missing"), Some(&"true".into()));
        assert_eq!(parsed.db_options.get("max_background_jobs"), Some(&"4".into()));
        assert_eq!(parsed.cf_options.get("compression"), Some(&"kLZ4Compression".into()));
        assert_eq!(
            parsed.block_based_table_options.get("filter_policy"),
            Some(&"rocksdb.BloomFilter:10:false".into())
        );
    }

    #[test]
    fn applies_java_entry_location_without_errors() {
        let (_opts, cache) = load_or_default(None, DEFAULT_ENTRY_LOCATION_CONF).unwrap();
        assert!(cache.is_some(), "block cache should be set from the file");
    }

    #[test]
    fn applies_java_ledger_metadata_without_errors() {
        let (_opts, cache) = load_or_default(None, DEFAULT_LEDGER_METADATA_CONF).unwrap();
        assert!(
            cache.is_none(),
            "ledger_metadata_rocksdb.conf has no block_cache — cache should be None"
        );
    }

    #[test]
    fn filter_policy_parses_bits_per_key() {
        assert_eq!(
            parse_filter_policy("rocksdb.BloomFilter:10:false").unwrap(),
            (10.0, false)
        );
        assert_eq!(
            parse_filter_policy("rocksdb.BloomFilter:16.0:true").unwrap(),
            (16.0, true)
        );
    }

    #[test]
    fn compression_type_names_match_java() {
        assert!(matches!(parse_compression("kLZ4Compression"), Ok(DBCompressionType::Lz4)));
        assert!(matches!(parse_compression("kNoCompression"), Ok(DBCompressionType::None)));
        assert!(parse_compression("bogus").is_err());
    }

    #[test]
    fn ignores_comments_and_blank_lines() {
        let text = r#"
# top-level comment
[DBOptions]
   # indented comment
   create_if_missing = true

[CFOptions "default"]
"#;
        let parsed = parse(text).expect("parse");
        assert_eq!(parsed.db_options.get("create_if_missing"), Some(&"true".into()));
    }

    #[test]
    fn unknown_keys_are_ignored() {
        let text = r#"
[DBOptions]
create_if_missing=true
brand_new_rocksdb_option=42
"#;
        let parsed = parse(text).expect("parse");
        let result = apply_to_options(&parsed);
        assert!(result.is_ok());
    }
}
