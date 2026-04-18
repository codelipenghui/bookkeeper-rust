/// Build the (start, end) key bounds for listing **immediate children**
/// of `parent`. This uses a sentinel byte (`\u{ff}`) as the terminal
/// character of the leaf segment. It works for direct children because
/// the final path segment contains no `/`, so the comparison degrades
/// to a plain byte comparison where `\u{ff}` is the maximum byte.
///
/// This does NOT work for listing all descendants of a path — see
/// `descendant_range` for that.
pub fn child_range(parent: &str) -> (String, String) {
    let mut start = parent.to_string();
    if !start.ends_with('/') {
        start.push('/');
    }
    let mut end = start.clone();
    end.push('\u{00ff}');
    (start, end)
}

/// Build the (start, end) key bounds for listing **all descendants**
/// (children, grandchildren, …) of `parent` under oxia's segment-wise
/// key-ordering (CompareWithSlash).
///
/// Oxia's CompareWithSlash comparison splits keys on `/` and compares
/// segment by segment. A key `P/A/B/C` is considered "greater than"
/// `P/Z` (no trailing slash) when `A < Z` in the final segment but the
/// key has more path components — making `child_range` unusable for
/// deep trees.
///
/// The correct upper bound is produced by incrementing the last byte of
/// `parent` (so the second path segment changes from, e.g., `ledgers`
/// to `ledgert`) and appending `/`. This ensures every `/parent/...`
/// key sorts before the upper bound in segment-wise comparison.
pub fn descendant_range(parent: &str) -> (String, String) {
    let start = format!("{parent}/");
    let end = match increment_last_byte(parent) {
        Some(e) => format!("{e}/"),
        // If parent is all-\xff bytes, use an empty end (open range).
        // In practice this path never occurs for BK ledger roots.
        None => String::new(),
    };
    (start, end)
}

/// Increment the last byte of `s` that is less than `0xff`, returning
/// `None` if all bytes are `0xff` (overflow).
fn increment_last_byte(s: &str) -> Option<String> {
    let mut bytes = s.as_bytes().to_vec();
    for i in (0..bytes.len()).rev() {
        if bytes[i] < 0xff {
            bytes[i] += 1;
            bytes.truncate(i + 1);
            return String::from_utf8(bytes).ok();
        }
    }
    None
}

/// Split a full key into (parent, leaf) on the last '/'. Returns
/// `None` when the key has no parent (top-level / single segment).
pub fn split_parent(key: &str) -> Option<(&str, &str)> {
    let idx = key.rfind('/')?;
    Some((&key[..idx], &key[idx + 1..]))
}

/// Given an absolute key, yield every non-empty ancestor in order
/// root-first, excluding the key itself.
pub fn ancestors_of(key: &str) -> Vec<String> {
    let mut acc = Vec::new();
    let trimmed = key.trim_start_matches('/');
    let mut current = String::from("/");
    let parts: Vec<&str> = trimmed.split('/').collect();
    for part in &parts[..parts.len().saturating_sub(1)] {
        if !current.ends_with('/') {
            current.push('/');
        }
        current.push_str(part);
        acc.push(current.clone());
    }
    acc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn child_range_appends_slash_and_sentinel() {
        let (lo, hi) = child_range("/ledgers/cookies");
        assert_eq!(lo, "/ledgers/cookies/");
        assert_eq!(hi, "/ledgers/cookies/\u{00ff}");
    }

    #[test]
    fn child_range_preserves_trailing_slash() {
        let (lo, hi) = child_range("/ledgers/");
        assert_eq!(lo, "/ledgers/");
        assert_eq!(hi, "/ledgers/\u{00ff}");
    }

    #[test]
    fn ancestors_of_full_ledger_path() {
        let got = ancestors_of("/ledgers/00/0001/L2345");
        assert_eq!(got, vec![
            "/ledgers".to_string(),
            "/ledgers/00".to_string(),
            "/ledgers/00/0001".to_string(),
        ]);
    }

    #[test]
    fn ancestors_of_top_level_is_empty() {
        assert!(ancestors_of("/ledgers").is_empty());
    }

    #[test]
    fn split_parent_splits_on_last_slash() {
        assert_eq!(split_parent("/a/b/c"), Some(("/a/b", "c")));
        assert_eq!(split_parent("/a"), Some(("", "a")));
        assert_eq!(split_parent("a"), None);
    }

    #[test]
    fn descendant_range_increments_last_byte() {
        let (lo, hi) = descendant_range("/ledgers");
        assert_eq!(lo, "/ledgers/");
        assert_eq!(hi, "/ledgert/");
    }

    #[test]
    fn descendant_range_nested_path() {
        let (lo, hi) = descendant_range("/ledgers/available");
        assert_eq!(lo, "/ledgers/available/");
        assert_eq!(hi, "/ledgers/availablf/");
    }
}
