/// Build the (start, end) key bounds for listing immediate children
/// of `parent`. Matches the convention in Pulsar's
/// `OxiaMetadataStore.getChildrenFromStore`: `{parent}/` to
/// `{parent}/\u{00ff}`. We use a high-codepoint upper bound to avoid
/// false ends on keys containing `/`.
pub fn child_range(parent: &str) -> (String, String) {
    let mut start = parent.to_string();
    if !start.ends_with('/') {
        start.push('/');
    }
    let mut end = start.clone();
    end.push('\u{00ff}');
    (start, end)
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
}
