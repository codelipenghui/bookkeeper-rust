use std::collections::HashMap;

/// Parse Java Properties format: key=value or key:value lines, # comments, blank lines ignored.
/// Matches Java's `java.util.Properties.load()` behavior for the common cases.
pub fn parse_properties(text: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with('!') {
            continue;
        }
        // Java Properties accepts both '=' and ':' as separators
        let split = line.split_once('=').or_else(|| line.split_once(':'));
        if let Some((key, value)) = split {
            map.insert(key.trim().to_string(), value.trim().to_string());
        }
    }
    map
}
