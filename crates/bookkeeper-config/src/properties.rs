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
        // Java Properties uses the first '=' or ':' (whichever comes first) as separator
        let eq_pos = line.find('=');
        let colon_pos = line.find(':');
        let sep_pos = match (eq_pos, colon_pos) {
            (Some(e), Some(c)) => Some(e.min(c)),
            (Some(e), None) => Some(e),
            (None, Some(c)) => Some(c),
            (None, None) => None,
        };
        if let Some(pos) = sep_pos {
            let key = &line[..pos];
            let value = &line[pos + 1..];
            map.insert(key.trim().to_string(), value.trim().to_string());
        }
    }
    map
}
