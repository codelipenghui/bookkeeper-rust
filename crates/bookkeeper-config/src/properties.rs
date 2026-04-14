use std::collections::HashMap;

/// Environment-variable prefix used to override any config property.
/// Matches the convention baked into Apache BookKeeper's Docker images:
/// `BK_<propertyName>=<value>` (e.g. `BK_journalDirectory=/tmp/journal`)
/// gets applied on top of the properties file so container platforms can
/// inject secrets/endpoints without rewriting the file. The Rust port
/// does the override in-process rather than via a shell script so it
/// works in any deployment mode, not just the official image.
pub const ENV_PREFIX: &str = "BK_";

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

/// Overlay any `BK_<propertyName>` environment variables onto the parsed
/// properties map. Called after `parse_properties` — env values win over
/// file values so operators can override any setting without touching
/// the config file. Matches Java's Docker `apply-config-from-env.py`.
///
/// Example: `BK_bookiePort=3281` → map.insert("bookiePort", "3281").
pub fn apply_env_overrides(map: &mut HashMap<String, String>) {
    apply_env_overrides_from(map, std::env::vars());
}

/// Testable variant of [`apply_env_overrides`] accepting any iterator of
/// `(name, value)` pairs.
pub fn apply_env_overrides_from<I>(map: &mut HashMap<String, String>, vars: I)
where
    I: IntoIterator<Item = (String, String)>,
{
    for (name, value) in vars {
        if let Some(key) = name.strip_prefix(ENV_PREFIX) {
            if key.is_empty() {
                continue;
            }
            map.insert(key.to_string(), value);
        }
    }
}

/// Parse a boolean property the way Java does (via Apache Commons
/// Configuration's `BooleanUtils.toBoolean`): accepts `true/false`,
/// `yes/no`, `y/n`, `on/off`, and `1/0` — case-insensitive. Unknown
/// strings default to `false`, matching Commons' behavior when no
/// default is registered for the token.
///
/// Previously the Rust port only recognised `true` / `True` etc., which
/// broke portability of Java-shaped config files that use `yes` / `on`.
pub fn parse_bool(raw: &str) -> bool {
    let s = raw.trim();
    matches!(
        s.to_ascii_lowercase().as_str(),
        "true" | "yes" | "y" | "on" | "1"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_bool_accepts_java_forms() {
        for t in ["true", "TRUE", "True", "yes", "YES", "y", "on", "ON", "1"] {
            assert!(parse_bool(t), "expected {t:?} to parse as true");
        }
        for f in ["false", "no", "n", "off", "0", "", "garbage"] {
            assert!(!parse_bool(f), "expected {f:?} to parse as false");
        }
    }

    #[test]
    fn env_override_replaces_file_value() {
        let mut map = HashMap::new();
        map.insert("bookiePort".into(), "3181".into());
        apply_env_overrides_from(&mut map, vec![
            ("BK_bookiePort".into(), "3281".into()),
            ("BK_journalDirectory".into(), "/mnt/journal".into()),
            ("UNRELATED".into(), "ignored".into()),
            ("BK_".into(), "empty-prefix-ignored".into()),
        ]);
        assert_eq!(map.get("bookiePort"), Some(&"3281".into()));
        assert_eq!(map.get("journalDirectory"), Some(&"/mnt/journal".into()));
        assert!(map.get("UNRELATED").is_none());
        assert!(map.get("").is_none());
    }
}
