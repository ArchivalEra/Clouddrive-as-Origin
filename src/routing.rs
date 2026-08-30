use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct RouteRule {
    pub prefix: String,
    pub upstream: String,
}

/// Longest-prefix-first route table.
/// Owns a sorted vec; empty prefix `""` is the default (catch-all).
#[derive(Debug, Clone)]
pub struct RouteTable {
    rules: Vec<RouteRule>,
}

impl RouteTable {
    pub fn new(mut rules: Vec<RouteRule>) -> Self {
        rules.sort_by(|a, b| b.prefix.len().cmp(&a.prefix.len()));
        Self { rules }
    }

    /// Resolve a validated key to an upstream id.
    /// Panics if the table is empty (config validation ensures a default route).
    pub fn resolve(&self, key: &str) -> &str {
        for r in &self.rules {
            if r.prefix.is_empty() || key.starts_with(&r.prefix) {
                return &r.upstream;
            }
        }
        &self.rules[0].upstream
    }

    pub fn rules(&self) -> &[RouteRule] {
        &self.rules
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table() -> RouteTable {
        RouteTable::new(vec![
            RouteRule { prefix: "".into(), upstream: "primary".into() },
            RouteRule { prefix: "archive/".into(), upstream: "archive".into() },
            RouteRule { prefix: "archive/deep/".into(), upstream: "deep".into() },
        ])
    }

    #[test]
    fn longest_prefix_wins() {
        let t = table();
        assert_eq!(t.resolve("archive/deep/file.png"), "deep");
        assert_eq!(t.resolve("archive/other.png"), "archive");
        assert_eq!(t.resolve("2026/08/a.png"), "primary");
    }

    #[test]
    fn trailing_slash_isolation() {
        let t = table();
        assert_eq!(t.resolve("archive2/file.png"), "primary");
        assert_eq!(t.resolve("archive"), "primary");
    }

    #[test]
    fn sorted_on_build() {
        let t = table();
        assert_eq!(t.rules[0].prefix, "archive/deep/");
    }
}
