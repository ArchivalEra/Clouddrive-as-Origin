use std::{collections::HashMap, sync::Arc};
use tokio::sync::{OnceCell, Semaphore};

/// Per-upstream token + concurrency state.
#[derive(Debug)]
pub struct UpstreamState {
    pub id: String,
    pub drive_root_path: String,
    pub client_id_env: String,
    pub client_secret_env: String,
    pub refresh_token_env: String,
    pub semaphore: Arc<Semaphore>,
    /// Single-flight for reactive 401 refresh per upstream.
    pub refresh_cell: OnceCell<String>,
    /// Last refresh error surfaced in healthz.
    pub needs_reauth: bool,
}

impl UpstreamState {
    pub fn new(
        id: String,
        drive_root_path: String,
        client_id_env: String,
        client_secret_env: String,
        refresh_token_env: String,
        concurrency: usize,
    ) -> Self {
        Self {
            id,
            drive_root_path,
            client_id_env,
            client_secret_env,
            refresh_token_env,
            semaphore: Arc::new(Semaphore::new(concurrency)),
            refresh_cell: OnceCell::new(),
            needs_reauth: false,
        }
    }

    pub fn client_id(&self) -> Option<String> {
        std::env::var(&self.client_id_env).ok()
    }
}

/// Registry of all upstreams, indexed by id.
#[derive(Debug, Default)]
pub struct UpstreamRegistry {
    map: HashMap<String, Arc<UpstreamState>>,
}

impl UpstreamRegistry {
    pub fn new(states: Vec<UpstreamState>) -> Self {
        let map = states.into_iter().map(|s| (s.id.clone(), Arc::new(s))).collect();
        Self { map }
    }

    pub fn get(&self, id: &str) -> Option<Arc<UpstreamState>> {
        self.map.get(id).cloned()
    }

    pub fn ids(&self) -> Vec<String> {
        self.map.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_resolves() {
        let r = UpstreamRegistry::new(vec![UpstreamState::new(
            "primary".into(),
            "/drive/root:/assets".into(),
            "A_ID".into(),
            "A_SECRET".into(),
            "A_TOKEN".into(),
            3,
        )]);
        assert!(r.get("primary").is_some());
        assert!(r.get("missing").is_none());
    }
}
