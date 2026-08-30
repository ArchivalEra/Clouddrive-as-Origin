use std::{collections::HashMap, sync::Arc};
use tokio::sync::{OnceCell, Mutex};

/// Per-key single-flight: concurrent callers for the same key coalesce to
/// one execution (spec §3.2). Different keys do not block each other.
/// Both success and error are cached in the cell so concurrent waiters
/// share the same outcome; the cell is removed after the first batch
/// completes so later calls re-execute.
pub struct Inflight<V: Clone + Send + Sync + 'static, E: Clone + Send + Sync + 'static> {
    cells: Mutex<HashMap<String, Arc<OnceCell<Result<V, E>>>>>,
}

impl<V: Clone + Send + Sync + 'static, E: Clone + Send + Sync + 'static> Default for Inflight<V, E> {
    fn default() -> Self {
        Self { cells: Mutex::new(HashMap::new()) }
    }
}

impl<V: Clone + Send + Sync + 'static, E: Clone + Send + Sync + 'static> Inflight<V, E> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Run `f` once per `key` — concurrent callers await the same result.
    /// The result (Ok or Err) is shared; the cell is removed after completion
    /// so a later call re-executes.
    pub async fn run<F, Fut>(&self, key: String, f: F) -> Result<V, E>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<V, E>> + Send,
    {
        let cell = {
            let mut guard = self.cells.lock().await;
            Arc::clone(guard.entry(key.clone()).or_insert_with(|| Arc::new(OnceCell::new())))
        };
        let res = cell.get_or_init(|| f()).await.clone();
        {
            let mut guard = self.cells.lock().await;
            if let Some(c) = guard.get(&key) {
                if Arc::ptr_eq(c, &cell) {
                    guard.remove(&key);
                }
            }
        }
        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn coalesces_concurrent_same_key() {
        let inflight: Arc<Inflight<usize, String>> = Arc::new(Inflight::new());
        let counter = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..20 {
            let inf = Arc::clone(&inflight);
            let ctr = Arc::clone(&counter);
            handles.push(tokio::spawn(async move {
                inf.run("same-key".into(), || {
                    let ctr = Arc::clone(&ctr);
                    async move {
                        ctr.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                        Ok::<usize, String>(42)
                    }
                })
                .await
                .unwrap()
            }));
        }
        for h in handles {
            assert_eq!(h.await.unwrap(), 42);
        }
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn coalesces_error_too() {
        let inflight: Arc<Inflight<usize, String>> = Arc::new(Inflight::new());
        let counter = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..10 {
            let inf = Arc::clone(&inflight);
            let ctr = Arc::clone(&counter);
            handles.push(tokio::spawn(async move {
                inf.run("err-key".into(), || {
                    let ctr = Arc::clone(&ctr);
                    async move {
                        ctr.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                        Err::<usize, String>("boom".into())
                    }
                })
                .await
                .unwrap_err()
            }));
        }
        for h in handles {
            assert_eq!(h.await.unwrap(), "boom");
        }
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn different_keys_do_not_block() {
        let inflight = Arc::new(Inflight::<String, String>::new());
        let a = {
            let inf = Arc::clone(&inflight);
            tokio::spawn(async move {
                inf.run("key-a".into(), || async { Ok::<String, String>("a".into()) }).await.unwrap()
            })
        };
        let b = {
            let inf = Arc::clone(&inflight);
            tokio::spawn(async move {
                inf.run("key-b".into(), || async { Ok::<String, String>("b".into()) }).await.unwrap()
            })
        };
        assert_eq!(a.await.unwrap(), "a");
        assert_eq!(b.await.unwrap(), "b");
    }

    #[tokio::test]
    async fn second_call_after_first_re_executes() {
        let inflight = Inflight::<u32, String>::new();
        let c = Arc::new(AtomicUsize::new(0));
        let c1 = Arc::clone(&c);
        inflight
            .run("k".into(), || {
                let c1 = Arc::clone(&c1);
                async move {
                    c1.fetch_add(1, Ordering::SeqCst);
                    Ok::<u32, String>(1)
                }
            })
            .await
            .unwrap();
        let c2 = Arc::clone(&c);
        inflight
            .run("k".into(), || {
                let c2 = Arc::clone(&c2);
                async move {
                    c2.fetch_add(1, Ordering::SeqCst);
                    Ok::<u32, String>(2)
                }
            })
            .await
            .unwrap();
        assert_eq!(c.load(Ordering::SeqCst), 2);
    }
}
