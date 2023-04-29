#![warn(
    clippy::all,
    clippy::dbg_macro,
    clippy::todo,
    clippy::empty_enum,
    clippy::enum_glob_use,
    clippy::mem_forget,
    clippy::unused_self,
    clippy::filter_map_next,
    clippy::needless_continue,
    clippy::needless_borrow,
    clippy::match_wildcard_for_single_variants,
    clippy::if_let_mutex,
    clippy::mismatched_target_os,
    clippy::await_holding_lock,
    clippy::match_on_vec_items,
    clippy::imprecise_flops,
    clippy::suboptimal_flops,
    clippy::lossy_float_literal,
    clippy::rest_pat_in_fully_bound_structs,
    clippy::fn_params_excessive_bools,
    clippy::exit,
    clippy::inefficient_to_string,
    clippy::linkedlist,
    clippy::macro_use_imports,
    clippy::option_option,
    clippy::verbose_file_reads,
    clippy::unnested_or_patterns,
    clippy::str_to_string,
    rust_2018_idioms,
    future_incompatible,
    nonstandard_style
)]
#![deny(unreachable_pub, private_in_public)]

use std::fmt::Debug;
use std::future::Future;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use schnellru::{ByLength, LruMap};
use tokio::sync::watch;

/// Deduplication cache for requests.
pub struct DedCache<Req, Res, Err>(Arc<SharedState<Req, Res, Err>>);

impl<Req, Res, Err> Clone for DedCache<Req, Res, Err> {
    #[inline]
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<Req, Res, Err> DedCache<Req, Res, Err>
where
    Req: Hash + Eq + Clone + Debug,
    Res: Clone,
    Err: Clone,
{
    /// # Arguments
    /// * `lifetime` - The lifetime of a cached value.
    /// * `size` - The maximum number of cached values.
    pub fn new(lifetime: Duration, size: u32) -> Self {
        Self(Arc::new(SharedState::new(lifetime, size)))
    }

    /// Calling this method can cause 3 different things to happen:
    /// 1. If the key is not in the cache, `f` will be called, all other callers will wait for it to finish and then return the result.
    /// 2. If the key is in the cache and the value is older than `lifetime` then `1.` will happen.
    /// 3. If the key is in the cache and the value is newer than `lifetime` then the cached value will be returned.
    ///
    /// Cache hits are tracked and can be retrieved using `stats`.
    ///
    /// # Arguments
    /// * `key` - The key to lookup in the cache.
    /// * `f` - The future to call if the key is not in the cache or the value is older than `lifetime`.
    pub async fn get_or_update<F, Fut>(&self, key: Req, f: F) -> Result<Res, CoalesceError<Err>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Res, Err>>,
    {
        self.0.get_or_update(key, f).await
    }

    /// Returns cache performance statistics.
    pub fn fetch_stats(&self) -> Stats {
        self.0.fetch_stats()
    }
}

struct SharedState<Req, Res, Err> {
    cache: RequestLru<Req, Res, Err>,
    lifetime: Duration,
    requests_made: AtomicU64,
    calls_made: AtomicU64,
    cache_hit: AtomicU64,
}

impl<Req, Res, Err> SharedState<Req, Res, Err>
where
    Req: Hash + Eq + Clone + Debug,
    Res: Clone,
    Err: Clone,
{
    fn new(lifetime: Duration, size: u32) -> Self {
        Self {
            cache: Mutex::new(LruMap::new(ByLength::new(size))),
            lifetime,
            requests_made: Default::default(),
            cache_hit: Default::default(),
            calls_made: Default::default(),
        }
    }

    async fn get_or_update<F, Fut>(&self, key: Req, f: F) -> Result<Res, CoalesceError<Err>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Res, Err>>,
    {
        struct RemoveWatchOnDrop<'a, Req, Res, Err>
        where
            Req: Hash + Eq,
        {
            key: Option<&'a Req>,
            cache: &'a RequestLru<Req, Res, Err>,
        }

        impl<Req, Res, Err> RemoveWatchOnDrop<'_, Req, Res, Err>
        where
            Req: Hash + Eq,
        {
            fn disarm(mut self) {
                self.key = None;
            }
        }

        impl<Req, Res, Err> Drop for RemoveWatchOnDrop<'_, Req, Res, Err>
        where
            Req: Hash + Eq,
        {
            fn drop(&mut self) {
                if let Some(key) = self.key.take() {
                    self.cache.lock().remove(key);
                }
            }
        }

        enum Task {
            New,
            Existing,
        }

        self.update_request_number();

        // Determine what to do based on cache
        let (tx, task) = 'task: {
            let mut cache = self.cache.lock();

            // Check and existing entry
            if let Some(entry) = cache.get(&key) {
                let result = entry.borrow();
                if let Some(result) = &*result {
                    // Return cached response if it is still valid
                    if result.since.elapsed() <= self.lifetime {
                        self.update_cache_hit();
                        return result.data.clone().map_err(CoalesceError::Indirect);
                    }
                } else {
                    break 'task (entry.clone(), Task::Existing);
                }
            }

            // Create a new entry
            let (tx, _) = watch::channel(None);
            let tx = Arc::new(tx);
            cache.insert(key.clone(), tx.clone());
            (tx, Task::New)
        };

        // Ensure that task will be dropped when the future is dropped
        let drop_guard = RemoveWatchOnDrop {
            key: Some(&key),
            cache: &self.cache,
        };

        // Execute task
        match task {
            Task::New => {
                // Execute request
                let result = f().await;
                self.update_calls_number();

                // Force replace the data in cache (even if no watch was created)
                if result.is_ok() {
                    // Prevent watch from being removed from the cache
                    drop_guard.disarm();

                    tx.send_modify(|value| {
                        *value = Some(Entry {
                            data: result.clone(),
                            since: Instant::now(),
                        })
                    });
                }

                // Done
                result.map_err(CoalesceError::Direct)
            }
            Task::Existing => {
                let mut rx = tx.subscribe();

                // Check if notify was already resolved
                {
                    let result = rx.borrow();
                    if let Some(result) = &*result {
                        return result.data.clone().map_err(CoalesceError::Indirect);
                    }
                }

                // Wait for an existing response
                rx.changed().await.unwrap();

                let result = rx.borrow();
                let result = result.as_ref().unwrap().data.clone();

                self.update_cache_hit();
                result.map_err(CoalesceError::Indirect)
            }
        }
    }

    fn fetch_stats(&self) -> Stats {
        let (memory_usage, len) = {
            let cache = self.cache.lock();
            (cache.memory_usage(), cache.len())
        };

        let requests_made = self.requests_made.load(Ordering::Relaxed);
        let calls_made = self.calls_made.load(Ordering::Relaxed);
        let cache_hit = self.cache_hit.load(Ordering::Relaxed);

        let cache_hit_ratio = if requests_made == 0 {
            0.0
        } else {
            cache_hit as f64 / requests_made as f64
        };

        Stats {
            requests_made,
            cache_hit,
            memory_usage,
            len,
            cache_hit_ratio,
            calls_made,
        }
    }

    fn update_request_number(&self) {
        self.requests_made.fetch_add(1, Ordering::Relaxed);
    }

    fn update_calls_number(&self) {
        self.calls_made.fetch_add(1, Ordering::Relaxed);
    }

    fn update_cache_hit(&self) {
        self.cache_hit.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(thiserror::Error, Debug)]
pub enum CoalesceError<E> {
    /// our own request failed
    #[error("request failed")]
    Direct(#[source] E),

    /// request which was in progress failed, you should log it cause it will cause log spam
    #[error("inflight request failed")]
    Indirect(#[source] E),
}

impl<E> CoalesceError<E> {
    pub fn into_inner(self) -> E {
        match self {
            Self::Direct(e) => e,
            Self::Indirect(e) => e,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Stats {
    pub memory_usage: usize,
    pub len: usize,

    pub requests_made: u64,
    pub calls_made: u64,
    pub cache_hit: u64,
    pub cache_hit_ratio: f64,
}

struct Entry<T, E> {
    data: Result<T, E>,
    since: Instant,
}

type ResultTx<T, E> = watch::Sender<Option<Entry<T, E>>>;
type RequestLru<K, V, E> = Mutex<LruMap<K, Arc<ResultTx<V, E>>, ByLength>>;

#[cfg(test)]
mod test {
    use std::convert::Infallible;
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn test_cache() {
        let cache: DedCache<_, _, Infallible> = DedCache::new(Duration::from_secs(1), 1024);

        let key = "key";

        // inserting a value
        let value = cache.get_or_update(key, fut).await.unwrap();
        assert_eq!(value, "value"); // value is returned

        let start = std::time::Instant::now();

        let value = cache.get_or_update(key, fut).await.unwrap();
        // value is returned immediately
        assert_eq!(value, "value");
        assert!(start.elapsed() < Duration::from_secs(1));

        tokio::time::sleep(Duration::from_secs(2)).await;
        // at this point the value is expired

        {
            let mut cache = cache.0.cache.lock();
            let val = cache.get(&key).unwrap();
            let val = val.borrow();
            let val = val.as_ref().unwrap();

            // last update is more than 1 second ago
            assert!(val.since.elapsed() > Duration::from_secs(1));
        }

        let start = std::time::Instant::now();
        let value = cache.get_or_update(key, fut).await.unwrap();
        assert_eq!(value, "value");
        // update took more than 1 second cause it was expired
        assert!(start.elapsed() > Duration::from_secs(1));
    }

    async fn fut() -> Result<&'static str, Infallible> {
        tokio::time::sleep(Duration::from_secs(2)).await;
        Ok("value")
    }

    #[tokio::test]
    async fn test_with_eviction() {
        let cache: Arc<DedCache<i32, u32, Infallible>> =
            Arc::new(DedCache::new(Duration::from_secs(1), 2));

        // creating 3 updates so that the first one is evicted
        let key1 = 1;
        let key2 = 2;
        let key3 = 3;

        let value1 = {
            let cache = cache.clone();
            tokio::spawn(async move {
                cache
                    .get_or_update(key1, || fut2(Duration::from_secs(1), 1337))
                    .await
            })
        };
        let value2 = {
            let cache = cache.clone();
            tokio::spawn(async move {
                cache
                    .get_or_update(key2, || fut2(Duration::from_secs(0), 1337))
                    .await
            })
        };
        let value3 = {
            let cache = cache.clone();
            tokio::spawn(async move {
                cache
                    .get_or_update(key3, || fut2(Duration::from_secs(0), 1337))
                    .await
            })
        };

        let value1_second_get = {
            let cache = cache.clone();
            tokio::spawn(async move {
                cache
                    .get_or_update(key1, || fut2(Duration::from_secs(0), 1337))
                    .await
            })
        };

        // waiting for the first update to finish
        println!("Val1 = {:?}", value1.await.unwrap().unwrap());
        // waiting for the second update to finish
        println!("Val2 = {:?}", value2.await.unwrap().unwrap());
        // waiting for the third update to finish
        println!("Val3 = {:?}", value3.await.unwrap().unwrap());

        // waiting for the first update to finish
        println!("Val1 = {:?}", value1_second_get.await.unwrap().unwrap());

        let lock = cache.0.cache.lock();
        for (i, entry) in lock.iter() {
            let entry = entry.borrow();
            println!("{i}: {:?}", entry.is_some());
        }
    }

    async fn fut2(time: Duration, retval: u32) -> Result<u32, Infallible> {
        tokio::time::sleep(time).await;
        println!("fut2 finished after {time:?}");
        Ok(retval)
    }

    #[tokio::test]
    async fn test_under_load() {
        let cache: Arc<DedCache<i32, u32, Infallible>> =
            Arc::new(DedCache::new(Duration::from_secs(1), 2));

        let start = Instant::now();
        // spawning 100 task groups which will try to get the same key within group

        let mut futures_list = Vec::new();
        for key in 0..100 {
            // {
            //     let cache = cache.clone();
            //     tokio::spawn(async move {
            //         cache
            //             .get_or_update(key, || fut2(Duration::from_secs(5), key as u32))
            //             .await
            //     });
            // }

            let mut futures = Vec::new();
            for _ in 0..100 {
                let cache = cache.clone();
                let handle = tokio::spawn(async move {
                    cache
                        .get_or_update(key, || fut2(Duration::from_secs(1), key as u32))
                        .await
                });
                futures.push(handle);
            }

            futures_list.push(futures);
        }
        for (group_id, futures) in futures_list.into_iter().enumerate() {
            for future in futures {
                let res = future.await.unwrap().unwrap();
                assert_eq!(res, group_id as u32);
            }
        }

        assert!(start.elapsed() < Duration::from_secs(6));
        println!("Stats: {:?}", cache.fetch_stats());
        assert!(cache.fetch_stats().cache_hit_ratio > 0.9);
        assert_eq!(cache.fetch_stats().calls_made, 100);
    }
}
