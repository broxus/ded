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
    nonstandard_style,
    missing_debug_implementations
)]
#![deny(unreachable_pub, private_in_public)]
#![allow(elided_lifetimes_in_paths, clippy::type_complexity)]

use std::fmt::Debug;
use std::future::Future;
use std::hash::Hash;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use schnellru::{ByLength, Limiter, LruMap};
use tokio::sync::Notify;

/// Shared cache using LRU eviction policy.
#[allow(missing_debug_implementations)]
#[derive(Clone)]
pub struct LCache<K, V, L = ByLength>
where
    L: Limiter<K, V>,
{
    cache: Arc<Mutex<LruMap<K, V, L>>>,
}

impl<K, V> LCache<K, V>
where
    K: Hash + Eq,
    V: Clone,
{
    pub fn new(size: usize) -> Self {
        LCache {
            cache: Arc::new(Mutex::new(LruMap::new(ByLength::new(size as u32)))),
        }
    }
}

impl<K, V, L> LCache<K, V, L>
where
    K: Hash + Eq,
    V: Clone,
    L: Limiter<K, V>,
{
    /// Returns a copy of the value for a given key and promotes that element to be the most recently used.
    pub fn get(&self, key: &K) -> Option<V> {
        self.cache.lock().get(key).cloned()
    }

    /// Inserts a new element into the map.
    /// Can fail if the element is rejected by the limiter or if we fail to grow an empty map.
    /// Returns true if the element was inserted; false otherwise.
    pub fn insert<'a>(&self, key: L::KeyToInsert<'a>, value: V) -> bool
    where
        L::KeyToInsert<'a>: Hash + PartialEq<K>,
    {
        self.cache.lock().insert(key, value)
    }

    pub fn stats(&self) -> LruStats {
        let lock = self.cache.lock();

        let memory_usage = lock.memory_usage();
        let len = lock.len();

        LruStats { len, memory_usage }
    }
}

#[derive(Clone, Debug)]
pub struct LruStats {
    pub len: usize,
    pub memory_usage: usize,
}

#[allow(missing_debug_implementations)]
pub struct Cache<Req, Res, Err> {
    cache: LCache<Req, Entry<Res, Err>, ByLength>,
    lifetime: Duration,
    requests_made: AtomicU64,
    cache_hit: AtomicU64,
}

impl<Req, Res, Err> Cache<Req, Res, Err>
where
    Req: Hash + Eq + Clone + Debug,
    Res: Clone,
    Err: Clone,
{
    /// # Arguments
    /// * `lifetime` - The lifetime of a cached value.
    /// * `size` - The maximum number of cached values.
    pub fn new(lifetime: Duration, size: usize) -> Self {
        Cache {
            cache: LCache::new(size),
            lifetime,
            requests_made: Default::default(),
            cache_hit: Default::default(),
        }
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
        self.update_request_number();

        if let Some(entry) = self.get(&key) {
            match entry {
                Entry::Data {
                    response,
                    last_update,
                } => {
                    // Return valid response
                    if last_update.elapsed() <= self.lifetime {
                        self.update_cache_hit();
                        return Ok(response);
                    }
                }
                Entry::UpdateInProgress(notify) => {
                    // Waiting for an existing response
                    return notify.wait().await.map_err(CoalesceError::Indirect);
                }
            }
        }

        // Initiate request otherwise
        self.update_data(key, f).await
    }

    /// Returns cache performance statistics.
    pub fn fetch_stats(&self) -> Stats {
        let requests_made = self.requests_made.load(Ordering::Relaxed);
        let cache_hit = self.cache_hit.load(Ordering::Relaxed);
        let (memory_usage, len) = {
            let lock = self.cache.cache.lock();
            (lock.memory_usage(), lock.len())
        };
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
        }
    }

    async fn update_data<F, Fut>(&self, key: Req, f: F) -> Result<Res, CoalesceError<Err>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Res, Err>>,
    {
        let notify = self.insert_notify(key.clone());

        match f().await {
            Ok(response) => {
                self.insert_response(key, response.clone());
                notify.notify_ok(response.clone());

                Ok(response)
            }
            Err(e) => {
                notify.notify_err(e.clone());
                Err(CoalesceError::Direct(e))
            }
        }
    }

    fn update_request_number(&self) {
        self.requests_made.fetch_add(1, Ordering::Relaxed);
    }

    fn update_cache_hit(&self) {
        self.cache_hit.fetch_add(1, Ordering::Relaxed);
    }

    fn get(&self, stat: &Req) -> Option<Entry<Res, Err>> {
        self.cache.get(stat)
    }

    fn insert_response(&self, key: Req, value: Res) {
        self.cache.insert(
            key,
            Entry::Data {
                response: value,
                last_update: std::time::Instant::now(),
            },
        );
    }

    fn insert_notify(&self, key: Req) -> UpdateNotify<Res, Err> {
        let notify = Arc::new(UpdateNotify::new());
        self.cache
            .insert(key, Entry::UpdateInProgress(notify.clone()));

        notify
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

impl<E> From<CoalesceError<E>> for E {
    #[inline]
    fn from(value: CoalesceError<E>) -> Self {
        value.into_inner()
    }
}

#[derive(Clone, Debug)]
enum Entry<Response, Error> {
    UpdateInProgress(Arc<UpdateNotify<Response, Error>>),
    Data {
        response: Response,
        last_update: std::time::Instant,
    },
}

#[derive(Clone, Debug)]
struct UpdateNotify<T, E> {
    notify: Notify,
    data: Mutex<Option<Result<T, E>>>,
    has_data: AtomicBool,
}

impl<T, E> UpdateNotify<T, E>
where
    T: Clone,
    E: Clone,
{
    fn new() -> Self {
        UpdateNotify {
            notify: Notify::new(),
            data: Mutex::new(None),
            has_data: Default::default(),
        }
    }

    fn notify_err(&self, error: E) {
        {
            let mut data = self.data.lock();
            *data = NotifyData::Err(error)
        }
        self.has_data.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    fn notify_ok(&self, value: T) {
        {
            let mut data = self.data.lock();
            *data = NotifyData::Ok(value);
        }
        self.has_data.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    async fn wait(&self) -> Result<T, E> {
        if self.has_data.load(Ordering::Acquire) {
            return self.data.lock().as_result();
        }

        self.notify.notified().await;
        let data = self.data.lock();

        data.as_result()
    }
}

#[derive(Clone, Debug)]
pub struct Stats {
    pub requests_made: u64,
    pub cache_hit: u64,
    pub memory_usage: usize,
    pub len: usize,
    pub cache_hit_ratio: f64,
}

#[cfg(test)]
mod test {
    use std::convert::Infallible;
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn test_cache() {
        let cache: Cache<_, _, Infallible> = Cache::new(Duration::from_secs(1), 1024);

        let key = "key";

        // inserting a value
        let value = cache.get_or_update(key, fut()).await.unwrap();
        assert_eq!(value, "value"); // value is returned

        let start = std::time::Instant::now();

        let value = cache.get_or_update(key, fut()).await.unwrap();
        // value is returned immediately
        assert_eq!(value, "value");
        assert!(start.elapsed() < Duration::from_secs(1));

        tokio::time::sleep(Duration::from_secs(2)).await;
        // at this point the value is expired

        let start = std::time::Instant::now();
        let val = cache.get(&key).unwrap();
        // last update is more than 1 second ago
        assert!(
            matches!(val, Entry::Data { last_update, .. } if last_update.elapsed() > Duration::from_secs(1))
        );

        let value = cache.get_or_update(key, fut()).await.unwrap();
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
        let cache: Arc<Cache<i32, u32, Infallible>> =
            Arc::new(Cache::new(Duration::from_secs(1), 2));

        // creating 3 updates so that the first one is evicted
        let key1 = 1;
        let key2 = 2;
        let key3 = 3;

        let value1 = {
            let cache = cache.clone();
            tokio::spawn(async move {
                cache
                    .get_or_update(key1, fut2(Duration::from_secs(1), 1337))
                    .await
            })
        };
        let value2 = {
            let cache = cache.clone();
            tokio::spawn(async move {
                cache
                    .get_or_update(key2, fut2(Duration::from_secs(0), 1337))
                    .await
            })
        };
        let value3 = {
            let cache = cache.clone();
            tokio::spawn(async move {
                cache
                    .get_or_update(key3, fut2(Duration::from_secs(0), 1337))
                    .await
            })
        };

        let value1_second_get = {
            let cache = cache.clone();
            tokio::spawn(async move {
                cache
                    .get_or_update(key1, fut2(Duration::from_secs(0), 1337))
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

        println!("len: {}", cache.cache.cache.lock().memory_usage());

        let lock = cache.cache.cache.lock();
        for elt in lock.iter() {
            println!("{}: {:?}", elt.0, elt.1);
        }
    }

    async fn fut2(time: Duration, retval: u32) -> Result<u32, Infallible> {
        tokio::time::sleep(time).await;
        println!("fut2 finished after {time:?}");
        Ok(retval)
    }

    #[tokio::test]
    async fn test_uner_load() {
        let cache: Arc<Cache<i32, u32, Infallible>> =
            Arc::new(Cache::new(Duration::from_secs(1), 2));

        let start = std::time::Instant::now();
        // spawning 100 task groups which will try to get the same key within group

        let mut futures_list = Vec::new();
        for key in 0..100 {
            {
                let cache = cache.clone();
                tokio::spawn(async move {
                    cache
                        .get_or_update(key, fut2(Duration::from_secs(5), key as u32))
                        .await
                });
            }

            let mut futures = Vec::new();
            for _ in 0..100 {
                let cache = cache.clone();
                let handle = tokio::spawn(async move {
                    cache
                        .get_or_update(key, fut2(Duration::from_secs(0), key as u32))
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
    }
}
