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
use schnellru::{Limiter, LruMap};
use tokio::sync::watch;

/// Deduplication cache for requests.
pub struct DedCache<Req, Res, Err> {
    cache: RequestLru<Req, Res, Err>,
    lifetime: Duration,
    requests_made: AtomicU64,
    calls_made: AtomicU64,
    cache_hit: AtomicU64,
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
        DedCache {
            cache: RequestLru::new(size),
            lifetime,
            requests_made: Default::default(),
            cache_hit: Default::default(),
            calls_made: Default::default(),
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

        {
            let mut cache = self.cache.lock();

            match cache.get(&key) {
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
                    let mut notify = notify.subscribe();
                    {
                        let result = notify.borrow();
                        if let Some(result) = &*result {
                            return result.clone().map_err(CoalesceError::Indirect);
                        }
                    }

                    // Waiting for an existing response
                    if notify.changed().await.is_ok() {
                        let result = notify.borrow().clone().unwrap();
                        self.update_cache_hit();
                        return result.map_err(CoalesceError::Indirect);
                    }
                }
            }

            let (tx, _) = watch::channel(None);
            let tx = Arc::new(tx);
            cache.insert(key.clone(), Entry::UpdateInProgress(tx.clone()));
            tx
        }

        if let Some(entry) = self.get(&key) {}

        // Initiate request otherwise
        self.update_data(key, f).await
    }

    /// Returns cache performance statistics.
    pub fn fetch_stats(&self) -> Stats {
        let requests_made = self.requests_made.load(Ordering::Relaxed);
        let cache_hit = self.cache_hit.load(Ordering::Relaxed);
        let calls_made = self.calls_made.load(Ordering::Relaxed);

        let LruStats { memory_usage, len } = self.cache.stats();
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

    async fn update_data<F, Fut>(&self, key: Req, f: F) -> Result<Res, CoalesceError<Err>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Res, Err>>,
    {
        struct RemoveWatchOnDrop<'a, Req, Res, Err>
        where
            Req: Hash + Eq,
            Entry<Res, Err>: Clone,
        {
            key: Option<&'a Req>,
            cache: &'a RequestLru<Req, Res, Err>,
        }

        impl<Req, Res, Err> RemoveWatchOnDrop<'_, Req, Res, Err>
        where
            Req: Hash + Eq,
            Entry<Res, Err>: Clone,
        {
            fn disarm(mut self) {
                self.key = None;
            }
        }

        impl<Req, Res, Err> Drop for RemoveWatchOnDrop<'_, Req, Res, Err>
        where
            Req: Hash + Eq,
            Entry<Res, Err>: Clone,
        {
            fn drop(&mut self) {
                if let Some(key) = self.key.take() {
                    self.cache.remove(key);
                }
            }
        }

        // Create notifier if it is a new request
        let watch = self.insert_watch(&key);

        // Remove watch from the cache if the guard was not disarmed
        let drop_guard = RemoveWatchOnDrop {
            key: watch.is_some().then_some(&key),
            cache: &self.cache,
        };

        // Execute request
        let result = f().await;
        self.update_calls_number();

        // Force replace the data in cache (even if no watch was created)
        if let Ok(response) = &result {
            // Prevent watch from being removed from the cache
            drop_guard.disarm();

            self.insert_response(key, response.clone());
        }

        // Update watch if some
        if let Some(watch) = watch {
            watch.send_modify(|value| *value = Some(result.clone()));
        }

        // Done
        result.map_err(CoalesceError::Direct)
    }

    fn update_calls_number(&self) {
        self.calls_made.fetch_add(1, Ordering::Relaxed);
    }

    fn update_request_number(&self) {
        self.requests_made.fetch_add(1, Ordering::Relaxed);
    }

    fn update_cache_hit(&self) {
        self.cache_hit.fetch_add(1, Ordering::Relaxed);
    }

    fn insert_response(&self, key: Req, value: Res) {
        self.cache.insert(
            key,
            Entry::Data {
                response: value,
                last_update: Instant::now(),
            },
        );
    }

    fn insert_watch(&self, key: &Req) -> Option<Arc<RequestTx<Res, Err>>> {
        let (tx, _) = watch::channel(None);
        let tx = Arc::new(tx);
        self.cache
            .insert(key.clone(), Entry::UpdateInProgress(tx.clone()))
            .then_some(tx)
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
    pub requests_made: u64,
    pub cache_hit: u64,
    pub memory_usage: usize,
    pub len: usize,
    pub cache_hit_ratio: f64,
    pub calls_made: u64,
}

#[derive(Clone, Debug)]
enum Entry<T, E> {
    UpdateInProgress(Arc<RequestTx<T, E>>),
    Data { response: T, last_update: Instant },
}

type RequestTx<T, E> = watch::Sender<Option<Result<T, E>>>;

type RequestLru<K, V, E> = Mutex<LruMap<K, Entry<V, E>, ByLengthOfData>>;

impl<K, V, E> RequestLru<K, V, E>
where
    K: Hash + Eq,
    Entry<V, E>: Clone,
{
    fn new(size: u32) -> Self {
        RequestLru {
            cache: Mutex::new(LruMap::new(ByLengthOfData::new(size))),
        }
    }

    fn get(&self, key: &K) -> Option<Entry<V, E>> {
        self.cache.lock().get(key).cloned()
    }

    fn insert(&self, key: K, value: Entry<V, E>) -> bool {
        self.cache.lock().insert(key, value)
    }

    fn remove(&self, key: &K) -> bool {
        self.cache.lock().remove(key).is_some()
    }

    fn stats(&self) -> LruStats {
        let lock = self.cache.lock();

        let memory_usage = lock.memory_usage();
        let len = lock.len();

        LruStats { len, memory_usage }
    }
}

#[derive(Clone, Debug)]
struct LruStats {
    len: usize,
    memory_usage: usize,
}

#[derive(Copy, Clone, Debug)]
#[repr(transparent)]
struct ByLengthOfData {
    max_length: u32,
}

impl ByLengthOfData {
    const fn new(max_length: u32) -> Self {
        ByLengthOfData { max_length }
    }
}

impl<K, T, E> Limiter<K, Entry<T, E>> for ByLengthOfData {
    type KeyToInsert<'a> = K;
    type LinkType = u32;

    #[inline]
    fn is_over_the_limit(&self, length: usize) -> bool {
        length > self.max_length as usize
    }

    #[inline]
    fn on_insert(
        &mut self,
        _length: usize,
        key: Self::KeyToInsert<'_>,
        value: Entry<T, E>,
    ) -> Option<(K, Entry<T, E>)> {
        if self.max_length > 0 {
            Some((key, value))
        } else {
            None
        }
    }

    #[inline]
    fn on_replace(
        &mut self,
        _length: usize,
        _old_key: &mut K,
        _new_key: K,
        old_value: &mut Entry<T, E>,
        new_value: &mut Entry<T, E>,
    ) -> bool {
        !matches!(
            (old_value, new_value),
            (Entry::UpdateInProgress(_), Entry::UpdateInProgress(_))
        )
    }

    #[inline]
    fn on_removed(&mut self, _key: &mut K, _value: &mut Entry<T, E>) {}

    #[inline]
    fn on_cleared(&mut self) {}

    #[inline]
    fn on_grow(&mut self, _new_memory_usage: usize) -> bool {
        true
    }
}

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

        let start = std::time::Instant::now();
        let val = cache.get(&key).unwrap();
        // last update is more than 1 second ago
        assert!(
            matches!(val, Entry::Data { last_update, .. } if last_update.elapsed() > Duration::from_secs(1))
        );

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
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
