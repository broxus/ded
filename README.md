# ded &emsp; [![crates-io-batch]][crates-io-link] [![docs-badge]][docs-url] [![rust-version-badge]][rust-version-link] [![workflow-badge]][workflow-link]

[crates-io-batch]: https://img.shields.io/crates/v/ded.svg

[crates-io-link]: https://crates.io/crates/ded

[docs-badge]: https://docs.rs/ded/badge.svg

[docs-url]: https://docs.rs/ded

[rust-version-badge]: https://img.shields.io/badge/rustc-1.65+-lightgray.svg

[rust-version-link]: https://blog.rust-lang.org/2022/11/03/Rust-1.65.0.html

[workflow-badge]: https://img.shields.io/github/actions/workflow/status/broxus/ded/master.yml?branch=master

[workflow-link]: https://github.com/broxus/ded/actions?query=workflow%3Amaster

Dead Easy Deduplication

## Usage

```toml
[dependencies]
ded = "0.1.0"
```

```rust
let cache = DedCache::new(Duration::from_secs(1), 1024);

let key = "key";
async fn value_fut() -> Result<&'static str, Infallible> {
    tokio::time::sleep(Duration::from_secs(2)).await;
    Ok("value")
}

// Accessing a new value
let value = cache.get_or_update(key, value_fut).await?;
assert_eq!(value, "value"); // value is returned, request is performed

// Accessing a cached value
{
    let start = std::time::Instant::now();
    
    let value = cache.get_or_update(key, value_fut).await?;
    assert_eq!(value, "value");

    // Value was returned immediately
    assert!(start.elapsed() < Duration::from_secs(1));
}
```

### Description

DED is a lib which performs request coalescing and caching backed
by [schnellru](https://github.com/koute/schnellru)
