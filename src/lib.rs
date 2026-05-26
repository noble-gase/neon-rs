//! `ners`：可按 feature 按需组装的 Rust 工具集。
//!
//! 默认包含 [`core`] 及其 [`core::helper`]（[`make_ctx`](core::helper::make_ctx)、
//! [`nonce`](core::helper::nonce)）；其余模块通过 Cargo feature 启用，
//! 例如 `crypto`、`redis`、`sql`、`macros`、`core-zoned` 等

pub use ners_core as core;

#[cfg(any(
    feature = "crypto",
    feature = "crypto-hash",
    feature = "crypto-aes",
    feature = "crypto-des",
    feature = "crypto-rsa"
))]
pub use ners_crypto as crypto;

#[cfg(feature = "macros")]
pub use ners_macros as macros;

#[cfg(any(feature = "redis", feature = "redis-cluster", feature = "redis-sync-lock"))]
pub use ners_redis as redis;

#[cfg(any(feature = "sql", feature = "sql-mysql", feature = "sql-postgres", feature = "sql-sqlite"))]
pub use ners_sql as sql;
