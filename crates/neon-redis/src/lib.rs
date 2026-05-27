//! Redis 工具集：连接池、缓存 helper（`redkit`）、分布式锁（`redlock`）。
//!
//! `redlock` 为单 key 互斥锁（`SET NX` + TTL），非 Antirez quorum Redlock。

pub mod client;
pub mod manager;
pub mod redkit;
pub mod redlock;

/// 同步分布式锁 [`redlock::RedLock`] 使用的连接池（Single / Cluster，需 `sync-lock` feature）。
#[cfg(feature = "sync-lock")]
pub enum SyncPool {
    /// 单节点 r2d2 连接池。
    Single(r2d2::Pool<redis::Client>),
    #[cfg(feature = "cluster")]
    /// Redis Cluster r2d2 连接池。
    Cluster(r2d2::Pool<redis::cluster::ClusterClient>),
}

/// 异步 API 使用的连接池（[`redkit`]、[`redlock::AsyncRedLock`] 等；Single / Cluster）。
#[derive(Clone)]
pub enum AsyncPool {
    /// 单节点 bb8 连接池。
    Single(client::SinglePool),
    #[cfg(feature = "cluster")]
    /// Redis Cluster bb8 连接池。
    Cluster(client::ClusterPool),
}
