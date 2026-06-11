//! Redis 工具集：连接池、缓存 helper（`redkit`）、分布式锁（`redlock`）
//!
//! `redlock` 为单 key 互斥锁（`SET NX` + TTL），非 Antirez quorum Redlock

pub mod factory;
pub mod manager;
pub mod redkit;
pub mod redlock;

use std::time::Duration;

use crate::factory::Factory;

/// 同步分布式锁 [`redlock::RedLock`] 使用的连接池（Single / Cluster，需 `sync-lock` feature）
#[cfg(feature = "sync-lock")]
pub enum SyncPool {
    /// 单节点 r2d2 连接池
    Single(r2d2::Pool<redis::Client>),
    #[cfg(feature = "cluster")]
    /// Redis Cluster r2d2 连接池
    Cluster(r2d2::Pool<redis::cluster::ClusterClient>),
}

/// 异步 API 使用的连接池（[`redkit`]、[`redlock::AsyncRedLock`] 等；Single / Cluster）
#[derive(Clone)]
pub enum AsyncPool {
    /// 单节点 bb8 连接池
    Single(factory::SinglePool),
    #[cfg(feature = "cluster")]
    /// Redis Cluster bb8 连接池
    Cluster(factory::ClusterPool),
}

/// 连接池参数
#[derive(Default, Debug)]
pub struct PoolParams {
    /// 最大连接数，默认 100
    pub max_size: Option<u32>,
    /// 最小空闲连接数
    pub min_idle: Option<u32>,
    /// 获取连接超时，默认 10 秒
    pub conn_timeout: Option<Duration>,
    /// 空闲连接回收时间
    pub idle_timeout: Option<Duration>,
    /// 连接最大存活时间
    pub max_lifetime: Option<Duration>,
}

/// 创建 Redis 连接池
///
/// # Examples
///
/// ```
/// // 单节点
/// let pool = redix::open::<Single>(vec!["redis://127.0.0.1:6379"], None).await?;
///
/// // 集群（需启用 `cluster` feature）
/// let cluster = redix::open::<Cluster>(vec!["redis://127.0.0.1:6379"], None).await?;
/// ```
pub async fn open<F>(
    dsn: Vec<impl AsRef<str>>,
    opt: Option<PoolParams>,
) -> anyhow::Result<bb8::Pool<F::Manager>>
where
    F: Factory,
{
    let manager = F::build(dsn)?;

    let params = opt.unwrap_or_default();

    let pool = bb8::Pool::builder()
        .max_size(params.max_size.unwrap_or(100))
        .min_idle(params.min_idle)
        .connection_timeout(params.conn_timeout.unwrap_or(Duration::from_secs(10)))
        .idle_timeout(params.idle_timeout)
        .max_lifetime(params.max_lifetime)
        .build(manager)
        .await?;

    Ok(pool)
}
