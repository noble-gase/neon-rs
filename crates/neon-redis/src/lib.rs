//! Redis 工具集：异步连接（`ConnectionManager` / 集群连接）、缓存 helper（`redkit`）、分布式锁（`redlock`）
//!
//! 异步路径基于 redis 自带的可共享 clone、自带自动重连的多路复用连接：
//! - 单节点使用 [`redis::aio::ConnectionManager`]
//! - 集群（`cluster` feature）使用 [`redis::cluster_async::ClusterConnection`]
//!
//! `redlock` 为单 key 互斥锁（`SET NX` + TTL），非 Antirez quorum Redlock

pub mod client;
pub mod factory;
pub mod redkit;
pub mod redlock;

use std::time::Duration;

use redis::aio::ConnectionManagerConfig;

#[cfg(feature = "cluster")]
use redis::cluster::ClusterClient;

use crate::factory::Factory;

/// 连接参数：重连退避与超时配置
///
/// 单节点映射到 `ConnectionManagerConfig`
///
/// Cluster 映射到 `ClusterClientBuilder`
#[derive(Default, Debug, Clone)]
pub struct ConnOptions {
    /// 连接断开后的最大重连次数，默认 6
    pub number_of_retries: Option<usize>,
    /// 重连退避的最小间隔（仅单节点）
    pub min_delay: Option<Duration>,
    /// 重连退避的最大间隔（仅单节点）
    pub max_delay: Option<Duration>,
    /// 指数退避底数（仅单节点）
    pub exponent_base: Option<f32>,
    /// 单条命令响应超时
    pub response_timeout: Option<Duration>,
    /// 建立连接超时
    pub connection_timeout: Option<Duration>,
}

impl ConnOptions {
    /// 转换为单节点 [`ConnectionManagerConfig`]
    pub(crate) fn to_manager_config(&self) -> ConnectionManagerConfig {
        let mut cfg = ConnectionManagerConfig::new();
        if let Some(n) = self.number_of_retries {
            cfg = cfg.set_number_of_retries(n);
        }
        if let Some(d) = self.min_delay {
            cfg = cfg.set_min_delay(d);
        }
        if let Some(d) = self.max_delay {
            cfg = cfg.set_max_delay(d);
        }
        if let Some(b) = self.exponent_base {
            cfg = cfg.set_exponent_base(b);
        }
        if self.response_timeout.is_some() {
            cfg = cfg.set_response_timeout(self.response_timeout);
        }
        if self.connection_timeout.is_some() {
            cfg = cfg.set_connection_timeout(self.connection_timeout);
        }
        cfg
    }

    /// 将可用子集（重试次数、连接/响应超时）应用到集群 [`ClusterClientBuilder`](redis::cluster::ClusterClientBuilder)
    ///
    /// 退避相关参数（`min_delay` / `max_delay` / `exponent_base`）集群层不支持，忽略
    #[cfg(feature = "cluster")]
    pub(crate) fn to_cluster_builder(
        &self,
        nodes: Vec<String>,
    ) -> redis::cluster::ClusterClientBuilder {
        let mut builder = ClusterClient::builder(nodes);
        if let Some(n) = self.number_of_retries {
            builder = builder.retries(n as u32);
        }
        if let Some(d) = self.connection_timeout {
            builder = builder.connection_timeout(d);
        }
        if let Some(d) = self.response_timeout {
            builder = builder.response_timeout(d);
        }
        builder
    }
}

/// 建立 Redis 客户端，返回具体客户端类型
///
/// 具体客户端可用 `conn()` 取共享连接执行命令、`open_conn()` / `pubsub()` 新开独占连接；
/// `conn()` 返回的连接句柄可直接传给 [`redkit`] / [`redlock`]
///
/// # Examples
///
/// DSN 格式：`redis://[<用户名>][:<密码>]@<host>:<port>[/<db>]`
///
/// TLS 支持：将 scheme 换成 `rediss://`（需启用本 crate 的 TLS feature：
/// `tls-rustls`（rustls 后端）或 `tls-native-tls`（native-tls 后端））
/// - `rediss://[<用户名>][:<密码>]@<host>:<port>[/<db>]`
/// - 自签名证书 / 跳过证书校验：
///     - 启用 `tls-rustls-insecure` feature
///     - 在末尾加 `#insecure`，如：`rediss://<host>:<port>/0#insecure`
///
/// ```ignore
/// // 单节点：得到 client::Single
/// let single = redix::open::<Single>(vec!["redis://127.0.0.1:6379"], None).await?;
///
/// // 单节点 + TLS
/// let single = redix::open::<Single>(vec!["rediss://:pass@127.0.0.1:6380/0"], None).await?;
///
/// // 集群（需启用 `cluster` feature）：得到 client::Cluster
/// let cluster = redix::open::<Cluster>(vec!["redis://127.0.0.1:6379"], None).await?;
///
/// // 取共享连接执行命令 / 传给 redkit、redlock
/// let mut conn = single.conn();
/// ```
pub async fn open<F>(
    dsn: Vec<impl AsRef<str> + Send>,
    opt: Option<ConnOptions>,
) -> anyhow::Result<F::Client>
where
    F: Factory,
{
    F::open(dsn, opt).await
}
