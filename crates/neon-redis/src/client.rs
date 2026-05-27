//! Redis 连接池：单节点与 Cluster（`cluster` feature）的 bb8 / r2d2 封装。

use std::time::Duration;

use bb8::ManageConnection;

use crate::manager;

/// 单节点 Redis 连接池
pub type SinglePool = bb8::Pool<manager::RedisConnManager>;

/// Redis Cluster 连接池
#[cfg(feature = "cluster")]
pub type ClusterPool = bb8::Pool<manager::RedisClusterManager>;

/// 连接池工厂：根据 DSN 构建 bb8 `ManageConnection`
pub trait Factory {
    type Manager: ManageConnection<Error: std::error::Error + Send + Sync + 'static>;

    fn build(dsn: Vec<impl AsRef<str>>) -> anyhow::Result<Self::Manager>;
}

/// 单节点 Redis
pub struct Single;

impl Factory for Single {
    type Manager = manager::RedisConnManager;

    fn build(dsn: Vec<impl AsRef<str>>) -> anyhow::Result<Self::Manager> {
        let first = dsn.first().ok_or_else(|| anyhow::anyhow!("DSN is empty"))?;

        let client = redis::Client::open(first.as_ref())?;
        let mut conn = client.get_connection()?;
        let _ = redis::cmd("PING").query::<String>(&mut conn)?;

        Ok(manager::RedisConnManager::new(client))
    }
}

/// Redis Cluster
#[cfg(feature = "cluster")]
pub struct Cluster;

#[cfg(feature = "cluster")]
impl Factory for Cluster {
    type Manager = manager::RedisClusterManager;

    fn build(dsn: Vec<impl AsRef<str>>) -> anyhow::Result<Self::Manager> {
        let nodes: Vec<&str> = dsn.iter().map(|s| s.as_ref()).collect();

        let client = redis::cluster::ClusterClient::new(nodes)?;
        let mut conn = client.get_connection()?;
        let _ = redis::cmd("PING").query::<String>(&mut conn)?;

        Ok(manager::RedisClusterManager::new(client))
    }
}

/// 连接池参数；未设置的项使用库内默认值
#[derive(Default, Debug)]
pub struct Params {
    /// 最大连接数，默认 100。
    pub max_size: Option<u32>,
    /// 最小空闲连接数。
    pub min_idle: Option<u32>,
    /// 获取连接超时，默认 10 秒。
    pub conn_timeout: Option<Duration>,
    /// 空闲连接回收时间。
    pub idle_timeout: Option<Duration>,
    /// 连接最大存活时间。
    pub max_lifetime: Option<Duration>,
}

/// 创建 Redis 连接池
///
/// 建池前会对首个 DSN 执行 `PING` 校验连通性
///
/// # Examples
///
/// ```
/// // 单节点
/// let pool = open::<Single>(vec!["redis://127.0.0.1:6379"], None).await?;
///
/// // 集群（需启用 `cluster` feature）
/// let cluster = open::<Cluster>(vec!["redis://127.0.0.1:6379"], None).await?;
/// ```
pub async fn open<F>(dsn: Vec<impl AsRef<str>>, opt: Option<Params>) -> anyhow::Result<bb8::Pool<F::Manager>>
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
