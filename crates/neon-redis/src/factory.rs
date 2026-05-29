//! Redis 连接池：单节点与 Cluster（`cluster` feature）的 bb8 / r2d2 封装

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
