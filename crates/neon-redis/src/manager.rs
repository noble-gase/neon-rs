/// bb8 单节点 Redis 连接管理器。
#[derive(Clone)]
pub struct RedisConnManager {
    client: redis::Client,
}

impl RedisConnManager {
    pub fn new(c: redis::Client) -> Self {
        Self { client: c }
    }
}

impl bb8::ManageConnection for RedisConnManager {
    type Connection = redis::aio::MultiplexedConnection;
    type Error = redis::RedisError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.client.get_multiplexed_async_connection().await
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        let pong: String = redis::cmd("PING").query_async(conn).await?;
        match pong.as_str() {
            "PONG" => Ok(()),
            _ => Err((redis::ErrorKind::ResponseError, "ping request").into()),
        }
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}

#[cfg(feature = "cluster")]
use redis::{cluster, cluster_async};

#[cfg(feature = "cluster")]
/// bb8 Redis Cluster 连接管理器。
#[derive(Clone)]
pub struct RedisClusterManager {
    client: cluster::ClusterClient,
}

#[cfg(feature = "cluster")]
impl RedisClusterManager {
    pub fn new(c: cluster::ClusterClient) -> Self {
        Self { client: c }
    }
}

#[cfg(feature = "cluster")]
impl bb8::ManageConnection for RedisClusterManager {
    type Connection = cluster_async::ClusterConnection;
    type Error = redis::RedisError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let c = self.client.get_async_connection().await?;
        Ok(c)
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        let pong: String = redis::cmd("PING").query_async(conn).await?;
        match pong.as_str() {
            "PONG" => Ok(()),
            _ => Err((redis::ErrorKind::ResponseError, "ping request").into()),
        }
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}
