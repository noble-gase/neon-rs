use std::time::Duration;

use bb8::ManageConnection;

use crate::manager;

pub type SinglePool = bb8::Pool<manager::RedisConnManager>;

pub type ClusterPool = bb8::Pool<manager::RedisClusterManager>;

pub trait Factory {
    type Manager: ManageConnection<Error: std::error::Error + Send + Sync + 'static>;

    fn build(dsn: Vec<impl AsRef<str>>) -> anyhow::Result<Self::Manager>;
}

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

pub struct Cluster;

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

#[derive(Default, Debug)]
pub struct Params {
    pub max_size: Option<u32>,
    pub min_idle: Option<u32>,
    pub conn_timeout: Option<Duration>,
    pub idle_timeout: Option<Duration>,
    pub max_lifetime: Option<Duration>,
}

/// 生成 Redis 连接池
///
/// # Examples
///
/// ```
/// // DSN
/// // redis://<host>:6379/<db>
/// // redis://:<password>@<host>:6379/<db>
/// // redis://<username>:<password>@<host>:6379/<db>
///
/// // 单节点
/// let x = redix::open::<redix::Single>(vec!["dsn"], None).await;
///
/// // 集群
/// let x = redix::open::<redix::Cluster>(vec!["dsn1", "dsn2"], None).await;
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
