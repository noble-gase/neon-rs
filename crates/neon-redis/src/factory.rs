//! Redis 连接工厂：构建具体客户端（单节点 [`client::Single`](crate::client::Single) /
//! Cluster [`client::Cluster`](crate::client::Cluster)）

use std::future::Future;

#[cfg(feature = "cluster")]
use crate::client::AsyncCluster;
use crate::{ConnOptions, client::AsyncSingle};

/// 连接工厂：根据 DSN 异步建立具体客户端
pub trait Factory {
    /// 建立的具体客户端类型（单节点 / Cluster）
    type Client;

    /// 建立客户端
    fn open(
        dsn: Vec<String>,
        opt: Option<ConnOptions>,
    ) -> impl Future<Output = anyhow::Result<Self::Client>> + Send;
}

/// 单节点 Redis
pub struct Single;

impl Factory for Single {
    type Client = AsyncSingle;

    async fn open(dsn: Vec<String>, opt: Option<ConnOptions>) -> anyhow::Result<AsyncSingle> {
        let first = dsn.first().ok_or_else(|| anyhow::anyhow!("DSN is empty"))?;

        let client = match opt {
            Some(o) => AsyncSingle::with_options(first.as_ref(), &o).await?,
            None => AsyncSingle::new(first.as_ref()).await?,
        };

        Ok(client)
    }
}

/// Redis Cluster
#[cfg(feature = "cluster")]
pub struct Cluster;

#[cfg(feature = "cluster")]
impl Factory for Cluster {
    type Client = AsyncCluster;

    async fn open(dsn: Vec<String>, opt: Option<ConnOptions>) -> anyhow::Result<AsyncCluster> {
        let client = match opt {
            Some(o) => AsyncCluster::with_options(dsn, &o).await?,
            None => AsyncCluster::new(dsn).await?,
        };

        Ok(client)
    }
}
