//! Redis 客户端封装：共享复用连接 + 底层 client（单节点 / Cluster）

use redis::aio::{ConnectionManager, MultiplexedConnection, PubSub};
use redis::{Client, RedisResult};

#[cfg(feature = "cluster")]
use redis::cluster::ClusterClient;
#[cfg(feature = "cluster")]
use redis::cluster_async::ClusterConnection;

use crate::ConnOptions;

/// Redis 封装：
/// - `manager`: 启动时建一次、共享复用的 `ConnectionManager`（带自动重连），用于普通非阻塞命令
/// - `client`:  保留底层 `Client`，用于按需新建独占连接（阻塞命令 / Pub-Sub 等）
#[derive(Clone)]
pub struct AsyncSingle {
    client: Client,
    manager: ConnectionManager,
}

impl AsyncSingle {
    /// 默认配置初始化
    pub async fn new(url: &str) -> RedisResult<Self> {
        let client = Client::open(url)?;
        let manager = client.get_connection_manager().await?;
        Ok(Self { client, manager })
    }

    /// 使用 crate 的 `ConnOptions` 初始化（自定义重连/超时配置）
    pub async fn with_options(url: &str, opt: &ConnOptions) -> RedisResult<Self> {
        let client = Client::open(url)?;
        let manager =
            ConnectionManager::new_with_config(client.clone(), opt.to_manager_config()).await?;
        Ok(Self { client, manager })
    }

    /// 普通命令：返回共享 `ConnectionManager` 的 clone（廉价）
    ///
    /// 命令方法签名是 `&mut self`，所以这里直接给出一个可变副本
    pub fn conn(&self) -> ConnectionManager {
        self.manager.clone()
    }

    /// 阻塞命令（如：BLPOP、BRPOP等）/ 需要独占的场景：新开一条独占的 `MultiplexedConnection`
    ///
    /// 注意：这条连接不要与其他流量共享
    pub async fn open_conn(&self) -> RedisResult<MultiplexedConnection> {
        self.client.get_multiplexed_async_connection().await
    }

    /// 新开一条 Pub/Sub 连接（独占，专用于 `SUBSCRIBE` / `PSUBSCRIBE`）
    pub async fn pubsub(&self) -> RedisResult<PubSub> {
        self.client.get_async_pubsub().await
    }

    /// 暴露底层 `client`（特殊需求）
    pub fn raw_client(&self) -> &Client {
        &self.client
    }
}

/// Redis Cluster 封装：
/// - `conn`:   启动时建一次、共享复用的 `ClusterConnection`，用于普通非阻塞命令
/// - `client`: 保留 `ClusterClient`，用于按需新建独立连接（阻塞命令等）
#[cfg(feature = "cluster")]
#[derive(Clone)]
pub struct AsyncCluster {
    client: ClusterClient,
    conn: ClusterConnection,
}

#[cfg(feature = "cluster")]
impl AsyncCluster {
    /// 初始化。nodes 只需给集群中任意一个或多个种子节点
    pub async fn new(nodes: Vec<String>) -> RedisResult<Self> {
        let client = ClusterClient::new(nodes)?;
        let conn = client.get_async_connection().await?;
        Ok(Self { client, conn })
    }

    /// 使用 crate 的 `ConnOptions` 初始化（映射重试次数、连接/响应超时）
    pub async fn with_options(nodes: Vec<String>, opt: &ConnOptions) -> RedisResult<Self> {
        let client = opt.to_cluster_builder(nodes).build()?;
        let conn = client.get_async_connection().await?;
        Ok(Self { client, conn })
    }

    /// 普通命令：返回共享 `ClusterConnection` 的 clone（廉价）
    ///
    /// 自动路由 / MOVED-ASK 重定向 / 重连均由 cluster 层处理
    pub fn conn(&self) -> ClusterConnection {
        self.conn.clone()
    }

    /// 阻塞命令（如：BLPOP、BRPOP等）  / 需要独占的场景：新开一条独立的 `ClusterConnection`
    ///
    /// 与主流量不共享，避免阻塞命令拖累其他请求；路由和故障转移仍由 cluster 层处理
    pub async fn open_conn(&self) -> RedisResult<ClusterConnection> {
        self.client.get_async_connection().await
    }

    /// 新开一条专用于 Pub/Sub 的独占 `ClusterConnection`
    ///
    /// 集群没有独立的 PubSub 类型，订阅基于 RESP3 push：在该连接上调用
    /// `subscribe` / `psubscribe` / `ssubscribe`，再从连接读取推送消息。
    /// 要求客户端启用 RESP3（在 DSN 上带 `?protocol=resp3`），否则订阅会报错
    pub async fn pubsub(&self) -> RedisResult<ClusterConnection> {
        self.client.get_async_connection().await
    }

    /// 暴露底层 `client`（特殊需求）
    pub fn raw_client(&self) -> &ClusterClient {
        &self.client
    }
}
