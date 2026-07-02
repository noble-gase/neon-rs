//! 基于 Redis `SET NX` + TTL 的分布式互斥锁
//!
//! **注意**：这不是 Antirez 的多 master quorum Redlock 算法；
//! 仅在单个 Redis 实例 或 Redis Cluster 上通过单 key 互斥，不提供跨独立 Redis 实例的 quorum 语义
//! 未获锁时 [`RedLock::acquire`](RedLock::acquire) / [`AsyncRedLock::acquire`](AsyncRedLock::acquire) 返回 `Ok(None)`，不保证公平排队
//!
//! 锁无自动续期（watchdog）机制：**TTL 必须大于临界区最坏耗时**，
//! 否则锁提前过期后互斥失效；TTL 按毫秒精度（`PX`）下发，最小 1ms

#[cfg(feature = "sync-lock")]
use redis::Commands;
use redis::{AsyncCommands, ExistenceCheck::NX, SetExpiry::PX};
#[cfg(feature = "sync-lock")]
use std::thread;
use std::time;
use tokio::time::sleep;
use uuid::Uuid;

/// 释放锁脚本：仅当 value 与持有者 token 一致时才 `DEL`
const DEL: &str = r#"
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
else
	return 0
end
"#;

/// 基于Redis的分布式锁（离开作用域自动释放）
///
/// # Examples
///
/// ```ignore
/// // pool 为 r2d2::Pool<redis::Client> 或 r2d2::Pool<redis::cluster::ClusterClient>
/// let lock = RedLock::new(pool.clone(), "key", Duration::from_secs(10)).acquire()?;
/// if lock.is_none() {
///     return Err("operation is too frequent, please try again later")
/// }
/// // 手动释放
/// lock.unwrap().release()?;
///
/// // 尝试获取锁（重试3次，间隔100ms）
/// let lock = RedLock::new(pool.clone(), "key", Duration::from_secs(10)).try_acquire(3, Duration::from_millis(100))?;
/// if lock.is_none() {
///     return Err("operation is too frequent, please try again later")
/// }
/// // 手动释放
/// lock.unwrap().release()?;
/// ```
#[cfg(feature = "sync-lock")]
pub struct RedLock<M>
where
    M: r2d2::ManageConnection,
    M::Connection: Commands,
{
    pool: r2d2::Pool<M>,
    key: String,
    ttl: time::Duration,
    token: Option<String>,
    prevent: bool,
}

#[cfg(feature = "sync-lock")]
impl<M> RedLock<M>
where
    M: r2d2::ManageConnection,
    M::Connection: Commands,
{
    /// `pool` 为 r2d2 连接池（单节点 `r2d2::Pool<redis::Client>` 或集群 `r2d2::Pool<ClusterClient>`）
    pub fn new(pool: r2d2::Pool<M>, key: impl Into<String>, ttl: time::Duration) -> Self {
        RedLock {
            pool,
            key: key.into(),
            ttl,
            token: None,
            prevent: false,
        }
    }

    /// 获取锁
    pub fn acquire(mut self) -> anyhow::Result<Option<Self>> {
        self.set_nx()?;
        if self.token.is_none() {
            return Ok(None);
        }
        Ok(Some(self))
    }

    /// 阻塞式重试获取锁
    ///
    /// `attempts` 为最大尝试次数；相邻两次尝试间隔 `duration`
    pub fn try_acquire(
        mut self,
        attempts: usize,
        duration: time::Duration,
    ) -> anyhow::Result<Option<Self>> {
        let threshold = attempts.saturating_sub(1);
        for i in 0..attempts {
            self.set_nx()?;
            if self.token.is_some() {
                return Ok(Some(self));
            }
            if i < threshold {
                thread::sleep(duration);
            }
        }
        Ok(None)
    }

    /// 手动释放锁
    pub fn release(&mut self) -> anyhow::Result<()> {
        if self.token.is_none() {
            return Ok(());
        }

        let token = self.token.take().unwrap();
        let mut conn = self.pool.get()?;
        let _: () = redis::Script::new(DEL)
            .key(&self.key)
            .arg(&token)
            .invoke(&mut *conn)?;
        Ok(())
    }

    /// 调用 `prevent` 后，Drop 时不会自动释放锁
    pub fn prevent(&mut self) {
        self.prevent = true;
    }

    fn set_nx(&mut self) -> anyhow::Result<()> {
        let new_token = Uuid::new_v4().to_string();

        // 毫秒精度下发 TTL：秒级截断会使锁比请求的提前过期（如 1.5s → 1s），破坏互斥
        let opts = redis::SetOptions::default()
            .conditional_set(NX)
            .with_expiration(PX(ttl_millis(self.ttl)));
        let mut conn = self.pool.get()?;
        match conn.set_options(&self.key, &new_token, opts) {
            Ok(v) => {
                if v {
                    self.token = Some(new_token);
                }
                Ok(())
            }
            Err(e) => {
                // SET 异常时 GET 一次，避免因网络错误误判加锁失败
                let ret_get: Option<String> = conn.get(&self.key)?;
                let v = ret_get.ok_or(e)?;
                if v == new_token {
                    self.token = Some(new_token);
                }
                Ok(())
            }
        }
    }
}

/// 自动释放锁
#[cfg(feature = "sync-lock")]
impl<M> Drop for RedLock<M>
where
    M: r2d2::ManageConnection,
    M::Connection: Commands,
{
    fn drop(&mut self) {
        if self.prevent || self.token.is_none() {
            return;
        }

        if let Err(e) = self.release() {
            tracing::error!(err = ?e, "[neon-redis.red_lock] drop release(key={}) failed", self.key);
        }
    }
}

/// 基于Redis的异步分布式锁（离开作用域自动释放）
///
/// # Examples
///
/// ```ignore
/// // 获取锁（conn 由具体客户端提供，如 client.conn()）
/// let lock = AsyncRedLock::new(client.conn(), "key", Duration::from_secs(10))
///     .acquire()
///     .await?;
/// if lock.is_none() {
///     return Err("operation is too frequent, please try again later")
/// }
/// // 手动释放
/// lock.unwrap().release().await?;
///
/// // 尝试获取锁（重试3次，间隔100ms）
/// let lock = AsyncRedLock::new(client.conn(), "key", Duration::from_secs(10))
///     .try_acquire(3, Duration::from_millis(100))
///     .await?;
/// if lock.is_none() {
///     return Err("operation is too frequent, please try again later")
/// }
/// // 手动释放
/// lock.unwrap().release().await?;
/// ```
pub struct AsyncRedLock<C: AsyncCommands + Clone + Send + 'static> {
    conn: C,
    key: String,
    ttl: time::Duration,
    token: Option<String>,
    prevent: bool,
}

impl<C: AsyncCommands + Clone + Send + 'static> AsyncRedLock<C> {
    /// `conn` 为执行命令的连接（如 `client.conn()` 取得的 `ConnectionManager` / `ClusterConnection`）
    pub fn new(conn: C, key: impl Into<String>, ttl: time::Duration) -> Self {
        AsyncRedLock {
            conn,
            key: key.into(),
            ttl,
            token: None,
            prevent: false,
        }
    }

    /// 获取锁
    pub async fn acquire(mut self) -> anyhow::Result<Option<Self>> {
        self.set_nx().await?;
        if self.token.is_none() {
            return Ok(None);
        }
        Ok(Some(self))
    }

    /// 异步重试获取锁
    ///
    /// `attempts` 为最大尝试次数；相邻两次尝试间隔 `duration`
    pub async fn try_acquire(
        mut self,
        attempts: usize,
        duration: time::Duration,
    ) -> anyhow::Result<Option<Self>> {
        let threshold = attempts.saturating_sub(1);
        for i in 0..attempts {
            self.set_nx().await?;
            if self.token.is_some() {
                return Ok(Some(self));
            }
            if i < threshold {
                sleep(duration).await;
            }
        }
        Ok(None)
    }

    /// 手动释放锁
    pub async fn release(&mut self) -> anyhow::Result<()> {
        if self.token.is_none() {
            return Ok(());
        }

        let token = self.token.take().unwrap();
        redis::Script::new(DEL)
            .key(&self.key)
            .arg(&token)
            .invoke_async::<()>(&mut self.conn)
            .await?;
        Ok(())
    }

    /// 调用 `prevent` 后，Drop 时不会自动释放锁
    pub fn prevent(&mut self) {
        self.prevent = true;
    }

    async fn set_nx(&mut self) -> anyhow::Result<()> {
        let new_token = Uuid::new_v4().to_string();

        // 毫秒精度下发 TTL：秒级截断会使锁比请求的提前过期（如 1.5s → 1s），破坏互斥
        let opts = redis::SetOptions::default()
            .conditional_set(NX)
            .with_expiration(PX(ttl_millis(self.ttl)));
        match self.conn.set_options(&self.key, &new_token, opts).await {
            Ok(v) => {
                if v {
                    self.token = Some(new_token);
                }
                Ok(())
            }
            Err(e) => {
                // SET 异常时 GET 一次，避免因网络错误误判加锁失败
                let ret_get: Option<String> = self.conn.get(&self.key).await?;
                let v = ret_get.ok_or(e)?;
                if v == new_token {
                    self.token = Some(new_token);
                }
                Ok(())
            }
        }
    }
}

impl<C: AsyncCommands + Clone + Send + 'static> Drop for AsyncRedLock<C> {
    fn drop(&mut self) {
        if self.prevent || self.token.is_none() {
            return;
        }

        let key = self.key.clone();
        let token = self.token.take().unwrap();
        let conn = self.conn.clone();

        spawn_release(conn, key, token);
    }
}

/// TTL 转毫秒（最小 1ms，超大值饱和为 u64::MAX）
fn ttl_millis(ttl: time::Duration) -> u64 {
    u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX).max(1)
}

/// Drop 中后台释放锁：在独立任务里执行 `DEL` 脚本（连接句柄需 `Send + 'static`）
///
/// Drop 可能发生在 tokio runtime 之外（runtime 已关闭、锁被移入普通线程等）：
/// 此时无法 spawn，降级为记日志，锁由 TTL 到期自动释放；若在 Drop 中无条件
/// `tokio::spawn` 会直接 panic，而 Drop 中的 panic 会导致进程 abort
fn spawn_release<C>(mut conn: C, key: String, token: String)
where
    C: AsyncCommands + Send + 'static,
{
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        tracing::warn!(
            key = %key,
            "[neon-redis.async_red_lock] drop 时不在 tokio runtime 内，无法后台释放锁，将由 TTL 到期自动释放"
        );
        return;
    };
    handle.spawn(async move {
        if let Err(e) = async {
            redis::Script::new(DEL)
                .key(&key)
                .arg(&token)
                .invoke_async::<()>(&mut conn)
                .await?;
            Ok::<_, anyhow::Error>(())
        }
        .await
        {
            tracing::error!(err = ?e, "[neon-redis.async_red_lock] drop release(key={}) failed", key);
        }
    });
}

// `AsyncDrop` 稳定后可替换上方 `tokio::spawn` 方案，在 drop 中 await 释放锁
// impl AsyncDrop for AsyncRedLock {
//     fn drop(&mut self) {
//         if self.prevent || self.token.is_none() {
//             return;
//         }
//
//         // 释放锁
//         let ret = self.release().await;
//         if let Err(e) = ret {
//             tracing::error!(err = ?e, "[mutex.async_red_lock] drop release(key={}) failed", self.key);
//         }
//     }
// }

#[cfg(test)]
mod tests {
    use std::time::Duration;

    #[cfg(feature = "cluster")]
    use crate::factory::Cluster;
    use crate::{factory::Single, open};

    use super::*;

    #[cfg(feature = "sync-lock")]
    #[test]
    #[ignore = "requires local Redis at redis://127.0.0.1:6379"]
    fn test_red_lock() {
        let pool = r2d2::Pool::new(redis::Client::open("redis://127.0.0.1:6379").unwrap()).unwrap();
        let lock = RedLock::new(pool, "test_red_lock", Duration::from_secs(10))
            .acquire()
            .unwrap();
        assert!(lock.is_some());
    }

    #[cfg(all(feature = "sync-lock", feature = "cluster"))]
    #[test]
    #[ignore = "requires local Redis cluster"]
    fn test_red_lock_cluster() {
        let cc = redis::cluster::ClusterClient::new(vec!["redis://127.0.0.1:6379"]).unwrap();
        let pool = r2d2::Pool::builder().build(cc).unwrap();
        let lock = RedLock::new(pool, "test_red_lock_cluster", Duration::from_secs(10))
            .acquire()
            .unwrap();
        assert!(lock.is_some());
    }

    #[tokio::test]
    #[ignore = "requires local Redis at redis://127.0.0.1:6379"]
    async fn test_async_red_lock() {
        let client = open::<Single>(vec!["redis://127.0.0.1:6379"], None)
            .await
            .unwrap();

        {
            let lock = AsyncRedLock::new(
                client.conn(),
                "test_async_red_lock",
                Duration::from_secs(10),
            )
            .acquire()
            .await
            .unwrap();
            assert!(lock.is_some());
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    #[ignore = "requires local Redis cluster"]
    async fn test_async_red_lock_cluster() {
        let client = open::<Cluster>(vec!["redis://127.0.0.1:6379"], None)
            .await
            .unwrap();

        {
            let lock = AsyncRedLock::new(
                client.conn(),
                "test_async_red_lock_cluster",
                Duration::from_secs(10),
            )
            .acquire()
            .await
            .unwrap();
            assert!(lock.is_some());
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
