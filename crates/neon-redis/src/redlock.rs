//! 基于 Redis `SET NX` + TTL 的分布式互斥锁。
//!
//! **注意**：这不是 Antirez 的多 master quorum Redlock 算法；
//! 仅在单个 Redis 实例 或 Redis Cluster 上通过单 key 互斥，不提供跨独立 Redis 实例的 quorum 语义。
//! 未获锁时 [`RedLock::acquire`](RedLock::acquire) / [`AsyncRedLock::acquire`](AsyncRedLock::acquire) 返回 `Ok(None)`，不保证公平排队。

#[cfg(feature = "sync-lock")]
use redis::Commands;
use redis::{AsyncCommands, ExistenceCheck::NX, SetExpiry::EX};
#[cfg(feature = "sync-lock")]
use std::thread;
use std::time;
use tokio::time::sleep;
use uuid::Uuid;

use crate::AsyncPool;
#[cfg(feature = "sync-lock")]
use crate::SyncPool;

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
/// // 获取锁
/// let lock = RedLock::new(pool, "key", Duration::from_secs(10)).acquire()?;
/// if lock.is_none() {
///     return Err("operation is too frequent, please try again later")
/// }
/// // 手动释放
/// lock.unwrap().release()?;
///
/// // 尝试获取锁（重试3次，间隔100ms）
/// let lock = RedLock::new(pool, "key", Duration::from_secs(10)).try_acquire(3, Duration::from_millis(100))?;
/// if lock.is_none() {
///     return Err("operation is too frequent, please try again later")
/// }
/// // 手动释放
/// lock.unwrap().release()?;
/// ```
#[cfg(feature = "sync-lock")]
pub struct RedLock {
    pool: SyncPool,
    key: String,
    ttl: time::Duration,
    token: Option<String>,
    prevent: bool,
}

#[cfg(feature = "sync-lock")]
impl RedLock {
    pub fn new(pool: SyncPool, key: impl AsRef<str>, ttl: time::Duration) -> Self {
        RedLock {
            pool,
            key: key.as_ref().to_string(),
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

    /// 阻塞式重试获取锁。
    ///
    /// `attempts` 为最大尝试次数；相邻两次尝试间隔 `duration`
    pub fn try_acquire(mut self, attempts: usize, duration: time::Duration) -> anyhow::Result<Option<Self>> {
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

        match &mut self.pool {
            SyncPool::Single(pool) => {
                let mut conn = pool.get()?;
                self.del_conn(&mut conn)
            }
            #[cfg(feature = "cluster")]
            SyncPool::Cluster(pool) => {
                let mut conn = pool.get()?;
                self.del_conn(&mut conn)
            }
        }
    }

    /// 调用 `prevent` 后，Drop 时不会自动释放锁。
    pub fn prevent(&mut self) {
        self.prevent = true;
    }

    fn set_nx(&mut self) -> anyhow::Result<()> {
        match &mut self.pool {
            SyncPool::Single(pool) => {
                let mut conn = pool.get()?;
                self.set_nx_conn(&mut conn)
            }
            #[cfg(feature = "cluster")]
            SyncPool::Cluster(pool) => {
                let mut conn = pool.get()?;
                self.set_nx_conn(&mut conn)
            }
        }
    }

    fn set_nx_conn<C: Commands>(&mut self, conn: &mut C) -> anyhow::Result<()> {
        let new_token = Uuid::new_v4().to_string();

        let opts = redis::SetOptions::default()
            .conditional_set(NX)
            .with_expiration(EX(self.ttl.as_secs().max(1)));
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

    fn del_conn<C: Commands>(&mut self, conn: &mut C) -> anyhow::Result<()> {
        let token = self.token.take().unwrap();
        let _: () = redis::Script::new(DEL).key(&self.key).arg(&token).invoke(conn)?;
        Ok(())
    }
}

/// 自动释放锁。
#[cfg(feature = "sync-lock")]
impl Drop for RedLock {
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
/// // 获取锁
/// let lock = AsyncRedLock::new(pool, "key", Duration::from_secs(10))
///     .acquire()
///     .await?;
/// if lock.is_none() {
///     return Err("operation is too frequent, please try again later")
/// }
/// // 手动释放
/// lock.unwrap().release().await?;
///
/// // 尝试获取锁（重试3次，间隔100ms）
/// let lock = AsyncRedLock::new(pool, "key", Duration::from_secs(10))
///     .try_acquire(3, Duration::from_millis(100))
///     .await?;
/// if lock.is_none() {
///     return Err("operation is too frequent, please try again later")
/// }
/// // 手动释放
/// lock.unwrap().release().await?;
/// ```
pub struct AsyncRedLock {
    pool: AsyncPool,
    key: String,
    ttl: time::Duration,
    token: Option<String>,
    prevent: bool,
}

impl AsyncRedLock {
    pub fn new(pool: AsyncPool, key: impl AsRef<str>, ttl: time::Duration) -> Self {
        AsyncRedLock {
            pool,
            key: key.as_ref().to_string(),
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

    /// 异步重试获取锁。
    ///
    /// `attempts` 为最大尝试次数；相邻两次尝试间隔 `duration`
    pub async fn try_acquire(mut self, attempts: usize, duration: time::Duration) -> anyhow::Result<Option<Self>> {
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

        match self.pool.clone() {
            AsyncPool::Single(pool) => {
                let mut conn = pool.get().await?;
                self.del_conn(&mut *conn).await
            }
            #[cfg(feature = "cluster")]
            AsyncPool::Cluster(pool) => {
                let mut conn = pool.get().await?;
                self.del_conn(&mut *conn).await
            }
        }
    }

    /// 调用 `prevent` 后，Drop 时不会自动释放锁。
    pub fn prevent(&mut self) {
        self.prevent = true;
    }

    async fn set_nx(&mut self) -> anyhow::Result<()> {
        match self.pool.clone() {
            AsyncPool::Single(pool) => {
                let mut conn = pool.get().await?;
                self.set_nx_conn(&mut *conn).await
            }
            #[cfg(feature = "cluster")]
            AsyncPool::Cluster(pool) => {
                let mut conn = pool.get().await?;
                self.set_nx_conn(&mut *conn).await
            }
        }
    }

    async fn set_nx_conn<C: AsyncCommands>(&mut self, conn: &mut C) -> anyhow::Result<()> {
        let new_token = Uuid::new_v4().to_string();

        let opts = redis::SetOptions::default()
            .conditional_set(NX)
            .with_expiration(EX(self.ttl.as_secs().max(1)));
        match conn.set_options(&self.key, &new_token, opts).await {
            Ok(v) => {
                if v {
                    self.token = Some(new_token);
                }
                Ok(())
            }
            Err(e) => {
                // SET 异常时 GET 一次，避免因网络错误误判加锁失败
                let ret_get: Option<String> = conn.get(&self.key).await?;
                let v = ret_get.ok_or(e)?;
                if v == new_token {
                    self.token = Some(new_token);
                }
                Ok(())
            }
        }
    }

    async fn del_conn<C: AsyncCommands>(&mut self, conn: &mut C) -> anyhow::Result<()> {
        let token = self.token.take().unwrap();
        redis::Script::new(DEL)
            .key(&self.key)
            .arg(&token)
            .invoke_async::<()>(conn)
            .await?;
        Ok(())
    }
}

impl Drop for AsyncRedLock {
    fn drop(&mut self) {
        if self.prevent || self.token.is_none() {
            return;
        }

        let pool = self.pool.clone();
        let key = self.key.clone();
        let token = self.token.take().unwrap();

        tokio::spawn(async move {
            if let Err(e) = async {
                let script = redis::Script::new(DEL);
                match pool {
                    AsyncPool::Single(p) => {
                        let mut conn = p.get().await?;
                        script.key(&key).arg(&token).invoke_async::<()>(&mut *conn).await?;
                    }
                    #[cfg(feature = "cluster")]
                    AsyncPool::Cluster(p) => {
                        let mut conn = p.get().await?;
                        script.key(&key).arg(&token).invoke_async::<()>(&mut *conn).await?;
                    }
                }
                Ok::<_, anyhow::Error>(())
            }
            .await
            {
                tracing::error!(err = ?e, "[neon-redis.async_red_lock] drop release(key={}) failed", key);
            }
        });
    }
}

// `AsyncDrop` 稳定后可替换上方 `tokio::spawn` 方案，在 drop 中 await 释放锁。
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

    use crate::client::{self, Cluster, Single};

    use super::*;

    #[cfg(feature = "sync-lock")]
    #[test]
    #[ignore = "requires local Redis at redis://127.0.0.1:6379"]
    fn test_red_lock() {
        let pool = r2d2::Pool::new(redis::Client::open("redis://127.0.0.1:6379").unwrap()).unwrap();
        let lock = RedLock::new(SyncPool::Single(pool), "test_red_lock", Duration::from_secs(10))
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
        let lock = RedLock::new(SyncPool::Cluster(pool), "test_red_lock_cluster", Duration::from_secs(10))
            .acquire()
            .unwrap();
        assert!(lock.is_some());
    }

    #[tokio::test]
    #[ignore = "requires local Redis at redis://127.0.0.1:6379"]
    async fn test_async_red_lock() {
        let pool = client::open::<Single>(vec!["redis://127.0.0.1:6379"], None).await.unwrap();

        {
            let lock = AsyncRedLock::new(AsyncPool::Single(pool), "test_async_red_lock", Duration::from_secs(10))
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
        let pool = client::open::<Cluster>(vec!["redis://127.0.0.1:6379"], None).await.unwrap();

        {
            let lock = AsyncRedLock::new(AsyncPool::Cluster(pool), "test_async_red_lock_cluster", Duration::from_secs(10))
                .acquire()
                .await
                .unwrap();
            assert!(lock.is_some());
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
