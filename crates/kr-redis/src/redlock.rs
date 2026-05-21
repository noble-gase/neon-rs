use redis::{AsyncCommands, Commands, ExistenceCheck::NX, SetExpiry::EX};
use std::{thread, time};
use tokio::time::sleep;
use uuid::Uuid;

use crate::client;

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
/// ```
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
pub struct RedLock {
    pool: r2d2::Pool<redis::Client>,
    key: String,
    ttl: time::Duration,
    token: Option<String>,
    prevent: bool,
}

impl RedLock {
    pub fn new(pool: r2d2::Pool<redis::Client>, key: impl AsRef<str>, ttl: time::Duration) -> Self {
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

    /// 尝试获取锁
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

        let mut conn = self.pool.get()?;
        redis::Script::new(DEL)
            .key(&self.key)
            .arg(&self.token)
            .invoke::<()>(&mut conn)?;
        self.token = None;
        Ok(())
    }

    /// 阻止锁自动释放
    pub fn prevent(&mut self) {
        self.prevent = true;
    }

    fn set_nx(&mut self) -> anyhow::Result<()> {
        let mut conn = self.pool.get()?;

        let token = Uuid::new_v4().to_string();

        let opts = redis::SetOptions::default()
            .conditional_set(NX)
            .with_expiration(EX(self.ttl.as_secs().max(1)));
        let ret_setnx: redis::RedisResult<bool> = conn.set_options(&self.key, &token, opts);
        match ret_setnx {
            Ok(v) => {
                if v {
                    self.token = Some(token);
                }
                Ok(())
            }
            Err(e) => {
                // 异常错误，尝试GET一次：避免因网络错误导致误加锁
                let ret_get: Option<String> = conn.get(&self.key)?;
                let v = ret_get.ok_or(e)?;
                if v == token {
                    self.token = Some(token);
                }
                Ok(())
            }
        }
    }
}

/// 自动释放锁
impl Drop for RedLock {
    fn drop(&mut self) {
        if self.prevent || self.token.is_none() {
            return;
        }

        // 释放锁
        if let Err(e) = self.release() {
            tracing::error!(err = ?e, "[mutex.red_lock] drop release(key={}) failed", self.key);
        }
    }
}

/// 基于Redis的异步分布式锁（离开作用域自动释放）
///
/// # Examples
///
/// ```
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
    pool: client::SinglePool,
    key: String,
    ttl: time::Duration,
    token: Option<String>,
    prevent: bool,
}

impl AsyncRedLock {
    pub fn new(pool: client::SinglePool, key: impl AsRef<str>, ttl: time::Duration) -> Self {
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

    /// 尝试获取锁
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

        let mut conn = self.pool.get().await?;
        redis::Script::new(DEL)
            .key(&self.key)
            .arg(&self.token)
            .invoke_async::<()>(&mut *conn)
            .await?;
        self.token = None;
        Ok(())
    }

    /// 阻止锁自动释放
    pub fn prevent(&mut self) {
        self.prevent = true;
    }

    async fn set_nx(&mut self) -> anyhow::Result<()> {
        let mut conn = self.pool.get().await?;

        let token = Uuid::new_v4().to_string();

        let opts = redis::SetOptions::default()
            .conditional_set(NX)
            .with_expiration(EX(self.ttl.as_secs().max(1)));
        let ret_setnx: redis::RedisResult<bool> = conn.set_options(&self.key, &token, opts).await;
        match ret_setnx {
            Ok(v) => {
                if v {
                    self.token = Some(token);
                }
                Ok(())
            }
            Err(e) => {
                // 异常错误，尝试GET一次：避免因网络错误导致误加锁
                let ret_get: Option<String> = conn.get(&self.key).await?;
                let v = ret_get.ok_or(e)?;
                if v == token {
                    self.token = Some(token);
                }
                Ok(())
            }
        }
    }
}

// 自动释放锁
impl Drop for AsyncRedLock {
    fn drop(&mut self) {
        if self.prevent || self.token.is_none() {
            return;
        }

        let pool = self.pool.clone();
        let key = self.key.clone();
        let token = self.token.clone().unwrap();

        // 异步释放锁
        tokio::spawn(async move {
            if let Err(e) = async {
                let mut conn = pool.get().await?;
                redis::Script::new(DEL)
                    .key(&key)
                    .arg(&token)
                    .invoke_async::<()>(&mut *conn)
                    .await?;
                Ok::<_, anyhow::Error>(())
            }
            .await
            {
                tracing::error!(err = ?e, "[mutex.async_red_lock] drop release(key={}) failed", key);
            }
        });
    }
}

// 自动释放锁
// impl AsyncDrop for AsyncRedLock {
//     fn drop(&mut self) {
//         if self.prevent || self.token.is_none() {
//             return;
//         }

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

    use super::*;

    #[test]
    fn test_red_lock() {
        let pool = r2d2::Pool::new(redis::Client::open("redis://127.0.0.1:6379").unwrap()).unwrap();
        let lock = RedLock::new(pool, "test_red_lock", Duration::from_secs(10)).acquire().unwrap();
        assert!(lock.is_some());
    }

    #[tokio::test]
    async fn test_async_red_lock() {
        let pool = client::open::<client::Single>(vec!["redis://127.0.0.1:6379"], None)
            .await
            .unwrap();

        {
            let lock = AsyncRedLock::new(pool, "test_async_red_lock", Duration::from_secs(10))
                .acquire()
                .await
                .unwrap();
            assert!(lock.is_some());
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
