use std::{collections::HashMap, future::Future, time::Duration};

use redis::{AsyncCommands, RedisResult};
use serde::{Serialize, de::DeserializeOwned};

use crate::client;

const HSET: &str = r#"
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
if redis.call('TTL', KEYS[1]) == -1 then
    redis.call('EXPIRE', KEYS[1], ARGV[3])
end
"#;

pub enum Redis {
    Single(client::SinglePool),
    Cluster(client::ClusterPool),
}

impl Redis {
    pub async fn get_or_set<T, F, Fut>(&self, key: impl AsRef<str>, loader: F, ttl: Option<Duration>) -> anyhow::Result<Option<T>>
    where
        T: Serialize + DeserializeOwned + Send + 'static,
        F: FnOnce() -> Fut,
        Fut: Future<Output = anyhow::Result<Option<T>>>,
    {
        match self {
            Redis::Single(pool) => {
                let mut conn = pool.get().await?;
                Self::get_or_set_inner(&mut *conn, key.as_ref(), loader, ttl).await
            }
            Redis::Cluster(pool) => {
                let mut conn = pool.get().await?;
                Self::get_or_set_inner(&mut *conn, key.as_ref(), loader, ttl).await
            }
        }
    }

    async fn get_or_set_inner<C, T, F, Fut>(conn: &mut C, key: &str, loader: F, ttl: Option<Duration>) -> anyhow::Result<Option<T>>
    where
        C: AsyncCommands,
        T: Serialize + DeserializeOwned + Send + 'static,
        F: FnOnce() -> Fut,
        Fut: Future<Output = anyhow::Result<Option<T>>>,
    {
        // 从缓存读取
        let ret_get: Option<String> = conn.get(key).await?;
        if let Some(v) = ret_get {
            let parsed = serde_json::from_str(&v)?;
            return Ok(parsed);
        }

        // 缓存未命中，调用loader获取数据
        let data = loader().await?;

        // 数据存在，写入缓存
        if let Some(v) = &data {
            let json_str = serde_json::to_string(&v)?;
            let set_ret: RedisResult<()> = match ttl {
                Some(d) => conn.set_ex(key, &json_str, d.as_secs()).await,
                None => conn.set(key, &json_str).await,
            };
            if let Err(e) = set_ret {
                tracing::error!(error = ?e, key = key, data = json_str, "[cache::get_or_set] set data failed")
            }
        }

        Ok(data)
    }

    pub async fn hget_or_set<T, F, Fut>(
        &self, key: impl AsRef<str>, field: impl AsRef<str>, loader: F, ttl: Option<Duration>,
    ) -> anyhow::Result<Option<T>>
    where
        T: Serialize + DeserializeOwned + Send + 'static,
        F: FnOnce() -> Fut,
        Fut: Future<Output = anyhow::Result<Option<T>>>,
    {
        match self {
            Redis::Single(pool) => {
                let mut conn = pool.get().await?;
                Self::hget_or_set_inner(&mut *conn, key.as_ref(), field.as_ref(), loader, ttl).await
            }
            Redis::Cluster(pool) => {
                let mut conn = pool.get().await?;
                Self::hget_or_set_inner(&mut *conn, key.as_ref(), field.as_ref(), loader, ttl).await
            }
        }
    }

    async fn hget_or_set_inner<C, T, F, Fut>(
        conn: &mut C, key: &str, field: &str, loader: F, ttl: Option<Duration>,
    ) -> anyhow::Result<Option<T>>
    where
        C: AsyncCommands,
        T: Serialize + DeserializeOwned + Send + 'static,
        F: FnOnce() -> Fut,
        Fut: Future<Output = anyhow::Result<Option<T>>>,
    {
        // 从缓存读取
        let ret_get: Option<String> = conn.hget(key, field).await?;
        if let Some(v) = ret_get {
            let parsed = serde_json::from_str(&v)?;
            return Ok(parsed);
        }

        // 缓存未命中，调用loader获取数据
        let data = loader().await?;

        // 数据存在，写入缓存
        if let Some(v) = &data {
            let json_str = serde_json::to_string(&v)?;
            let set_ret: RedisResult<()> = match ttl {
                Some(d) => {
                    redis::Script::new(HSET)
                        .key(key)
                        .arg(field)
                        .arg(&json_str)
                        .arg(d.as_secs() as i64)
                        .invoke_async(&mut *conn)
                        .await
                }
                None => conn.hset(key, field, &json_str).await,
            };
            if let Err(e) = set_ret {
                tracing::error!(error = ?e, key = key, data = json_str, "[cache::hget_or_hset] set data failed")
            }
        }

        Ok(data)
    }

    pub async fn mget_map<K, T>(&self, keys: &[K]) -> anyhow::Result<HashMap<String, T>>
    where
        K: AsRef<str> + Sync,
        T: Serialize + DeserializeOwned,
    {
        match self {
            Redis::Single(pool) => {
                let mut conn = pool.get().await?;
                Self::mget_map_inner(&mut *conn, keys).await
            }
            Redis::Cluster(pool) => {
                let mut conn = pool.get().await?;
                Self::mget_map_inner(&mut *conn, keys).await
            }
        }
    }

    async fn mget_map_inner<C, K, T>(conn: &mut C, keys: &[K]) -> anyhow::Result<HashMap<String, T>>
    where
        C: AsyncCommands,
        K: AsRef<str> + Sync,
        T: Serialize + DeserializeOwned,
    {
        let key_vec: Vec<&str> = keys.iter().map(|k| k.as_ref()).collect();
        let raw: Vec<Option<String>> = conn.mget(key_vec).await?;

        let mut map = HashMap::with_capacity(keys.len());
        for (k, v) in keys.iter().zip(raw.into_iter()) {
            if let Some(s) = v {
                map.insert(k.as_ref().to_string(), serde_json::from_str(&s)?);
            }
        }
        Ok(map)
    }

    pub async fn mget_str_map<K>(&self, keys: &[K]) -> anyhow::Result<HashMap<String, String>>
    where
        K: AsRef<str> + Sync,
    {
        match self {
            Redis::Single(pool) => {
                let mut conn = pool.get().await?;
                Self::mget_str_map_inner(&mut *conn, keys).await
            }
            Redis::Cluster(pool) => {
                let mut conn = pool.get().await?;
                Self::mget_str_map_inner(&mut *conn, keys).await
            }
        }
    }

    async fn mget_str_map_inner<C, K>(conn: &mut C, keys: &[K]) -> anyhow::Result<HashMap<String, String>>
    where
        C: AsyncCommands,
        K: AsRef<str> + Sync,
    {
        let key_vec: Vec<&str> = keys.iter().map(|k| k.as_ref()).collect();
        let raw: Vec<Option<String>> = conn.mget(key_vec).await?;

        let mut map = HashMap::with_capacity(keys.len());
        for (k, v) in keys.iter().zip(raw.into_iter()) {
            if let Some(s) = v {
                map.insert(k.as_ref().to_string(), s);
            }
        }
        Ok(map)
    }

    pub async fn hgetall<T>(&self, key: impl AsRef<str>) -> anyhow::Result<HashMap<String, T>>
    where
        T: Serialize + DeserializeOwned,
    {
        match self {
            Redis::Single(pool) => {
                let mut conn = pool.get().await?;
                Self::hgetall_inner(&mut *conn, key.as_ref()).await
            }
            Redis::Cluster(pool) => {
                let mut conn = pool.get().await?;
                Self::hgetall_inner(&mut *conn, key.as_ref()).await
            }
        }
    }

    async fn hgetall_inner<C, T>(conn: &mut C, key: &str) -> anyhow::Result<HashMap<String, T>>
    where
        C: AsyncCommands,
        T: Serialize + DeserializeOwned,
    {
        let raw: HashMap<String, String> = conn.hgetall(key).await?;

        let mut map = HashMap::with_capacity(raw.len());
        for (k, v) in raw {
            let parsed = serde_json::from_str(&v)?;
            map.insert(k, parsed);
        }
        Ok(map)
    }

    pub async fn hmget_map<K, T>(&self, key: K, fields: &[K]) -> anyhow::Result<HashMap<String, T>>
    where
        K: AsRef<str> + Sync,
        T: Serialize + DeserializeOwned,
    {
        match self {
            Redis::Single(pool) => {
                let mut conn = pool.get().await?;
                Self::hmget_map_inner(&mut *conn, key.as_ref(), fields).await
            }
            Redis::Cluster(pool) => {
                let mut conn = pool.get().await?;
                Self::hmget_map_inner(&mut *conn, key.as_ref(), fields).await
            }
        }
    }

    async fn hmget_map_inner<C, K, T>(conn: &mut C, key: &str, fields: &[K]) -> anyhow::Result<HashMap<String, T>>
    where
        C: AsyncCommands,
        K: AsRef<str> + Sync,
        T: Serialize + DeserializeOwned,
    {
        let field_vec: Vec<&str> = fields.iter().map(|k| k.as_ref()).collect();
        let raw: Vec<Option<String>> = conn.hmget(key, field_vec).await?;

        let mut map = HashMap::with_capacity(fields.len());
        for (k, v) in fields.iter().zip(raw.into_iter()) {
            if let Some(s) = v {
                map.insert(k.as_ref().to_string(), serde_json::from_str(&s)?);
            }
        }
        Ok(map)
    }

    pub async fn hmget_str_map<K>(&self, key: K, fields: &[K]) -> anyhow::Result<HashMap<String, String>>
    where
        K: AsRef<str> + Sync,
    {
        match self {
            Redis::Single(pool) => {
                let mut conn = pool.get().await?;
                Self::hmget_str_map_inner(&mut *conn, key.as_ref(), fields).await
            }
            Redis::Cluster(pool) => {
                let mut conn = pool.get().await?;
                Self::hmget_str_map_inner(&mut *conn, key.as_ref(), fields).await
            }
        }
    }

    async fn hmget_str_map_inner<C, K>(conn: &mut C, key: &str, fields: &[K]) -> anyhow::Result<HashMap<String, String>>
    where
        C: AsyncCommands,
        K: AsRef<str> + Sync,
    {
        let field_vec: Vec<&str> = fields.iter().map(|k| k.as_ref()).collect();
        let raw: Vec<Option<String>> = conn.hmget(key, field_vec).await?;

        let mut map = HashMap::with_capacity(fields.len());
        for (k, v) in fields.iter().zip(raw.into_iter()) {
            if let Some(s) = v {
                map.insert(k.as_ref().to_string(), s);
            }
        }
        Ok(map)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use anyhow::Ok;
    use serde::Deserialize;
    use serde_json::json;

    use super::*;

    #[derive(Debug, Deserialize, Serialize)]
    struct Demo {
        id: i64,
        name: String,
    }

    #[tokio::test]
    async fn test_get_or_set() {
        let pool = client::open::<client::Single>(vec!["redis://127.0.0.1:6379"], None)
            .await
            .unwrap();

        let ret = Redis::Single(pool.clone())
            .get_or_set(
                "hello",
                || async {
                    println!(">> call loader");
                    Ok(Some(Demo {
                        id: 1,
                        name: "hello".to_string(),
                    }))
                },
                Some(Duration::from_secs(60)),
            )
            .await
            .unwrap();
        println!(">> {:#?}", ret);

        let s: String = pool.get().await.unwrap().get("hello").await.unwrap();
        println!(">> {}", s);

        let _: RedisResult<()> = pool.get().await.unwrap().del("hello").await;
    }

    #[tokio::test]
    async fn test_hget_or_set() {
        let pool = client::open::<client::Single>(vec!["redis://127.0.0.1:6379"], None)
            .await
            .unwrap();

        let ret = Redis::Single(pool.clone())
            .hget_or_set(
                "foo",
                "bar",
                || async {
                    println!(">> call loader");
                    Ok(Some(Demo {
                        id: 1,
                        name: "hello".to_string(),
                    }))
                },
                Some(Duration::from_secs(60)),
            )
            .await
            .unwrap();
        println!(">> {:#?}", ret);

        let s: String = pool.get().await.unwrap().hget("foo", "bar").await.unwrap();
        println!(">> {}", s);

        let _: RedisResult<()> = pool.get().await.unwrap().del("foo").await;
    }

    #[tokio::test]
    async fn test_mget_map() {
        let pool = client::open::<client::Single>(vec!["redis://127.0.0.1:6379"], None)
            .await
            .unwrap();

        let _: RedisResult<()> = pool
            .get()
            .await
            .unwrap()
            .mset(&[
                ("foo", json!({"id":1,"name":"foo"}).to_string()),
                ("bar", json!({"id":2,"name":"bar"}).to_string()),
                ("hello", json!({"id":3,"name":"hello"}).to_string()),
            ])
            .await;

        let ret: HashMap<String, Demo> = Redis::Single(pool.clone())
            .mget_map(&["foo", "bar", "hello", "none"])
            .await
            .unwrap();
        println!(">> {:#?}", ret);

        let _: RedisResult<()> = pool.get().await.unwrap().del(&["foo", "bar", "hello"]).await;
    }

    #[tokio::test]
    async fn test_mget_str_map() {
        let pool = client::open::<client::Single>(vec!["redis://127.0.0.1:6379"], None)
            .await
            .unwrap();

        let _: RedisResult<()> = pool
            .get()
            .await
            .unwrap()
            .mset(&[
                ("foo", json!({"id":1,"name":"foo"}).to_string()),
                ("bar", json!({"id":2,"name":"bar"}).to_string()),
                ("hello", json!({"id":3,"name":"hello"}).to_string()),
            ])
            .await;

        let ret: HashMap<String, String> = Redis::Single(pool.clone())
            .mget_str_map(&["foo", "bar", "hello", "none"])
            .await
            .unwrap();
        println!(">> {:#?}", ret);

        let _: RedisResult<()> = pool.get().await.unwrap().del(&["foo", "bar", "hello"]).await;
    }

    #[tokio::test]
    async fn test_hgetall() {
        let pool = client::open::<client::Single>(vec!["redis://127.0.0.1:6379"], None)
            .await
            .unwrap();

        let _: RedisResult<()> = pool
            .get()
            .await
            .unwrap()
            .hset_multiple(
                "test",
                &[
                    ("foo", json!({"id":1,"name":"foo"}).to_string()),
                    ("bar", json!({"id":2,"name":"bar"}).to_string()),
                    ("hello", json!({"id":3,"name":"hello"}).to_string()),
                ],
            )
            .await;

        let ret: HashMap<String, Demo> = Redis::Single(pool.clone()).hgetall("test").await.unwrap();
        println!(">> {:#?}", ret);

        let _: RedisResult<()> = pool.get().await.unwrap().del("test").await;
    }

    #[tokio::test]
    async fn test_hmget_map() {
        let pool = client::open::<client::Single>(vec!["redis://127.0.0.1:6379"], None)
            .await
            .unwrap();

        let _: RedisResult<()> = pool
            .get()
            .await
            .unwrap()
            .hset_multiple(
                "test",
                &[
                    ("foo", json!({"id":1,"name":"foo"}).to_string()),
                    ("bar", json!({"id":2,"name":"bar"}).to_string()),
                    ("hello", json!({"id":3,"name":"hello"}).to_string()),
                ],
            )
            .await;

        let ret: HashMap<String, Demo> = Redis::Single(pool.clone())
            .hmget_map("test", &["foo", "bar", "hello", "none"])
            .await
            .unwrap();
        println!(">> {:#?}", ret);

        let _: RedisResult<()> = pool.get().await.unwrap().del("test").await;
    }

    #[tokio::test]
    async fn test_hmget_str_map() {
        let pool = client::open::<client::Single>(vec!["redis://127.0.0.1:6379"], None)
            .await
            .unwrap();

        let _: RedisResult<()> = pool
            .get()
            .await
            .unwrap()
            .hset_multiple(
                "test",
                &[
                    ("foo", json!({"id":1,"name":"foo"}).to_string()),
                    ("bar", json!({"id":2,"name":"bar"}).to_string()),
                    ("hello", json!({"id":3,"name":"hello"}).to_string()),
                ],
            )
            .await;

        let ret: HashMap<String, String> = Redis::Single(pool.clone())
            .hmget_str_map("test", &["foo", "bar", "hello", "none"])
            .await
            .unwrap();
        println!(">> {:#?}", ret);

        let _: RedisResult<()> = pool.get().await.unwrap().del("test").await;
    }
}
