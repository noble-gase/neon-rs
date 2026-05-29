use std::{collections::HashMap, future::Future, time::Duration};

use redis::{AsyncCommands, RedisResult};
use serde::{Serialize, de::DeserializeOwned};

use crate::AsyncPool;

/// Hash 字段写入脚本：若 key 无 TTL 则设置过期时间（用于 `hget_or_set`）。
const HSET: &str = r#"
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
if redis.call('TTL', KEYS[1]) == -1 then
    redis.call('EXPIRE', KEYS[1], ARGV[3])
end
"#;

/// 读取 string key；miss 时调用 `loader`，命中后可选写入 Redis
///
/// 值以 JSON 字符串存储并发 miss 时 `loader` 可能被多次调用
/// 回源成功但写缓存失败时仍返回 loader 结果（失败仅记 error 日志）
pub async fn get_or_set<T, F, Fut>(pool: AsyncPool, key: impl AsRef<str>, loader: F, ttl: Option<Duration>) -> anyhow::Result<Option<T>>
where
    T: Serialize + DeserializeOwned + Send + 'static,
    F: FnOnce() -> Fut,
    Fut: Future<Output = anyhow::Result<Option<T>>>,
{
    match pool {
        AsyncPool::Single(pool) => {
            let mut conn = pool.get().await?;
            get_or_set_inner(&mut *conn, key.as_ref(), loader, ttl).await
        }
        #[cfg(feature = "cluster")]
        AsyncPool::Cluster(pool) => {
            let mut conn = pool.get().await?;
            get_or_set_inner(&mut *conn, key.as_ref(), loader, ttl).await
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
    // 缓存命中
    let ret_get: Option<String> = conn.get(key).await?;
    if let Some(v) = ret_get {
        let parsed = serde_json::from_str(&v)?;
        return Ok(parsed);
    }

    let data = loader().await?;

    if let Some(v) = &data {
        let json_str = serde_json::to_string(&v)?;
        let set_ret: RedisResult<()> = match ttl {
            Some(d) => conn.set_ex(key, &json_str, d.as_secs()).await,
            None => conn.set(key, &json_str).await,
        };
        if let Err(e) = set_ret {
            tracing::error!(error = ?e, key = key, data = json_str, "[redkit::get_or_set] set data failed")
        }
    }

    Ok(data)
}

/// 读取 hash field；miss 时调用 `loader`，命中后可选写入 Redis
///
/// 指定 `ttl` 时，若 hash key 尚无 TTL 会通过 Lua 脚本补设过期时间
/// 回源成功但写缓存失败时仍返回 loader 结果（失败仅记 error 日志）
pub async fn hget_or_set<T, F, Fut>(
    pool: AsyncPool, key: impl AsRef<str>, field: impl AsRef<str>, loader: F, ttl: Option<Duration>,
) -> anyhow::Result<Option<T>>
where
    T: Serialize + DeserializeOwned + Send + 'static,
    F: FnOnce() -> Fut,
    Fut: Future<Output = anyhow::Result<Option<T>>>,
{
    match pool {
        AsyncPool::Single(pool) => {
            let mut conn = pool.get().await?;
            hget_or_set_inner(&mut *conn, key.as_ref(), field.as_ref(), loader, ttl).await
        }
        #[cfg(feature = "cluster")]
        AsyncPool::Cluster(pool) => {
            let mut conn = pool.get().await?;
            hget_or_set_inner(&mut *conn, key.as_ref(), field.as_ref(), loader, ttl).await
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
    // 缓存命中
    let ret_get: Option<String> = conn.hget(key, field).await?;
    if let Some(v) = ret_get {
        let parsed = serde_json::from_str(&v)?;
        return Ok(parsed);
    }

    let data = loader().await?;

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
            tracing::error!(error = ?e, key = key, data = json_str, "[redkit::hget_or_set] set data failed")
        }
    }

    Ok(data)
}

/// `MGET` 并将存在的 key 反序列化为 `HashMap<key, T>`；不存在的 key 被跳过
///
/// # Redis Cluster 限制
///
/// Cluster 下 `MGET` 要求所有 key 落在同一 hash slot，否则会返回 `CROSSSLOT` 错误
/// 批量读取时需为 key 使用相同 hash tag，例如 `{user}:1`、`{user}:2`
pub async fn mget_map<K, T>(pool: AsyncPool, keys: &[K]) -> anyhow::Result<HashMap<String, T>>
where
    K: AsRef<str> + Sync,
    T: Serialize + DeserializeOwned,
{
    match pool {
        AsyncPool::Single(pool) => {
            let mut conn = pool.get().await?;
            mget_map_inner(&mut *conn, keys).await
        }
        #[cfg(feature = "cluster")]
        AsyncPool::Cluster(pool) => {
            let mut conn = pool.get().await?;
            mget_map_inner(&mut *conn, keys).await
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
    for (k, v) in keys.iter().zip(raw) {
        if let Some(s) = v {
            map.insert(k.as_ref().to_string(), serde_json::from_str(&s)?);
        }
    }
    Ok(map)
}

/// `MGET` 并将存在的 key 收集为 `HashMap<key, String>`
///
/// # Redis Cluster 限制
///
/// Cluster 下 `MGET` 要求所有 key 落在同一 hash slot，否则会返回 `CROSSSLOT` 错误
/// 批量读取时需为 key 使用相同 hash tag，例如 `{user}:1`、`{user}:2`
pub async fn mget_str_map<K>(pool: AsyncPool, keys: &[K]) -> anyhow::Result<HashMap<String, String>>
where
    K: AsRef<str> + Sync,
{
    match pool {
        AsyncPool::Single(pool) => {
            let mut conn = pool.get().await?;
            mget_str_map_inner(&mut *conn, keys).await
        }
        #[cfg(feature = "cluster")]
        AsyncPool::Cluster(pool) => {
            let mut conn = pool.get().await?;
            mget_str_map_inner(&mut *conn, keys).await
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
    for (k, v) in keys.iter().zip(raw) {
        if let Some(s) = v {
            map.insert(k.as_ref().to_string(), s);
        }
    }
    Ok(map)
}

/// `HGETALL` 并将各 field 反序列化为 `HashMap<field, T>`
pub async fn hgetall<T>(pool: AsyncPool, key: impl AsRef<str>) -> anyhow::Result<HashMap<String, T>>
where
    T: Serialize + DeserializeOwned,
{
    match pool {
        AsyncPool::Single(pool) => {
            let mut conn = pool.get().await?;
            hgetall_inner(&mut *conn, key.as_ref()).await
        }
        #[cfg(feature = "cluster")]
        AsyncPool::Cluster(pool) => {
            let mut conn = pool.get().await?;
            hgetall_inner(&mut *conn, key.as_ref()).await
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

/// `HMGET` 并将存在的 field 反序列化为 `HashMap<field, T>`
pub async fn hmget_map<K, T>(pool: AsyncPool, key: K, fields: &[K]) -> anyhow::Result<HashMap<String, T>>
where
    K: AsRef<str> + Sync,
    T: Serialize + DeserializeOwned,
{
    match pool {
        AsyncPool::Single(pool) => {
            let mut conn = pool.get().await?;
            hmget_map_inner(&mut *conn, key.as_ref(), fields).await
        }
        #[cfg(feature = "cluster")]
        AsyncPool::Cluster(pool) => {
            let mut conn = pool.get().await?;
            hmget_map_inner(&mut *conn, key.as_ref(), fields).await
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
    for (k, v) in fields.iter().zip(raw) {
        if let Some(s) = v {
            map.insert(k.as_ref().to_string(), serde_json::from_str(&s)?);
        }
    }
    Ok(map)
}

/// `HMGET` 并将存在的 field 收集为 `HashMap<field, String>`
pub async fn hmget_str_map<K>(pool: AsyncPool, key: K, fields: &[K]) -> anyhow::Result<HashMap<String, String>>
where
    K: AsRef<str> + Sync,
{
    match pool {
        AsyncPool::Single(pool) => {
            let mut conn = pool.get().await?;
            hmget_str_map_inner(&mut *conn, key.as_ref(), fields).await
        }
        #[cfg(feature = "cluster")]
        AsyncPool::Cluster(pool) => {
            let mut conn = pool.get().await?;
            hmget_str_map_inner(&mut *conn, key.as_ref(), fields).await
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
    for (k, v) in fields.iter().zip(raw) {
        if let Some(s) = v {
            map.insert(k.as_ref().to_string(), s);
        }
    }
    Ok(map)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use anyhow::Ok;
    use serde::{Deserialize, Serialize};
    use serde_json::json;

    use crate::{
        factory::{Cluster, Single},
        open,
    };

    use super::*;

    #[derive(Debug, Deserialize, Serialize)]
    struct Demo {
        id: i64,
        name: String,
    }

    #[tokio::test]
    #[ignore = "requires local Redis at redis://127.0.0.1:6379"]
    async fn test_get_or_set() {
        let pool = open::<Single>(vec!["redis://127.0.0.1:6379"], None).await.unwrap();

        let ret = get_or_set(
            AsyncPool::Single(pool.clone()),
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
    #[ignore = "requires local Redis at redis://127.0.0.1:6379"]
    async fn test_hget_or_set() {
        let pool = open::<Single>(vec!["redis://127.0.0.1:6379"], None).await.unwrap();

        let ret = hget_or_set(
            AsyncPool::Single(pool.clone()),
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
    #[ignore = "requires local Redis at redis://127.0.0.1:6379"]
    async fn test_mget_map() {
        let pool = open::<Single>(vec!["redis://127.0.0.1:6379"], None).await.unwrap();

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

        let ret: HashMap<String, Demo> = mget_map(AsyncPool::Single(pool.clone()), &["foo", "bar", "hello", "none"])
            .await
            .unwrap();
        println!(">> {:#?}", ret);

        let _: RedisResult<()> = pool.get().await.unwrap().del(&["foo", "bar", "hello"]).await;
    }

    #[tokio::test]
    #[ignore = "requires local Redis at redis://127.0.0.1:6379"]
    async fn test_mget_str_map() {
        let pool = open::<Single>(vec!["redis://127.0.0.1:6379"], None).await.unwrap();

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

        let ret: HashMap<String, String> = mget_str_map(AsyncPool::Single(pool.clone()), &["foo", "bar", "hello", "none"])
            .await
            .unwrap();
        println!(">> {:#?}", ret);

        let _: RedisResult<()> = pool.get().await.unwrap().del(&["foo", "bar", "hello"]).await;
    }

    #[tokio::test]
    #[ignore = "requires local Redis at redis://127.0.0.1:6379"]
    async fn test_hgetall() {
        let pool = open::<Single>(vec!["redis://127.0.0.1:6379"], None).await.unwrap();

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

        let ret: HashMap<String, Demo> = hgetall(AsyncPool::Single(pool.clone()), "test").await.unwrap();
        println!(">> {:#?}", ret);

        let _: RedisResult<()> = pool.get().await.unwrap().del("test").await;
    }

    #[tokio::test]
    #[ignore = "requires local Redis at redis://127.0.0.1:6379"]
    async fn test_hmget_map() {
        let pool = open::<Single>(vec!["redis://127.0.0.1:6379"], None).await.unwrap();

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

        let ret: HashMap<String, Demo> = hmget_map(AsyncPool::Single(pool.clone()), "test", &["foo", "bar", "hello", "none"])
            .await
            .unwrap();
        println!(">> {:#?}", ret);

        let _: RedisResult<()> = pool.get().await.unwrap().del("test").await;
    }

    #[tokio::test]
    #[ignore = "requires local Redis at redis://127.0.0.1:6379"]
    async fn test_hmget_str_map() {
        let pool = open::<Single>(vec!["redis://127.0.0.1:6379"], None).await.unwrap();

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

        let ret: HashMap<String, String> = hmget_str_map(AsyncPool::Single(pool.clone()), "test", &["foo", "bar", "hello", "none"])
            .await
            .unwrap();
        println!(">> {:#?}", ret);

        let _: RedisResult<()> = pool.get().await.unwrap().del("test").await;
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    #[ignore = "requires local Redis cluster"]
    async fn test_get_or_set_cluster() {
        let pool = open::<Cluster>(vec!["redis://127.0.0.1:6379"], None).await.unwrap();

        let ret = get_or_set(
            AsyncPool::Cluster(pool.clone()),
            "redkit:cluster:get_or_set",
            || async {
                Ok(Some(Demo {
                    id: 1,
                    name: "cluster".to_string(),
                }))
            },
            Some(Duration::from_secs(60)),
        )
        .await
        .unwrap();
        assert_eq!(ret.as_ref().map(|d| d.name.as_str()), Some("cluster"));

        let _: RedisResult<()> = pool.get().await.unwrap().del("redkit:cluster:get_or_set").await;
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    #[ignore = "requires local Redis cluster"]
    async fn test_hget_or_set_cluster() {
        let pool = open::<Cluster>(vec!["redis://127.0.0.1:6379"], None).await.unwrap();

        let ret = hget_or_set(
            AsyncPool::Cluster(pool.clone()),
            "redkit:cluster:hash",
            "field",
            || async {
                Ok(Some(Demo {
                    id: 2,
                    name: "cluster".to_string(),
                }))
            },
            Some(Duration::from_secs(60)),
        )
        .await
        .unwrap();
        assert_eq!(ret.as_ref().map(|d| d.id), Some(2));

        let _: RedisResult<()> = pool.get().await.unwrap().del("redkit:cluster:hash").await;
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    #[ignore = "requires local Redis cluster"]
    async fn test_mget_map_cluster() {
        let pool = open::<Cluster>(vec!["redis://127.0.0.1:6379"], None).await.unwrap();

        let _: RedisResult<()> = pool
            .get()
            .await
            .unwrap()
            .mset(&[
                ("{redkit}:foo", json!({"id":1,"name":"foo"}).to_string()),
                ("{redkit}:bar", json!({"id":2,"name":"bar"}).to_string()),
                ("{redkit}:hello", json!({"id":3,"name":"hello"}).to_string()),
            ])
            .await;

        let ret: HashMap<String, Demo> = mget_map(
            AsyncPool::Cluster(pool.clone()),
            &["{redkit}:foo", "{redkit}:bar", "{redkit}:hello", "{redkit}:none"],
        )
        .await
        .unwrap();
        assert_eq!(ret.len(), 3);

        let _: RedisResult<()> = pool
            .get()
            .await
            .unwrap()
            .del(&["{redkit}:foo", "{redkit}:bar", "{redkit}:hello"])
            .await;
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    #[ignore = "requires local Redis cluster"]
    async fn test_hgetall_cluster() {
        let pool = open::<Cluster>(vec!["redis://127.0.0.1:6379"], None).await.unwrap();

        let _: RedisResult<()> = pool
            .get()
            .await
            .unwrap()
            .hset_multiple(
                "redkit:cluster:hgetall",
                &[("foo", json!({"id":1,"name":"foo"}).to_string()), ("bar", json!({"id":2,"name":"bar"}).to_string())],
            )
            .await;

        let ret: HashMap<String, Demo> = hgetall(AsyncPool::Cluster(pool.clone()), "redkit:cluster:hgetall")
            .await
            .unwrap();
        assert_eq!(ret.len(), 2);

        let _: RedisResult<()> = pool.get().await.unwrap().del("redkit:cluster:hgetall").await;
    }
}
