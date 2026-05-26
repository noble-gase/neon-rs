//! SQL 连接池（按后端 feature 启用 [`Factory`] 实现）。

use std::time::Duration;

use sqlx::{Database, Pool, pool::PoolOptions};

#[cfg(feature = "mysql")]
use sqlx::{MySql, mysql::MySqlPoolOptions};

#[cfg(feature = "postgres")]
use sqlx::{Postgres, postgres::PgPoolOptions};

#[cfg(feature = "sqlite")]
use sqlx::{Sqlite, sqlite::SqlitePoolOptions};

/// 连接池工厂：返回对应后端的 `PoolOptions`
pub trait Factory {
    type DB: Database;

    fn build() -> PoolOptions<Self::DB>;
}

#[cfg(feature = "mysql")]
/// MySQL 后端
pub struct MySQL;

#[cfg(feature = "mysql")]
impl Factory for MySQL {
    type DB = MySql;

    fn build() -> PoolOptions<Self::DB> {
        MySqlPoolOptions::new()
    }
}

#[cfg(feature = "postgres")]
/// PostgreSQL 后端
pub struct PgSQL;

#[cfg(feature = "postgres")]
impl Factory for PgSQL {
    type DB = Postgres;

    fn build() -> PoolOptions<Self::DB> {
        PgPoolOptions::new()
    }
}

#[cfg(feature = "sqlite")]
/// SQLite 后端
pub struct SQLite;

#[cfg(feature = "sqlite")]
impl Factory for SQLite {
    type DB = Sqlite;

    fn build() -> PoolOptions<Self::DB> {
        SqlitePoolOptions::new()
    }
}

/// 连接池参数；未设置的项使用库内默认值
#[derive(Default, Debug)]
pub struct Params {
    /// 最小连接数，默认 10
    pub min_conns: Option<u32>,
    /// 最大连接数，默认 20
    pub max_conns: Option<u32>,
    /// 获取连接超时，默认 10 秒
    pub conn_timeout: Option<Duration>,
    /// 空闲连接回收时间，默认 300 秒
    pub idle_timeout: Option<Duration>,
    /// 连接最大存活时间，默认 600 秒
    pub max_lifetime: Option<Duration>,
}

/// 创建数据库连接池
///
/// # Examples
///
/// ```ignore
/// // MySQL: mysql://user:pass@host:3306/db?charset=utf8mb4
/// let pool = open::<MySQL>("dsn".into(), None).await?;
///
/// // PostgreSQL: postgres://user:pass@host:5432/db
/// let pool = open::<PgSQL>("dsn".into(), None).await?;
///
/// // SQLite: sqlite:///path/to.db 或 sqlite::memory:
/// let pool = open::<SQLite>("dsn".into(), None).await?;
/// ```
pub async fn open<F>(dsn: String, opt: Option<Params>) -> anyhow::Result<Pool<F::DB>>
where
    F: Factory,
{
    let params = opt.unwrap_or_default();

    let pool = F::build()
        .min_connections(params.min_conns.unwrap_or(10))
        .max_connections(params.max_conns.unwrap_or(20))
        .acquire_timeout(params.conn_timeout.unwrap_or(Duration::from_secs(10)))
        .idle_timeout(params.idle_timeout.unwrap_or(Duration::from_secs(300)))
        .max_lifetime(params.max_lifetime.unwrap_or(Duration::from_secs(600)))
        .connect(&dsn)
        .await?;

    Ok(pool)
}
