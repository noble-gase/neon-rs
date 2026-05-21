use std::time::Duration;

use sqlx::{
    Database, MySql, Pool, Postgres, Sqlite, mysql::MySqlPoolOptions, pool::PoolOptions, postgres::PgPoolOptions, sqlite::SqlitePoolOptions,
};

pub trait Factory {
    type DB: Database;

    fn build() -> PoolOptions<Self::DB>;
}

pub struct MySQL;

impl Factory for MySQL {
    type DB = MySql;

    fn build() -> PoolOptions<Self::DB> {
        MySqlPoolOptions::new()
    }
}

pub struct PgSQL;

impl Factory for PgSQL {
    type DB = Postgres;

    fn build() -> PoolOptions<Self::DB> {
        PgPoolOptions::new()
    }
}

pub struct SQLite;

impl Factory for SQLite {
    type DB = Sqlite;

    fn build() -> PoolOptions<Self::DB> {
        SqlitePoolOptions::new()
    }
}

#[derive(Default, Debug)]
pub struct Params {
    pub min_conns: Option<u32>,
    pub max_conns: Option<u32>,
    pub conn_timeout: Option<Duration>,
    pub idle_timeout: Option<Duration>,
    pub max_lifetime: Option<Duration>,
}

/// 生成 DB 连接池
///
/// # Examples
///
/// ```
/// // [MySQL] mysql://<username>:<password>@<host>:3306/<db>&charset=utf8mb4&parseTime=True&loc=Local
/// let x = sql::open::<sql::MySQL>("dsn", None).await;
///
/// // [PgSQL] postgres://<username>:<password>@<host>:5432/<db>?options=-c%20TimeZone%3DAsia/Shanghai
/// let x = sql::open::<sql::PgSQL>("dsn", None).await;
///
/// // [SQLite] sqlite://</path/test.db> || sqlite::memory:?cache=shared
/// let x = sql::open::<sql::SQLite>("dsn", None).await;
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
