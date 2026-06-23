//! SQL 连接池（按后端 feature 启用 [`Factory`] 实现）

use std::{sync::OnceLock, time::Duration};

use sqlx::{Database, pool::PoolOptions};

#[cfg(feature = "mysql")]
use sqlx::{MySql, mysql::MySqlPoolOptions};

#[cfg(feature = "postgres")]
use sqlx::{Postgres, postgres::PgPoolOptions};

#[cfg(feature = "sqlite")]
use sqlx::{Sqlite, sqlite::SqlitePoolOptions};

#[cfg(any(feature = "mysql", feature = "postgres", feature = "sqlite"))]
use crate::{InsertResult, is_unique_violation};

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

// --------- SQL Trace ---------

type Logger = Box<dyn Fn(String, Duration, Option<&anyhow::Error>) + Send + Sync + 'static>;

pub(crate) static SQL_LOGGER: OnceLock<Logger> = OnceLock::new();

#[cfg(any(feature = "mysql", feature = "postgres", feature = "sqlite"))]
#[inline]
fn trace_sql(sql: String, cost: Duration, err: Option<&anyhow::Error>) {
    if let Some(logger) = SQL_LOGGER.get() {
        logger(sql, cost, err)
    }
}

/// 包装 insert 结果；唯一约束冲突时返回 [`InsertResult::Duplicate`]
#[cfg(any(feature = "mysql", feature = "postgres", feature = "sqlite"))]
pub(crate) fn trace_insert_result<T, R, F>(
    sql: String,
    cost: Duration,
    ret: Result<R, sqlx::Error>,
    map_ok: F,
) -> anyhow::Result<InsertResult<T>>
where
    F: FnOnce(R) -> T,
{
    match ret {
        Ok(v) => {
            trace_sql(sql, cost, None);
            Ok(InsertResult::Inserted(map_ok(v)))
        }
        Err(e) => {
            if is_unique_violation(&e) {
                trace_sql(sql, cost, None);
                Ok(InsertResult::Duplicate)
            } else {
                let err = anyhow::Error::from(e);
                trace_sql(sql, cost, Some(&err));
                Err(err)
            }
        }
    }
}

/// 包装 `execute` 结果（如 `rows_affected`）并触发 SQL 日志
#[cfg(any(feature = "mysql", feature = "postgres", feature = "sqlite"))]
pub(crate) fn trace_execute_result<R, F>(
    sql: String,
    cost: Duration,
    ret: Result<R, sqlx::Error>,
    map_ok: F,
) -> anyhow::Result<u64>
where
    F: FnOnce(R) -> u64,
{
    match ret {
        Ok(v) => {
            trace_sql(sql, cost, None);
            Ok(map_ok(v))
        }
        Err(e) => {
            let err = anyhow::Error::from(e);
            trace_sql(sql, cost, Some(&err));
            Err(err)
        }
    }
}

/// 包装查询类 SQL 结果并触发 [`set_sql_logger`] 回调
#[cfg(any(feature = "mysql", feature = "postgres", feature = "sqlite"))]
pub(crate) fn trace_query_result<T>(
    sql: String,
    cost: Duration,
    ret: Result<T, sqlx::Error>,
) -> anyhow::Result<T> {
    match ret {
        Ok(v) => {
            trace_sql(sql, cost, None);
            Ok(v)
        }
        Err(e) => {
            let err = anyhow::Error::from(e);
            trace_sql(sql, cost, Some(&err));
            Err(err)
        }
    }
}
