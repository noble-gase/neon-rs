//! SQL 工具集：连接池与基于 sea-query 的 CRUD helper（按后端 feature 启用）。

pub mod pool;

#[cfg(feature = "mysql")]
pub mod mysql;

#[cfg(feature = "postgres")]
pub mod pgsql;

#[cfg(feature = "sqlite")]
pub mod sqlite;

use std::sync::OnceLock;
use std::time::Duration;

/// `insert` 的执行结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InsertOutcome<T> {
    /// 插入成功。`T` 为后端返回值：MySQL `last_insert_id`、SQLite `last_insert_rowid`、
    /// PostgreSQL 为 `RETURNING` 映射的行类型。
    Inserted(T),
    /// 唯一约束冲突（`is_unique_violation`），视为幂等重复。
    Duplicate,
}

/// 判断 `sqlx::Error` 是否为唯一约束冲突。
#[inline]
pub fn is_unique_violation(err: &sqlx::Error) -> bool {
    err.as_database_error().is_some_and(|db| db.is_unique_violation())
}

/// 判断 `anyhow::Error` 内层是否为唯一约束冲突。
#[inline]
pub fn is_unique_violation_anyhow(err: &anyhow::Error) -> bool {
    err.downcast_ref::<sqlx::Error>().is_some_and(is_unique_violation)
}

/// SQL 日志回调：`(sql, 耗时, 错误)`
pub type Logger = Box<dyn Fn(String, Duration, Option<&anyhow::Error>) + Send + Sync + 'static>;

static SQL_LOGGER: OnceLock<Logger> = OnceLock::new();

/// 注册全局 SQL 日志回调；重复调用时仅第一次生效。
///
/// 唯一约束冲突（[`InsertOutcome::Duplicate`]）在日志中同样记为无错误（`err` 为 `None`）。
///
/// # Examples
///
/// ```ignore
/// sql::set_sql_logger(|sql, cost, err| {
///     match err {
///         Some(e) => {
///             tracing::error!(sql = sql, cost_ms = cost.as_millis(), err = %e, "sql error");
///         }
///         None => {
///             if cost > Duration::from_millis(200) {
///                 tracing::warn!(sql = sql, cost_ms = cost.as_millis(), "slow sql");
///             } else {
///                 tracing::info!(sql = sql, cost_ms = cost.as_millis(), "sql");
///             }
///         }
///     }
/// })
/// ```
pub fn set_sql_logger<F>(f: F)
where
    F: Fn(String, Duration, Option<&anyhow::Error>) + Send + Sync + 'static,
{
    let _ = SQL_LOGGER.set(Box::new(f));
}

#[cfg(any(feature = "mysql", feature = "postgres", feature = "sqlite"))]
#[inline]
fn trace_sql(sql: String, cost: Duration, err: Option<&anyhow::Error>) {
    if let Some(logger) = SQL_LOGGER.get() {
        logger(sql, cost, err)
    }
}

/// 包装 insert 结果；唯一约束冲突时返回 [`InsertOutcome::Duplicate`]
#[cfg(any(feature = "mysql", feature = "postgres", feature = "sqlite"))]
fn trace_insert_result<T, R, F>(sql: String, cost: Duration, ret: Result<R, sqlx::Error>, map_ok: F) -> anyhow::Result<InsertOutcome<T>>
where
    F: FnOnce(R) -> T,
{
    match ret {
        Ok(v) => {
            trace_sql(sql, cost, None);
            Ok(InsertOutcome::Inserted(map_ok(v)))
        }
        Err(e) => {
            if is_unique_violation(&e) {
                trace_sql(sql, cost, None);
                Ok(InsertOutcome::Duplicate)
            } else {
                let err = anyhow::Error::from(e);
                trace_sql(sql, cost, Some(&err));
                Err(err)
            }
        }
    }
}

/// 包装 `execute` 结果（如 `rows_affected`）并触发 SQL 日志。
#[cfg(any(feature = "mysql", feature = "postgres", feature = "sqlite"))]
fn trace_execute_result<R, F>(sql: String, cost: Duration, ret: Result<R, sqlx::Error>, map_ok: F) -> anyhow::Result<u64>
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

/// 包装查询类 SQL 结果并触发 [`set_sql_logger`] 回调。
#[cfg(any(feature = "mysql", feature = "postgres", feature = "sqlite"))]
fn trace_query_result<T>(sql: String, cost: Duration, ret: Result<T, sqlx::Error>) -> anyhow::Result<T> {
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::set_sql_logger;

    #[test]
    fn test_sql_logger() {
        set_sql_logger(|sql, cost, err| match err {
            Some(e) => {
                println!("sql error: {sql}, cost: {}ms, err: {e}", cost.as_millis());
            }
            None => {
                if cost > Duration::from_millis(200) {
                    println!("slow sql: {sql}, cost: {}ms", cost.as_millis());
                } else {
                    println!("sql: {sql}, cost: {}ms", cost.as_millis());
                }
            }
        })
    }
}
