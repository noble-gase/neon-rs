//! SQL 工具集：连接池与基于 sea-query 的 CRUD helper（按后端 feature 启用）

pub mod factory;

#[cfg(feature = "mysql")]
pub mod mysql;

#[cfg(feature = "postgres")]
pub mod pgsql;

#[cfg(feature = "sqlite")]
pub mod sqlite;

use std::time::Duration;

use sqlx::Pool;

use crate::factory::{Factory, SQL_LOGGER};

/// 注册全局 SQL 日志回调
///
/// # Examples
///
/// ```ignore
/// set_sql_logger(|sql, cost, err| {
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

/// 连接池参数
#[derive(Default, Debug)]
pub struct PoolParams {
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
/// // [MySQL] mysql://username:password@host:3306/db?charset=utf8mb4
/// let pool = sql::open::<MySQL>("dsn".into(), None).await?;
///
/// // [PgSQL] postgres://username:password@host:5432/db
/// let pool = sql::open::<PgSQL>("dsn".into(), None).await?;
///
/// // [SQLite] sqlite:///path/to.db 或 sqlite::memory:
/// let pool = sql::open::<SQLite>("dsn".into(), None).await?;
/// ```
pub async fn open<F>(dsn: String, opt: Option<PoolParams>) -> anyhow::Result<Pool<F::DB>>
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

/// `insert` 的执行结果
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InsertOutcome<T> {
    /// 插入成功
    ///
    /// `T` 为后端返回值：MySQL `last_insert_id`、SQLite `last_insert_rowid`、PostgreSQL 为 `RETURNING` 映射的行类型
    Inserted(T),
    /// 唯一约束冲突
    Duplicate,
}

impl<T> InsertOutcome<T> {
    /// 是否为 [`InsertOutcome::Inserted`]
    #[inline]
    pub fn is_inserted(&self) -> bool {
        matches!(self, Self::Inserted(_))
    }

    /// 是否为 [`InsertOutcome::Duplicate`]
    #[inline]
    pub fn is_duplicate(&self) -> bool {
        matches!(self, Self::Duplicate)
    }

    /// 消费自身，成功则返回 `Some(value)`，唯一约束冲突返回 `None`
    ///
    /// 进一步可链式使用 `Option` 的 `unwrap_or` / `ok_or` / `map` 等方法
    #[inline]
    pub fn inserted(self) -> Option<T> {
        match self {
            Self::Inserted(v) => Some(v),
            Self::Duplicate => None,
        }
    }
}

/// 判断 `sqlx::Error` 是否为唯一约束冲突
#[inline]
pub fn is_unique_violation(err: &sqlx::Error) -> bool {
    err.as_database_error().is_some_and(|db| db.is_unique_violation())
}

/// 判断 `anyhow::Error` 内层是否为唯一约束冲突
#[inline]
pub fn is_unique_violation_anyhow(err: &anyhow::Error) -> bool {
    err.downcast_ref::<sqlx::Error>().is_some_and(is_unique_violation)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::InsertOutcome;
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

    #[test]
    fn insert_outcome_helpers() {
        let ok: InsertOutcome<u64> = InsertOutcome::Inserted(42);
        assert!(ok.is_inserted());
        assert!(!ok.is_duplicate());
        assert_eq!(ok.inserted(), Some(42));

        let dup: InsertOutcome<u64> = InsertOutcome::Duplicate;
        assert!(dup.is_duplicate());
        assert!(!dup.is_inserted());
        assert_eq!(dup.inserted(), None);
    }
}
