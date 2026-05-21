use std::{sync::OnceLock, time::Duration};

pub mod client;
pub mod mysql;
pub mod pgsql;
pub mod sqlite;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InsertOutcome<T> {
    Inserted(T),
    Duplicate,
}

#[inline]
pub fn is_unique_violation(err: &sqlx::Error) -> bool {
    err.as_database_error().is_some_and(|db| db.is_unique_violation())
}

#[inline]
pub fn is_unique_violation_anyhow(err: &anyhow::Error) -> bool {
    err.downcast_ref::<sqlx::Error>().is_some_and(is_unique_violation)
}

pub type Logger = Box<dyn Fn(String, Duration, Option<&anyhow::Error>) + Send + Sync + 'static>;

static SQL_LOGGER: OnceLock<Logger> = OnceLock::new();

/// 设置SQL日志
///
/// # Examples
///
/// ```
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

#[inline]
fn trace_sql(sql: String, cost: Duration, err: Option<&anyhow::Error>) {
    if let Some(logger) = SQL_LOGGER.get() {
        logger(sql, cost, err)
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
