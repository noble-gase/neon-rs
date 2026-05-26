//! MySQL CRUD helper（基于 sea-query + sqlx）。

use std::time::Instant;

use sea_query::{DeleteStatement, Expr, InsertStatement, MysqlQueryBuilder, SelectStatement, UpdateStatement};
use sea_query_sqlx::SqlxBinder;
use sqlx::{Executor, FromRow, MySql, mysql::MySqlRow};

use crate::{InsertOutcome, trace_execute_result, trace_insert_result, trace_query_result};

/// 插入记录；成功时返回 `last_insert_id`
///
/// # Examples
///
/// ```ignore
/// let stmt = Query::insert()
///     .into_table(table::Demo::Table)
///     .columns([table::Demo::Name])
///     .values_panic(["demo".into()])
///     .to_owned();
///
/// let ret = mysql::insert(&pool, stmt).await;
/// ```
pub async fn insert<'e, E>(db: E, stmt: InsertStatement) -> anyhow::Result<InsertOutcome<u64>>
where
    E: Executor<'e, Database = MySql>,
{
    let (sql, values) = stmt.build_sqlx(MysqlQueryBuilder);

    let start = Instant::now();
    let ret = sqlx::query_with(&sql, values).execute(db).await;
    let cost = start.elapsed();

    trace_insert_result(sql, cost, ret, |v| v.last_insert_id())
}

/// 更新记录
///
/// # Examples
///
/// ```ignore
/// let stmt = Query::update()
///     .table(table::Demo::Table)
///     .values([(table::Demo::Name, "demo".into())])
///     .and_where(Expr::col(table::Demo::Id).eq(1))
///     .to_owned();
///
/// let ret = mysql::update(&pool, stmt).await;
/// ```
pub async fn update<'e, E>(db: E, stmt: UpdateStatement) -> anyhow::Result<u64>
where
    E: Executor<'e, Database = MySql>,
{
    let (sql, values) = stmt.build_sqlx(MysqlQueryBuilder);

    let start = Instant::now();
    let ret = sqlx::query_with(&sql, values).execute(db).await;
    let cost = start.elapsed();

    trace_execute_result(sql, cost, ret, |v| v.rows_affected())
}

/// 删除记录
///
/// # Examples
///
/// ```ignore
/// let stmt = Query::delete()
///     .from_table(table::Demo::Table)
///     .and_where(Expr::col(table::Demo::Id).eq(1))
///     .to_owned();
///
/// let ret = mysql::delete(&pool, stmt).await;
/// ```
pub async fn delete<'e, E>(db: E, stmt: DeleteStatement) -> anyhow::Result<u64>
where
    E: Executor<'e, Database = MySql>,
{
    let (sql, values) = stmt.build_sqlx(MysqlQueryBuilder);

    let start = Instant::now();
    let ret = sqlx::query_with(&sql, values).execute(db).await;
    let cost = start.elapsed();

    trace_execute_result(sql, cost, ret, |v| v.rows_affected())
}

/// 统计记录数
///
/// # Examples
///
/// ```ignore
/// let stmt = Query::select()
///     .from(table::Demo::Table)
///     .and_where(Expr::col(table::Demo::Name).like("%demo%"))
///     .to_owned();
///
/// let ret = mysql::count(&pool, stmt).await;
/// ```
pub async fn count<'e, E>(db: E, mut stmt: SelectStatement) -> anyhow::Result<i64>
where
    E: Executor<'e, Database = MySql>,
{
    stmt.clear_selects();
    stmt.clear_order_by();
    stmt.expr(Expr::cust("COUNT(*)"));

    let (sql, values) = stmt.build_sqlx(MysqlQueryBuilder);

    let start = Instant::now();
    let ret: Result<i64, sqlx::Error> = sqlx::query_scalar_with(&sql, values).fetch_one(db).await;
    let cost = start.elapsed();

    trace_query_result(sql, cost, ret)
}

/// 查询单条记录
///
/// # Examples
///
/// ```ignore
/// let stmt = Query::select()
///     .from(table::Demo::Table)
///     .expr(Expr::cust("*"))
///     .and_where(Expr::col(table::Demo::Id).eq(1))
///     .to_owned();
///
/// let ret = mysql::find_one::<model::Demo>(&pool, stmt).await;
/// ```
pub async fn find_one<'e, E, T>(db: E, mut stmt: SelectStatement) -> anyhow::Result<Option<T>>
where
    E: Executor<'e, Database = MySql>,
    T: for<'r> FromRow<'r, MySqlRow> + Send + Unpin,
{
    stmt.limit(1);
    let (sql, values) = stmt.build_sqlx(MysqlQueryBuilder);

    let start = Instant::now();
    let ret = sqlx::query_as_with::<_, T, _>(&sql, values).fetch_optional(db).await;
    let cost = start.elapsed();

    trace_query_result(sql, cost, ret)
}

/// 查询多条记录
///
/// # Examples
///
/// ```ignore
/// let stmt = Query::select()
///     .from(table::Demo::Table)
///     .expr(Expr::cust("*"))
///     .and_where(Expr::col(table::Demo::Name).like("%demo%"))
///     .to_owned();
///
/// let ret = mysql::find_all::<model::Demo>(&pool, stmt).await;
/// ```
pub async fn find_all<'e, E, T>(db: E, stmt: SelectStatement) -> anyhow::Result<Vec<T>>
where
    E: Executor<'e, Database = MySql>,
    T: for<'r> FromRow<'r, MySqlRow> + Send + Unpin,
{
    let (sql, values) = stmt.build_sqlx(MysqlQueryBuilder);

    let start = Instant::now();
    let ret = sqlx::query_as_with::<_, T, _>(&sql, values).fetch_all(db).await;
    let cost = start.elapsed();

    trace_query_result(sql, cost, ret)
}

/// 分页查询
///
/// # Examples
///
/// ```ignore
/// let stmt = Query::select()
///     .from(table::Demo::Table)
///     .expr(Expr::cust("*"))
///     .and_where(Expr::col(table::Demo::Name).like("%demo%"))
///     .order_by(table::Demo::Id, Order::Desc)
///     .to_owned();
///
/// let ret = mysql::paginate::<model::Demo>(&pool, stmt, 1, 10).await;
/// ```
pub async fn paginate<'e, E, T>(db: E, mut stmt: SelectStatement, mut page: i32, mut size: i32) -> anyhow::Result<(Vec<T>, i64)>
where
    E: Executor<'e, Database = MySql> + Copy,
    T: for<'r> FromRow<'r, MySqlRow> + Send + Unpin,
{
    // 构建 count 查询
    let mut count = stmt.clone();
    count.clear_selects();
    count.clear_order_by();
    count.expr(Expr::cust("COUNT(*)"));

    let (count_sql, count_values) = count.build_sqlx(MysqlQueryBuilder);

    let count_start = Instant::now();
    let ret: Result<i64, sqlx::Error> = sqlx::query_scalar_with(&count_sql, count_values).fetch_one(db).await;
    let count_cost = count_start.elapsed();

    let total = trace_query_result(count_sql, count_cost, ret)?;
    if total == 0 {
        return Ok((Vec::new(), total));
    }

    // 构建分页查询
    if page <= 0 {
        page = 1
    }
    if size <= 0 {
        size = 20
    }
    stmt.limit(size as u64).offset(((page - 1) * size) as u64);

    let (query_sql, query_values) = stmt.build_sqlx(MysqlQueryBuilder);

    let query_start = Instant::now();
    let ret = sqlx::query_as_with::<_, T, _>(&query_sql, query_values).fetch_all(db).await;
    let query_cost = query_start.elapsed();

    let list = trace_query_result(query_sql, query_cost, ret)?;
    Ok((list, total))
}
