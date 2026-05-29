# neon-rs

[<img alt="crates.io" src="https://img.shields.io/crates/v/neon.svg?style=for-the-badge&color=fc8d62&logo=rust" height="20">](https://crates.io/crates/neon)
[<img alt="MIT" src="http://img.shields.io/badge/license-MIT-brightgreen.svg?style=for-the-badge" height="20">](http://opensource.org/licenses/MIT)

[氖-Neon] Rust 开发工具集

## 安装

```shell
# 按需启用 feature
cargo add neon-rs --features "crypto,macros,redis,sql-mysql"
```

## Crates

| 模块     | 说明                                                    |
| -------- | ------------------------------------------------------- |
| `core`   | 基础模块，包含：`zoned` 时区转换 等                     |
| `crypto` | 加密模块，包含：hash / aes / des / rsa                  |
| `redis`  | Redis模块，包含：异步连接池、redlock 分布式锁、辅助方法 |
| `sql`    | DB模块，包含：连接池 和 基于 `sea-query` 的 CRUD 封装   |
| `macros` | `sqlx` 模型派生宏 `Model`                               |

## Features

- **`helper`** — 一些辅助方法
- **`zoned`** — Unix 时间戳 → `jiff::Zoned`
- **`crypto`** — 加密模块全集（hash / aes / des / rsa）
  - `crypto-hash` — HASH 与 HMAC
  - `crypto-aes` — AES (CBC / ECB / GCM)
  - `crypto-des` — DES
  - `crypto-rsa` — RSA
- **`macros`** — `sqlx` 模型派生宏 `Model`
- **`redis`** — 异步连接池、redlock 分布式锁、辅助方法
  - `redis-cluster` — Redis Cluster 异步连接池
  - `redis-sync-lock` — 同步 `RedLock`（r2d2）
- **`sql`** — 连接池 和 基于 `sea-query` 的 CRUD 封装
  - `sql-mysql` — 仅 MySQL
  - `sql-postgres` — 仅 PostgreSQL
  - `sql-sqlite` — 仅 SQLite



### PostgreSQL 插入

`pgsql::insert` / `batch_insert` 通过 `query_as` 读取结果，**INSERT 语句须包含 `RETURNING`**（例如 `.returning_all()` 或 `.returning_col(...)`）

### Redis 分布式锁

[`redlock`](crates/neon-redis/src/redlock.rs) 为单 key `SET NX` + TTL 互斥锁，**非** Antirez 多 master quorum Redlock；未获锁时 `acquire` 返回 `None`

## neon-macros

#### 派生宏：Model

```rust
#[derive(Model)]
#[model(UserLite !(email, phone))] // 排除字段
#[model(UserBrief (id, name), derive(Copy, Debug))] // 包含字段
pub struct User {
    pub id: i64,

    #[sqlx(rename = "username")]
    pub name: String,

    pub email: String,
    pub phone: String,
    pub created_at: String,
    pub updated_at: String,
}
```

生成带 `sqlx::FromRow` 的子 struct（字段子集），便于查询映射

**Enjoy 😊**
