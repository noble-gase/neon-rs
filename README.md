# neon-rs

[<img alt="crates.io" src="https://img.shields.io/crates/v/neon-rs.svg?style=for-the-badge&color=fc8d62&logo=rust" height="20">](https://crates.io/crates/neon-rs)
[<img alt="MIT" src="http://img.shields.io/badge/license-MIT-brightgreen.svg?style=for-the-badge" height="20">](http://opensource.org/licenses/MIT)

[氖-Neon] Rust 开发工具集

## 安装

```shell
# 按需启用 feature
cargo add neon-rs --features "crypto,macros,redis,sql-mysql"
```

## Crates

| 模块     | 说明                                                       |
| -------- | ---------------------------------------------------------- |
| `core`   | 基础模块（始终可用）                                       |
| `config` | 配置模块，基于 `config` crate 统一加载与读取；支持 `Nacos` |
| `crypto` | 加密模块，支持：hash / aes / des / rsa                     |
| `helper` | 辅助模块，支持：经纬度坐标转换、远程读取 ZIP 解析 等       |
| `log`    | 日志模块，封装 tracing Layer，对接外部日志后端；支持 `sls` |
| `macros` | 宏模块，支持：`sqlx` 模型派生宏 `Model`                    |
| `redis`  | Redis 模块，支持：异步连接创建、redlock 分布式锁、辅助方法 |
| `sql`    | DB 模块，支持：连接创建 和 基于 `sea-query` 的 CRUD 封装   |


## Features

- **`config`** — 第三方配置接入，基于 [`config`](https://docs.rs/config) crate 统一加载与读取
  - `config-nacos` — Nacos 配置源（toml / yaml / json 等；本地文件或远程；热更新）
- **`crypto`** — 加密模块全集（hash / aes / des / rsa）
  - `crypto-hash` — HASH 与 HMAC
  - `crypto-aes` — AES (CBC / ECB / GCM)
  - `crypto-des` — DES
  - `crypto-rsa` — RSA
- **`helper`** — 一些辅助方法
- **`log`** — 封装 tracing Layer，对接外部日志后端
  - `log-sls` — 阿里云 SLS 异步批量投递（含 span 上下文）
- **`macros`** — `sqlx` 模型派生宏 `Model`
- **`redis`** — 异步连接创建、redlock 分布式锁、辅助方法
  - `redis-cluster` — Redis Cluster 异步连接
  - `redis-sync-lock` — 同步 `RedLock`（r2d2）
  - `redis-tls-rustls` — TLS（`rediss://`，rustls 后端）
  - `redis-tls-rustls-insecure` — TLS + 跳过证书校验（自签名证书，`rediss://...#insecure`）
  - `redis-tls-native-tls` — TLS（`rediss://`，native-tls 后端）
- **`sql`** — 连接创建 和 基于 `sea-query` 的 CRUD 封装
  - `sql-mysql` — 仅 MySQL
  - `sql-postgres` — 仅 PostgreSQL
  - `sql-sqlite` — 仅 SQLite

### Nacos 配置

启用 `config-nacos` 后，通过 `neon::config::nacos` 访问。`APP_ENV=local` 时加载本地文件，其它环境从 Nacos 拉取并自动热更新。详见 [`neon-config` 模块文档](crates/neon-config/src/nacos/mod.rs)。

```rust
use neon::config::nacos::{DEFAULT_ENV_VAR, Env, NacosConfig, NacosSource};

let cfg = NacosConfig::builder()
    .local_file("config/app.local.toml")
    .nacos(NacosSource::new("127.0.0.1:8848", "").data_id("app.toml"))
    .build(Env::from_var(DEFAULT_ENV_VAR))
    .await?;

let port = cfg.get_int("server.port")?;
```

### 阿里云 SLS 日志

启用 `log` 或 `log-sls` 后，通过 `neon::log::sls` 接入 tracing Layer，业务线程几乎零阻塞。详见 [`neon-log` 模块文档](crates/neon-log/src/sls/mod.rs)。

```rust
use neon::log::sls::{SlsConfig, build};

let config = SlsConfig::new(endpoint, ak_id, ak_secret, project, logstore);
let (sls_layer, _guard) = build(config)?;
// _guard 须持有到进程结束
```

### Redis 分布式锁

[`redlock`](crates/neon-redis/src/redlock.rs) 为单 key `SET NX` + TTL 互斥锁，**非** Antirez 多 master quorum Redlock；未获锁时 `acquire` 返回 `None`

### PostgreSQL 插入

`pgsql::insert` / `batch_insert` 通过 `query_as` 读取结果，**INSERT 语句须包含 `RETURNING`**（例如：`.returning_all()` 或 `.returning_col(...)`）

## Macros

#### 派生宏：Model

> 生成带 `sqlx::FromRow` 的子 struct（字段子集），便于查询映射

```rust
#[derive(Model)]
#[model(UserLite !(email, phone))] // 排除字段
#[model(UserBrief (id, age), derive(Copy, Debug))] // 包含字段
pub struct User {
    pub id: i64,

    #[sqlx(rename = "username")]
    pub name: String,

    pub age: i8
    pub email: String,
    pub phone: String,
}
```

- 生成代码

```rust
#[derive(sqlx::FromRow)]
pub struct UserLite {
    pub id: i64,

    #[sqlx(rename = "username")]
    pub name: String,

    pub age: i8,
}

#[derive(sqlx::FromRow, Copy, Debug)]
pub struct UserBrief {
    pub id: i64,
    pub age: i8,
}
```

**Enjoy 😊**
