//! 基于 `nacos-sdk` 与 `config` crate 的 Nacos 配置
//!
//! # 启用方式
//!
//! 单独使用子 crate 时，在 `Cargo.toml` 中启用 `nacos` feature：
//!
//! ```toml
//! neon-config = { version = "0", features = ["nacos"] }
//! ```
//!
//! 通过 [`neon`](https://github.com/noble-gase/neon-rs) 聚合 crate 使用时，启用 `config-nacos` feature，
//! 并通过 `neon::config::nacos` 访问本模块
//!
//! # 能力
//!
//! - 支持 toml / yaml / json / json5 / ini / ron 多种格式
//! - 通过环境区分配置来源：`local` 环境加载本地文件或内联配置；其它环境从 nacos 加载
//! - 远程环境下自动注册监听器，实现 nacos 配置热更新
//! - 热更新失败时自动回滚 entries
//!
//! # 示例
//!
//! ```no_run
//! use neon_config::nacos::{DEFAULT_ENV_VAR, Env, NacosConfig, NacosSource};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let cfg = NacosConfig::builder()
//!     // local 环境使用本地文件
//!     .local_file("config/app.local.toml")
//!     // 其它环境使用 nacos（直连地址；如需 endpoint 寻址可使用 NacosSource::from_endpoint）
//!     .nacos(
//!         NacosSource::new("127.0.0.1:8848", "")
//!             .data_id(["app.toml", "shared.toml"]),
//!     )
//!     .build(Env::from_var(DEFAULT_ENV_VAR))
//!     .await?;
//!
//! // 无需指定类型，直接按路径取值
//! let name = cfg.get_string("name")?;
//! let port = cfg.get_int("server.port")?;
//! println!("{name} : {port}");
//! # Ok(())
//! # }
//! ```
//!
//! # 鉴权
//!
//! 普通 Nacos 用户名密码鉴权：
//!
//! ```no_run
//! use neon_config::nacos::NacosSource;
//!
//! let source = NacosSource::new("127.0.0.1:8848", "")
//!     .data_id("app.toml")
//!     .auth("username", "password");
//! # let _ = source;
//! ```
//!
//! 阿里云 RAM/ACM endpoint 寻址鉴权：
//!
//! ```no_run
//! use neon_config::nacos::NacosSource;
//!
//! let source = NacosSource::from_endpoint("acm.aliyun.com:8080", "")
//!     .data_id("app.toml")
//!     .access_key("access-key")
//!     .secret_key("secret-key")
//!     .region_id("cn-hangzhou");
//! # let _ = source;
//! ```
//!
//! # 热更新
//!
//! 热更新是自动的：远程环境下 nacos 推送变更后，内部监听器会自动重新解析并更新快照，
//! 之后再调用 `cfg.get_*(...)` 即可拿到最新值，**无需手动订阅**
//!
//! ```no_run
//! use neon_config::nacos::NacosConfig;
//!
//! # async fn poll(cfg: NacosConfig) -> Result<(), Box<dyn std::error::Error>> {
//! // 每次读取都是当前最新值
//! let port = cfg.get_int("server.port")?;
//! # let _ = port;
//! # Ok(())
//! # }
//! ```
//!
//! 仅当需要在配置变更时「主动响应」（如重建连接池）时，才需要订阅：
//!
//! - [`NacosConfig::subscribe`]：任一 data-id 变更时触发，推送合并后的完整快照
//! - [`NacosConfig::subscribe_data_id`]：仅指定 data-id 变更时触发
//!
//! ```no_run
//! use neon_config::nacos::NacosConfig;
//!
//! # async fn watch(cfg: NacosConfig) -> Result<(), Box<dyn std::error::Error>> {
//! let mut rx = cfg.subscribe();
//! tokio::spawn(async move {
//!     while rx.changed().await.is_ok() {
//!         let cfg = rx.borrow().clone();
//!         if let Ok(port) = cfg.get_int("server.port") {
//!             println!("配置已更新，最新端口: {port}");
//!         }
//!     }
//! });
//! # Ok(())
//! # }
//! ```
//!
//! 配置了多个 data-id 时，可分别订阅并按 data-id 响应变更：
//!
//! ```no_run
//! use neon_config::nacos::NacosConfig;
//!
//! # async fn watch_multi(cfg: NacosConfig) -> Result<(), Box<dyn std::error::Error>> {
//! let mut app_rx = cfg.subscribe_data_id("app.yaml").unwrap();
//! let mut shared_rx = cfg.subscribe_data_id("shared.yaml").unwrap();
//!
//! tokio::select! {
//!     Ok(()) = app_rx.changed() => {
//!         // app.yaml 变了
//!     }
//!     Ok(()) = shared_rx.changed() => {
//!         // shared.yaml 变了
//!     }
//! }
//! # Ok(())
//! # }
//! ```

mod config;
mod env;
mod error;
mod format;

pub use config::{ConfigBuilder, DEFAULT_GROUP, NacosConfig, NacosSource};
pub use env::{DEFAULT_ENV_VAR, Env};
pub use error::{ConfigError, Result};
pub use format::Format;
