//! tracing → 阿里云 SLS 日志投递层
//!
//! 把应用的结构化日志（含 span 上下文）异步、批量地投递到阿里云 SLS，
//! 对业务线程几乎零阻塞。模块划分：
//!
//! - [`layer`]：tracing 集成，事件提取、按级别路由、Layer 构建
//! - [`sink`]：单条投递管线（client / 队列 / 后台 worker / 批量发送 / 重试 / 优雅关闭）
//!
//! # 启用方式
//!
//! 单独使用子 crate 时，在 `Cargo.toml` 中启用 `sls` feature：
//!
//! ```toml
//! neon-log = { version = "0", features = ["sls"] }
//! ```
//!
//! 通过 [`neon`](https://github.com/noble-gase/neon-rs) 聚合 crate 使用时，启用 `log` 或 `log-sls` feature，
//! 并通过 `neon::log::sls` 访问本模块。
//!
//! # 快速接入
//!
//! ```ignore
//! use std::time::Duration;
//!
//! use tracing::Level;
//! use tracing_subscriber::layer::SubscriberExt;
//! use tracing_subscriber::util::SubscriberInitExt;
//! use tracing_subscriber::{EnvFilter, fmt};
//!
//! use neon_log::sls::{
//!     DEFAULT_EW_QUEUE_CAPACITY, SlsConfig, SlsLayerBuilder, StaticCredentialsProvider, build,
//! };
//!
//! // ===== 1. 配置 SLS（凭据勿硬编码，从环境变量或配置中心读取）=====
//! // 长期 AccessKey 用 StaticCredentialsProvider 作为凭据来源
//! let config = SlsConfig::new(
//!     std::env::var("SLS_ENDPOINT").expect("SLS_ENDPOINT"),
//!     std::env::var("SLS_PROJECT").expect("SLS_PROJECT"),
//!     std::env::var("SLS_LOGSTORE").expect("SLS_LOGSTORE"),
//!     StaticCredentialsProvider::new(
//!         std::env::var("SLS_ACCESS_KEY_ID").expect("SLS_ACCESS_KEY_ID"),
//!         std::env::var("SLS_ACCESS_KEY_SECRET").expect("SLS_ACCESS_KEY_SECRET"),
//!     ),
//! )
//! .topic("my-service")                       // 可选
//! .source("127.0.0.1")                       // 可选
//! .max_inflight(4)                           // 可选，默认 4
//! .max_batch_bytes(512 * 1024)               // 可选，按字节冲刷，默认 512KB
//! .no_retry_status([400, 404])               // 可选，命中即不重试，默认 [400, 404]
//! .flush_interval(Duration::from_secs(2));   // 可选，默认 2s
//!
//! // ===== 2. 构建 SLS 层与守卫 =====
//! // error/warn 与 info/debug/trace 各一条独立 sink（独立 client / 队列 / worker）
//! let error_config = config
//!     .clone()
//!     .topic("my-service-error")
//!     .queue_capacity(DEFAULT_EW_QUEUE_CAPACITY);
//! let info_config = config.topic("my-service");
//!
//! // build：client 创建失败时返回 Err
//! let (sls_layer, _guard) = SlsLayerBuilder::new()
//!     .add_sink([Level::ERROR, Level::WARN], error_config)
//!     .add_sink([Level::INFO, Level::DEBUG, Level::TRACE], info_config)
//!     .build()
//!     .expect("init sls layer");
//!
//! // _guard 必须持有到进程结束：drop 时会冲刷残留日志并等待后台 worker 退出
//!
//! // ===== 3. 初始化 tracing：本地控制台 + SLS 双路输出 =====
//! let env_filter = EnvFilter::try_from_default_env()
//!     .unwrap_or_else(|_| EnvFilter::new("info,hyper=warn,reqwest=warn,rustls=warn,h2=warn"));
//!
//! tracing_subscriber::registry()
//!     .with(env_filter)
//!     .with(fmt::layer())
//!     .with(sls_layer)
//!     .init();
//!
//! // ===== 4. 正常使用 tracing 宏即可，span 字段会自动并入 SLS 日志 =====
//! let span = tracing::info_span!("handle_request", request_id = "req-1001");
//! let _enter = span.enter();
//! tracing::info!(method = "GET", path = "/api/health", "收到请求");
//! tracing::warn!(latency_ms = 128, "下游响应较慢");
//! tracing::error!(error_code = 503, "下游服务暂不可用");
//! ```
//!
//! # 最简双 sink（一行构建器）
//!
//! 若不需要自定义 topic / 队列，可直接用 [`build`]：
//!
//! ```ignore
//! use neon_log::sls::{SlsConfig, StaticCredentialsProvider, build};
//!
//! let config = SlsConfig::new(endpoint, project, logstore, StaticCredentialsProvider::new(ak_id, ak_secret));
//! let (sls_layer, _guard) = build(config).expect("init sls layer");
//! // 默认：ERROR/WARN 一条 sink + INFO/DEBUG/TRACE 一条 sink，各自独立 client
//! // ERROR/WARN 队列容量上限为 DEFAULT_EW_QUEUE_CAPACITY（8192）
//! ```
//!
//! # 自定义路由
//!
//! 通过 [`SlsLayerBuilder`] 自由注册任意多条 sink，并指定每条负责的级别：
//!
//! ```ignore
//! use tracing::Level;
//! use neon_log::sls::SlsLayerBuilder;
//!
//! let (sls_layer, _guard) = SlsLayerBuilder::new()
//!     .add_sink([Level::ERROR, Level::WARN], err_config)   // 可指向不同 logstore
//!     .add_sink([Level::INFO, Level::DEBUG, Level::TRACE], info_config)
//!     .build()?;
//!
//! // 或者每个级别一条独立 sink：
//! // SlsLayerBuilder::new()
//! //     .add_sink([Level::ERROR], c_err)
//! //     .add_sink([Level::WARN],  c_warn)
//! //     .add_sink([Level::INFO],  c_info)
//! //     .build()?;
//! ```
//!
//! 路由规则：
//! - 同一级别被多条 sink 声明时，以**先注册者**为准
//! - 未被任何 sink 覆盖的级别会被忽略（不投递）
//! - `max_inflight` 是**每条 sink** 的并发上限
//!
//! # 运行期观测
//!
//! ```ignore
//! use neon_log::sls::SlsLayer;
//!
//! // 查询所有 sink 聚合后的丢弃统计
//! let snapshot = sls_layer.dropped_snapshot();
//! // snapshot.total / queue_full / worker_closed / send_failed / shutdown_dropped
//! ```
//!
//! # 注意事项
//!
//! - **`SlsGuard` 必须持有到进程结束**，否则 drop 前队列中残留日志可能丢失
//! - 进程被强杀（SIGKILL）时 guard 的 drop 不会执行
//! - 日志时间戳秒部分为 `u32`（SLS SDK 约束），2106 年溢出
//! - 不要在日志或 `Debug` 输出中打印 [`SlsConfig`]，以免泄漏凭据
//! - 批量冲刷同时受条数（`max_batch_size`）与字节（`max_batch_bytes`）两个阈值约束，
//!   任一达标即触发；字节数按各字段 key+value 的 UTF-8 长度估算，钳制在 [1KB, 4MB]
//! - `no_retry_status` 命中的服务端错误（默认 400/404）不重试，直接计为 `send_failed` 丢弃
//! - 凭据均经 [`CredentialsProvider`] 提供，长期 AccessKey 用 [`StaticCredentialsProvider`]，
//!   STS 临时凭据用自定义实现或闭包。
//!   worker 启动时调用一次 provider 构建 client；若凭据带 `expire_time` 则**按有效期精确刷新**
//!   （到期前 [`SlsConfig::credentials_refresh_ahead`]，默认 5min）；若不带有效期且未设
//!   [`SlsConfig::credentials_refresh_interval`]（长期 AccessKey 即属此类）则不再周期刷新；
//!   提供器失败仅告警并沿用旧凭据，且在最短间隔（60s）后尽快重试
//!
//! ## STS 自动刷新示例
//!
//! ```ignore
//! use std::time::Duration;
//! use neon_log::sls::{SlsConfig, SlsCredentials};
//!
//! // 任意 `Fn() -> Result<SlsCredentials, BoxError>` 闭包都自动实现 CredentialsProvider；
//! // 提供器可进行阻塞式网络请求（运行在 spawn_blocking 上），必须线程安全
//! let provider = || {
//!     let (id, secret, token, expire) = fetch_sts_from_metadata()?; // 用户自定义，expire 为 SystemTime
//!     // 带上到期时间，worker 就会在到期前提前刷新（不带则默认不再刷新，可用 credentials_refresh_interval 开启固定间隔）
//!     Ok(SlsCredentials::new(id, secret, token).expire_time(expire))
//! };
//!
//! // provider 作为唯一凭据来源直接传入 new，无需再传 AccessKey
//! let config = SlsConfig::new(endpoint, project, logstore, provider)
//!     .credentials_refresh_ahead(Duration::from_secs(5 * 60)); // 到期前提前量
//! ```

pub mod layer;
pub mod sink;

pub use layer::{DEFAULT_EW_QUEUE_CAPACITY, SlsLayer, SlsLayerBuilder, build};
pub use sink::{
    BoxError, CredentialsProvider, DropSnapshot, SlsBuildError, SlsConfig, SlsCredentials,
    SlsGuard, SlsSink, StaticCredentialsProvider,
};
