//! 单条 SLS 投递管线（sink）的完整封装
//!
//! 一个 [`SlsSink`] 自带 client、队列与后台 worker 线程，内部实现批量/定时冲刷、
//! 有界并发发送、指数退避重试与优雅关闭。上层（[`crate::sls::layer`]）负责
//! 把 tracing 事件转成 `LogRecord` 并按级别路由到某个 sink

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc as std_mpsc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime};

use aliyun_log_rust_sdk::{Client, Config, Error as SlsError, FromConfig};
use aliyun_log_sdk_protobuf::{Log, LogGroup};
use arc_swap::ArcSwap;
use crossbeam_queue::ArrayQueue;
use tokio::pin;
use tokio::sync::{Notify, Semaphore, watch};
use tokio::task::JoinSet;

// ===== 各配置项的默认值，集中在此便于统一调整 =====

/// 单批最多日志条数的默认值
const DEFAULT_MAX_BATCH_SIZE: usize = 4096;
/// 单批最多日志字节数的默认值（估算键值字节，达到即冲刷），默认 512KB
const DEFAULT_MAX_BATCH_BYTES: usize = 512 * 1024;
/// 单批字节数的下限，避免过小导致每条一批
const MIN_BATCH_BYTES: usize = 1024;
/// 单批字节数的上限：低于 SLS 单次 PutLogs 原始体量约束，给 protobuf 编码与 topic/source 留余量
const MAX_BATCH_BYTES_LIMIT: usize = 4 * 1024 * 1024;
/// STS 凭据自动刷新间隔的下限，避免过于频繁重建 client
const MIN_CREDENTIALS_REFRESH_INTERVAL: Duration = Duration::from_secs(60);
/// STS 凭据已临期/过期时的快速刷新间隔，避免继续使用即将失效的 client
const URGENT_CREDENTIALS_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
/// STS 凭据「提前刷新量」的默认值：凭据带有效期时，在其到期前该时长触发刷新
const DEFAULT_CREDENTIALS_REFRESH_AHEAD: Duration = Duration::from_secs(5 * 60);
/// 默认不重试的 HTTP 状态码：参数非法(400)与资源不存在(404)重试无意义
const DEFAULT_NO_RETRY_STATUS: &[u32] = &[400, 404];
/// 定时冲刷间隔的默认值
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(2);
/// 日志队列容量的默认值
const DEFAULT_QUEUE_CAPACITY: usize = 65_536;
/// 并发 in-flight PutLogs 请求上限的默认值
const DEFAULT_MAX_INFLIGHT: usize = 4;
/// 单批发送失败的默认最大重试次数
const DEFAULT_MAX_RETRIES: u32 = 5;
/// 指数退避初始间隔的默认值
const DEFAULT_RETRY_BASE: Duration = Duration::from_millis(200);
/// 指数退避上限间隔的默认值
const DEFAULT_RETRY_MAX: Duration = Duration::from_secs(10);
/// 单次 PutLogs 请求超时的默认值
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// 优雅关闭总时限的默认值
const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(15);
/// 所有时长配置允许的下限，避免出现 0 时长导致忙等或除零
const MIN_DURATION: Duration = Duration::from_millis(1);
/// 每条 Log 的 protobuf 结构保守开销，用于批量切分时留余量
const LOG_PROTO_OVERHEAD_BYTES: usize = 32;
/// 每个 content 字段的 protobuf tag/len 等保守开销，用于批量切分时留余量
const CONTENT_PROTO_OVERHEAD_BYTES: usize = 16;
/// LogGroup 的 topic/source 等固定开销估算，用于批量切分时留余量
const LOG_GROUP_PROTO_OVERHEAD_BYTES: usize = 256;

/// retry jitter 的无锁种子；仅用于打散退避时间，不用于安全随机
static JITTER_SEED: AtomicU64 = AtomicU64::new(0x9e37_79b9_7f4a_7c15);

/// 队列满（写入速度持续超过投递速度）时的溢出丢弃策略
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum OverflowPolicy {
    /// 丢弃新到达的日志，保留队列中较早的日志（默认，行为与旧版一致）
    #[default]
    DropNewest,
    /// 挤掉队首最早的日志以容纳新日志，更偏向保留最新事件（如崩溃前的现场）
    DropOldest,
}

/// 一条 sink 的完整配置：包含连接凭据、目标 project/logstore 以及批量、并发、重试、
/// 关闭等运行期参数。通过链式 setter 覆盖默认值
#[derive(Clone)]
pub struct SlsConfig {
    /// SLS 服务接入点，例如 `cn-hangzhou.log.aliyuncs.com`
    endpoint: String,

    /// SLS 工程名（Project）
    project: String,
    /// SLS 日志库名（Logstore）
    logstore: String,
    /// 日志 topic，写入 `LogGroup.topic`；`None` 表示不设置
    topic: Option<String>,
    /// 日志来源标识，写入 `LogGroup.source`（通常为主机名/IP）；`None` 表示不设置
    source: Option<String>,

    /// 单批最多日志条数，达到该阈值即触发一次冲刷，默认 4096
    max_batch_size: usize,
    /// 单批最多日志字节数（估算键值字节）；达到即触发冲刷，与条数阈值取先满足者，默认 512KB
    max_batch_bytes: usize,
    /// 定时冲刷间隔；即使未攒满一批也会在该间隔后冲刷，默认 2s
    flush_interval: Duration,
    /// 本 sink 日志队列容量；队列满时新日志按 `queue_full` 丢弃并计数，默认 65536
    queue_capacity: usize,
    /// 并发 in-flight 的 PutLogs 请求上限，形成有界背压，默认 4
    max_inflight: usize,
    /// 单批发送失败后的最大重试次数（指数退避），默认 5
    max_retries: u32,
    /// 重试退避的初始间隔，默认 200ms
    retry_base: Duration,
    /// 重试退避的上限间隔，默认 10s
    retry_max: Duration,
    /// 命中这些 HTTP 状态码的服务端错误不再重试，直接计为发送失败，默认 [400, 404]
    no_retry_status: Vec<u32>,
    /// 单次 PutLogs 请求的超时时间，默认 30s
    request_timeout: Duration,
    /// 优雅关闭的总时限：guard drop 时在该时限内排空队列并冲刷残留日志，默认 15s
    shutdown_timeout: Duration,

    /// 鉴权凭据提供器（唯一凭据来源）：sink 启动时调用一次以构建 client，
    /// 之后按凭据有效期或 `credentials_refresh_interval` 周期性刷新并重建 client
    credentials_provider: Arc<dyn CredentialsProvider>,
    /// STS 凭据刷新的回退间隔：仅当刷新到的凭据**不带** `expire_time` 时按此固定间隔刷新。
    /// `None`（默认）表示无有效期的凭据不再周期刷新（长期 AccessKey 即属此类）；
    /// 显式设置后最小 60s。刷新失败始终按最短间隔（60s）重试，与此项无关
    credentials_refresh_interval: Option<Duration>,
    /// STS 凭据「提前刷新量」：当刷新到的凭据带 `expire_time` 时，在其到期前该时长触发下一次刷新，
    /// 仅在凭据带有效期时生效，默认 5 分钟
    credentials_refresh_ahead: Duration,

    /// 队列满时的溢出丢弃策略，默认 [`OverflowPolicy::DropNewest`]
    overflow_policy: OverflowPolicy,
}

/// 一组 STS 临时凭据，由 [`CredentialsProvider`] 返回供 worker 刷新 client
#[derive(Clone)]
pub struct SlsCredentials {
    /// 临时 AccessKey ID
    pub access_key_id: String,
    /// 临时 AccessKey Secret
    pub access_key_secret: String,
    /// 安全令牌（SecurityToken）
    pub security_token: String,
    /// 凭据到期时刻（绝对时间）；`Some` 时 worker 在到期前提前刷新，`None` 时回退到固定间隔
    pub expire_time: Option<SystemTime>,
}

impl SlsCredentials {
    /// 用三段临时凭据构造（不带到期时间，按固定间隔刷新）
    pub fn new(
        access_key_id: impl Into<String>,
        access_key_secret: impl Into<String>,
        security_token: impl Into<String>,
    ) -> Self {
        Self {
            access_key_id: access_key_id.into(),
            access_key_secret: access_key_secret.into(),
            security_token: security_token.into(),
            expire_time: None,
        }
    }

    /// 设置该组凭据的到期时刻（绝对时间）。设置后 worker 会在到期前
    /// [`SlsConfig::credentials_refresh_ahead`] 触发下一次刷新，实现按有效期精确刷新
    pub fn expire_time(mut self, expire_time: SystemTime) -> Self {
        self.expire_time = Some(expire_time);
        self
    }
}

/// 凭据提供器：sink 启动及后续刷新时调用它获取一组鉴权凭据
///
/// 这是 SLS 鉴权的**唯一入口**（对齐 Go SDK 的 CredentialsProvider 模型）：
/// 长期 AccessKey 用 [`StaticCredentialsProvider`]，STS 临时凭据用自定义实现或闭包。
///
/// - `provide` 会在后台 worker 的 `spawn_blocking` 中被调用，允许内部进行阻塞式网络请求；
/// - 返回 `Err` 时保留旧 client，并在最短间隔（60s）后重试；
/// - 返回的 [`SlsCredentials`] 带 `expire_time` 时，worker 会在到期前自动刷新。
///
/// 任意 `Fn() -> Result<SlsCredentials, BoxError>` 闭包都自动实现本 trait，便于快速接入。
pub trait CredentialsProvider: Send + Sync + 'static {
    /// 返回一组当前可用的鉴权凭据
    fn provide(&self) -> Result<SlsCredentials, BoxError>;
}

impl<F> CredentialsProvider for F
where
    F: Fn() -> Result<SlsCredentials, BoxError> + Send + Sync + 'static,
{
    fn provide(&self) -> Result<SlsCredentials, BoxError> {
        self()
    }
}

/// 长期 AccessKey 的静态凭据提供器：始终返回同一组固定凭据，永不刷新
///
/// 对应 Go SDK 的 `NewStaticCredentialsProvider`，用于最常见的长期 AccessKey 场景。
pub struct StaticCredentialsProvider {
    creds: SlsCredentials,
}

impl StaticCredentialsProvider {
    /// 用长期 AccessKey 构造（不带 SecurityToken，以 AccessKey 方式鉴权）
    pub fn new(access_key_id: impl Into<String>, access_key_secret: impl Into<String>) -> Self {
        Self {
            creds: SlsCredentials::new(access_key_id, access_key_secret, ""),
        }
    }
}

impl CredentialsProvider for StaticCredentialsProvider {
    fn provide(&self) -> Result<SlsCredentials, BoxError> {
        Ok(self.creds.clone())
    }
}

impl SlsConfig {
    /// 用必填项创建配置，其余项取默认值（可链式调用 setter 覆盖）
    ///
    /// - `endpoint`：SLS 服务接入点，例如 `cn-hangzhou.log.aliyuncs.com`
    /// - `project`：SLS 工程名
    /// - `logstore`：SLS 日志库名
    /// - `credentials_provider`：鉴权凭据提供器（唯一凭据来源）。长期 AccessKey 用
    ///   [`StaticCredentialsProvider`]，STS 用自定义实现或 `Fn() -> Result<SlsCredentials, BoxError>` 闭包
    pub fn new(
        endpoint: impl Into<String>,
        project: impl Into<String>,
        logstore: impl Into<String>,
        credentials_provider: impl CredentialsProvider,
    ) -> Self {
        Self {
            // 必填连接与目标项：调用方提供的任意 Into<String> 统一转 owned String
            endpoint: endpoint.into(),

            project: project.into(),
            logstore: logstore.into(),
            // 可选项默认不设置，后续可用链式 setter 补充
            topic: None,
            source: None,

            // 运行期参数全部取模块级默认常量
            max_batch_size: DEFAULT_MAX_BATCH_SIZE,
            max_batch_bytes: DEFAULT_MAX_BATCH_BYTES,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
            max_inflight: DEFAULT_MAX_INFLIGHT,
            max_retries: DEFAULT_MAX_RETRIES,
            retry_base: DEFAULT_RETRY_BASE,
            retry_max: DEFAULT_RETRY_MAX,
            no_retry_status: DEFAULT_NO_RETRY_STATUS.to_vec(),
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,

            credentials_provider: Arc::new(credentials_provider),
            credentials_refresh_interval: None,
            credentials_refresh_ahead: DEFAULT_CREDENTIALS_REFRESH_AHEAD,

            overflow_policy: OverflowPolicy::DropNewest,
        }
    }

    /// 设置日志 topic（写入 `LogGroup.topic`），未设置时不带 topic
    pub fn topic(mut self, topic: impl Into<String>) -> Self {
        self.topic = Some(topic.into());
        self
    }

    /// 设置日志来源标识（写入 `LogGroup.source`，通常为主机名/IP），未设置时不带 source
    pub fn source(mut self, source: impl Into<String>) -> Self {
        self.source = Some(source.into());
        self
    }

    /// 设置单批最多日志条数；达到该阈值即触发一次冲刷，最小为 1，默认 4096
    ///
    /// 越界值在构建时由 `normalized` 统一钳制
    pub fn max_batch_size(mut self, n: usize) -> Self {
        self.max_batch_size = n;
        self
    }

    /// 设置单批最多日志字节数（按 (key+value) 长度估算）；达到该阈值即触发一次冲刷，
    /// 与条数阈值取先满足者。用于在超大日志下限制单请求体积、贴合 SLS 单批容量约束，
    /// 取值范围 [1KB, 4MB]，默认 512KB
    ///
    /// 越界值在构建时由 `normalized` 统一钳制
    pub fn max_batch_bytes(mut self, n: usize) -> Self {
        self.max_batch_bytes = n;
        self
    }

    /// 设置命中后不再重试的 HTTP 状态码集合（服务端返回该状态码时直接计为发送失败）。
    /// 覆盖默认值 `[400, 404]`；传入空集合表示所有失败都参与重试
    pub fn no_retry_status(mut self, codes: impl IntoIterator<Item = u32>) -> Self {
        self.no_retry_status = codes.into_iter().collect();
        self
    }

    /// 设置 STS 凭据刷新的回退间隔：仅当 provider 返回的凭据**不带** `expire_time` 时，
    /// 才按此固定间隔周期性刷新并重建 client。
    ///
    /// 默认不设置——无有效期的凭据不再周期刷新（长期 AccessKey 即属此类）。带 `expire_time`
    /// 的凭据始终按有效期精确刷新，与此项无关；刷新失败也始终按最短间隔（60s）重试。
    /// 设置的间隔最小 60s，越界值在构建时由 `normalized` 统一钳制
    pub fn credentials_refresh_interval(mut self, interval: Duration) -> Self {
        self.credentials_refresh_interval = Some(interval);
        self
    }

    /// 设置 STS 凭据「提前刷新量」：当 provider 返回的凭据带 `expire_time` 时，
    /// 会在其到期前该时长触发下一次刷新，实现按有效期精确刷新。默认 5 分钟
    ///
    /// 提前量过大（超过凭据有效期）时会退化为最短间隔（60s）刷新
    pub fn credentials_refresh_ahead(mut self, ahead: Duration) -> Self {
        self.credentials_refresh_ahead = ahead;
        self
    }

    /// 设置队列满时的溢出丢弃策略：[`OverflowPolicy::DropNewest`]（默认）丢弃新日志、
    /// 保留较早日志；[`OverflowPolicy::DropOldest`] 挤掉最早日志、保留最新日志。
    /// 对崩溃前现场更重要的关键级别 sink，可选用 `DropOldest`
    pub fn overflow_policy(mut self, policy: OverflowPolicy) -> Self {
        self.overflow_policy = policy;
        self
    }

    /// 设置定时冲刷间隔；即使未攒满一批，也会在该间隔后冲刷，最小 1ms，默认 2s
    ///
    /// 越界值在构建时由 `normalized` 统一钳制
    pub fn flush_interval(mut self, d: Duration) -> Self {
        self.flush_interval = d;
        self
    }

    /// 设置普通日志队列容量；队列满时新日志按 `queue_full` 丢弃并计数，最小为 1，默认 65536
    ///
    /// 越界值在构建时由 `normalized` 统一钳制
    pub fn queue_capacity(mut self, n: usize) -> Self {
        self.queue_capacity = n;
        self
    }

    /// 返回当前配置的队列容量（构建前未经 `normalized` 钳制）
    pub fn configured_queue_capacity(&self) -> usize {
        self.queue_capacity
    }

    /// 设置并发投递 PutLogs 请求的上限。提高可在网络抖动/慢响应时维持吞吐，
    /// 同时仍对发送速率形成有界背压（达到上限后才会反压采集），最小为 1，默认 4
    ///
    /// 越界值在构建时由 `normalized` 统一钳制
    pub fn max_inflight(mut self, n: usize) -> Self {
        self.max_inflight = n;
        self
    }

    /// 设置单批日志发送失败后的最大重试次数（指数退避），0 表示不重试，默认 5
    pub fn max_retries(mut self, n: u32) -> Self {
        self.max_retries = n;
        self
    }

    /// 设置重试退避的初始间隔，最小 1ms，默认 200ms
    ///
    /// 越界值在构建时由 `normalized` 统一钳制
    pub fn retry_base(mut self, d: Duration) -> Self {
        self.retry_base = d;
        self
    }

    /// 设置重试退避的上限间隔，最小 1ms，默认 10s
    ///
    /// 构建时由 `normalized` 统一钳制，并规整为不小于 `retry_base`
    pub fn retry_max(mut self, d: Duration) -> Self {
        self.retry_max = d;
        self
    }

    /// 设置单次 PutLogs 请求的超时时间，最小 1ms，默认 30s
    ///
    /// 越界值在构建时由 `normalized` 统一钳制
    pub fn request_timeout(mut self, d: Duration) -> Self {
        self.request_timeout = d;
        self
    }

    /// 设置优雅关闭的总时限：guard drop 时在该时限内排空队列并冲刷残留日志，最小 1ms，默认 15s
    ///
    /// 越界值在构建时由 `normalized` 统一钳制
    pub fn shutdown_timeout(mut self, d: Duration) -> Self {
        self.shutdown_timeout = d;
        self
    }

    /// 读取优雅关闭总时限，供上层（构建器/guard）计算等待截止时间
    pub(crate) fn shutdown_timeout_duration(&self) -> Duration {
        self.shutdown_timeout
    }

    /// 本 sink 目标 logstore 名称
    pub fn logstore(&self) -> &str {
        &self.logstore
    }

    /// 规整各项数值，确保它们落在有效范围（容量/批量 ≥ 1、各时长 ≥ 1ms、
    /// `retry_max ≥ retry_base`），并过滤空的 topic/source
    pub(crate) fn normalized(mut self) -> Self {
        // 容量与批量至少为 1
        self.max_batch_size = self.max_batch_size.max(1);
        self.queue_capacity = self.queue_capacity.max(1);
        self.max_inflight = self.max_inflight.max(1);
        // 各时长不低于 1ms
        self.flush_interval = self.flush_interval.max(MIN_DURATION);
        self.retry_base = self.retry_base.max(MIN_DURATION);
        // 退避上限不得小于初始间隔，否则退避序列无意义
        self.retry_max = self.retry_max.max(self.retry_base);
        self.request_timeout = self.request_timeout.max(MIN_DURATION);
        self.shutdown_timeout = self.shutdown_timeout.max(MIN_DURATION);
        // 单批字节数钳制在 [MIN_BATCH_BYTES, MAX_BATCH_BYTES_LIMIT]
        self.max_batch_bytes = self
            .max_batch_bytes
            .clamp(MIN_BATCH_BYTES, MAX_BATCH_BYTES_LIMIT);
        // STS 刷新间隔（若设置）不低于下限，避免频繁重建 client
        self.credentials_refresh_interval = self
            .credentials_refresh_interval
            .map(|d| d.max(MIN_CREDENTIALS_REFRESH_INTERVAL));
        // 空字符串的 topic/source 视为未设置，避免写出空字段
        self.topic = self.topic.filter(|s| !s.is_empty());
        self.source = self.source.filter(|s| !s.is_empty());
        self
    }
}

impl fmt::Debug for SlsConfig {
    /// 手写 Debug 以控制字段输出顺序与格式（凭据由 provider 持有，此处不展开明文）
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SlsConfig")
            .field("endpoint", &self.endpoint)
            .field("project", &self.project)
            .field("logstore", &self.logstore)
            .field("topic", &self.topic)
            .field("source", &self.source)
            .field("max_batch_size", &self.max_batch_size)
            .field("flush_interval", &self.flush_interval)
            .field("queue_capacity", &self.queue_capacity)
            .field("max_inflight", &self.max_inflight)
            .field("max_retries", &self.max_retries)
            .field("retry_base", &self.retry_base)
            .field("retry_max", &self.retry_max)
            .field("request_timeout", &self.request_timeout)
            .field("shutdown_timeout", &self.shutdown_timeout)
            .field("max_batch_bytes", &self.max_batch_bytes)
            .field("no_retry_status", &self.no_retry_status)
            .field("credentials_provider", &"<provider>")
            .field(
                "credentials_refresh_interval",
                &self.credentials_refresh_interval,
            )
            .field("credentials_refresh_ahead", &self.credentials_refresh_ahead)
            .field("overflow_policy", &self.overflow_policy)
            .finish()
    }
}

/// 一条待投递的日志记录：由上层从 tracing 事件转换而来
///
/// 字段键值使用 [`Arc<str>`] 以便在 tracing 热路径与队列之间共享，
/// 仅在批量发送前转换为 owned `String`
pub(crate) struct LogRecord {
    /// UNIX 秒（受 SLS `Log::from_unixtime` 约束为 u32）
    pub(crate) time: u32,
    /// 秒以下的纳秒余数
    pub(crate) time_ns: u32,
    /// 该条日志的全部 (key, value) 字段
    pub(crate) fields: Vec<(Arc<str>, Arc<str>)>,
}

/// 统一的装箱错误类型：可跨线程传递的动态错误对象
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// 按丢弃原因分类的原子计数器，供运行期统计与退出时汇总
#[derive(Debug, Default)]
pub(crate) struct DropCounters {
    /// 入队时队列已满而丢弃
    queue_full: AtomicU64,
    /// worker 已关闭（队列 closed）而丢弃
    worker_closed: AtomicU64,
    /// 重试用尽仍发送失败而丢弃
    send_failed: AtomicU64,
    /// 退出冲刷时限内未能送出而丢弃
    shutdown_dropped: AtomicU64,
}

impl DropCounters {
    /// 一次性读取四类计数并求和，生成不可变快照
    fn snapshot(&self) -> DropSnapshot {
        // 逐项 Relaxed 读取：计数器之间无需顺序一致性，仅用于统计展示
        let queue_full = self.queue_full.load(Ordering::Relaxed);
        let worker_closed = self.worker_closed.load(Ordering::Relaxed);
        let send_failed = self.send_failed.load(Ordering::Relaxed);
        let shutdown_dropped = self.shutdown_dropped.load(Ordering::Relaxed);

        DropSnapshot {
            total: queue_full + worker_closed + send_failed + shutdown_dropped,
            queue_full,
            worker_closed,
            send_failed,
            shutdown_dropped,
        }
    }

    /// 给某个计数器原子累加 `n`；`n == 0` 时跳过，省一次原子写
    fn add(counter: &AtomicU64, n: u64) {
        if n == 0 {
            return;
        }
        counter.fetch_add(n, Ordering::Relaxed);
    }

    /// 队列满丢弃 +1（单条事件触发，固定加 1）
    fn inc_queue_full(&self) {
        self.queue_full.fetch_add(1, Ordering::Relaxed);
    }

    /// worker 关闭丢弃 +n
    fn inc_worker_closed(&self, n: u64) {
        Self::add(&self.worker_closed, n);
    }

    /// 发送失败丢弃 +n
    fn inc_send_failed(&self, n: u64) {
        Self::add(&self.send_failed, n);
    }

    /// 退出冲刷丢弃 +n
    fn inc_shutdown_dropped(&self, n: u64) {
        Self::add(&self.shutdown_dropped, n);
    }
}

/// 丢弃计数的一次性快照：`total` 为四类之和，便于一次读取后展示或断言
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DropSnapshot {
    /// 四类丢弃之和
    pub total: u64,
    /// 队列满丢弃数
    pub queue_full: u64,
    /// worker 关闭丢弃数
    pub worker_closed: u64,
    /// 发送失败丢弃数
    pub send_failed: u64,
    /// 退出冲刷丢弃数
    pub shutdown_dropped: u64,
}

impl DropSnapshot {
    /// 把另一份快照逐项累加进自身，供多 sink 聚合统计复用
    pub(crate) fn merge(&mut self, other: &DropSnapshot) {
        self.total += other.total;
        self.queue_full += other.queue_full;
        self.worker_closed += other.worker_closed;
        self.send_failed += other.send_failed;
        self.shutdown_dropped += other.shutdown_dropped;
    }
}

/// 构建 sink/layer 过程中可能出现的错误
#[derive(Debug)]
pub enum SlsBuildError {
    /// 构建器未注册任何 sink
    NoSinks,
    /// SLS 客户端创建失败，内含底层错误
    Client {
        /// 底层客户端创建错误
        source: BoxError,
    },
    /// 后台 worker 线程启动失败
    WorkerThread(std::io::Error),
}

impl fmt::Display for SlsBuildError {
    /// 给出面向人类的中文错误描述
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoSinks => write!(f, "SlsLayerBuilder 未注册任何 sink"),
            Self::Client { source } => write!(f, "无法创建 SLS 客户端: {source}"),
            Self::WorkerThread(e) => write!(f, "无法启动 sls-tracing worker 线程: {e}"),
        }
    }
}

impl std::error::Error for SlsBuildError {
    /// 暴露底层错误以支持 `?` 链路与错误源追溯
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            // NoSinks 是逻辑错误，无底层来源
            Self::NoSinks => None,
            Self::Client { source } => Some(source.as_ref()),
            Self::WorkerThread(e) => Some(e),
        }
    }
}

/// 生产者与后台 worker 共享的有界日志队列：无锁环形缓冲 + 唤醒通知 + 关闭标志
///
/// 用 [`ArrayQueue`] 提供无锁有界入队，支持丢新（`push`）与丢旧（`force_push`）两种溢出策略；
/// 用 [`Notify`] 在入队后唤醒 worker，避免轮询
struct SinkQueue {
    /// 有界无锁环形缓冲
    ring: ArrayQueue<LogRecord>,
    /// 入队后唤醒 worker 的通知
    notify: Notify,
    /// 已穿过关闭检查、尚未完成 push/force_push 的生产者数量，供 close 等待这些入队完成
    active_enqueues: AtomicUsize,
    /// 关闭标志：置位后生产者不再入队，worker 排空后结束
    closed: AtomicBool,
}

impl SinkQueue {
    /// 创建指定容量的空队列（容量至少为 1）
    fn new(capacity: usize) -> Self {
        Self {
            ring: ArrayQueue::new(capacity.max(1)),
            notify: Notify::new(),
            active_enqueues: AtomicUsize::new(0),
            closed: AtomicBool::new(false),
        }
    }

    /// 关闭队列并唤醒 worker（使其能尽快进入排空收尾）
    ///
    /// 置位 closed 与随后读取 active_enqueues 均用 SeqCst：它与 [`ActiveEnqueue::enter`]
    /// 中「先自增再复检 closed」构成 Dekker 结构，只有单一全局顺序（SeqCst）才能排除
    /// store-buffering 松弛下「双方各读到对方旧值」的窗口，从而保证 close 返回后不再有已穿过
    /// 关闭检查的生产者继续入队
    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        while self.active_enqueues.load(Ordering::SeqCst) != 0 {
            std::hint::spin_loop();
            std::thread::yield_now();
        }
        self.notify.notify_waiters();
    }

    /// 队列是否已关闭
    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// 异步取一条日志；仅当队列已关闭且已排空时返回 `None`
    async fn recv(&self) -> Option<LogRecord> {
        loop {
            if let Some(record) = self.ring.pop() {
                return Some(record);
            }
            if self.is_closed() {
                return self.ring.pop();
            }
            // 先注册等待再重新检查队列，避免 push/notify 与此处交错时丢失唤醒
            let notified = self.notify.notified();
            if let Some(record) = self.ring.pop() {
                return Some(record);
            }
            if self.is_closed() {
                return self.ring.pop();
            }
            notified.await;
        }
    }
}

/// 入队临界区守卫：确保 close 能等待已穿过关闭检查的生产者完成入队
struct ActiveEnqueue<'a> {
    active_enqueues: &'a AtomicUsize,
}

impl<'a> ActiveEnqueue<'a> {
    fn enter(queue: &'a SinkQueue) -> Option<Self> {
        // 快速路径：已关闭直接放弃（Acquire 足够，仅作优化，正确性靠下面的 SeqCst 复检）
        if queue.is_closed() {
            return None;
        }
        // 先登记活跃入队，再复检 closed；自增与复检 load 均用 SeqCst，与 close 的
        // 「store(closed) 后 load(active)」构成单一全局顺序：若本入队读到 closed==false
        // 得以继续，则 close 必然在其 active_enqueues 上观察到本次自增并等待其结束
        queue.active_enqueues.fetch_add(1, Ordering::SeqCst);
        if queue.closed.load(Ordering::SeqCst) {
            queue.active_enqueues.fetch_sub(1, Ordering::AcqRel);
            return None;
        }
        Some(Self {
            active_enqueues: &queue.active_enqueues,
        })
    }
}

impl Drop for ActiveEnqueue<'_> {
    fn drop(&mut self) {
        self.active_enqueues.fetch_sub(1, Ordering::AcqRel);
    }
}

/// 一条完整、独立的 SLS 投递管线：自带 client、队列与后台 worker 线程，
/// 内部实现批量/定时冲刷、有界并发发送、指数退避重试与优雅关闭
///
/// 通过 [`crate::sls::layer::SlsLayerBuilder`] 组合多个 `SlsSink`，
/// 即可按级别把日志路由到不同 sink
pub struct SlsSink {
    /// 生产者与后台 worker 共享的有界队列
    queue: Arc<SinkQueue>,
    /// 队列满时的溢出丢弃策略
    policy: OverflowPolicy,
    /// 本 sink 的丢弃计数（与 worker 共享同一份）
    dropped: Arc<DropCounters>,
}

impl SlsSink {
    /// 创建并启动一条 sink（独立 client + 队列 + 后台 worker 线程）
    ///
    /// `index` 是该 sink 在构建器中的下标，`level_tag` 是其负责级别的紧凑标签
    /// （如 `EW` 表示 ERROR/WARN）。二者一起拼出**唯一且自解释**的线程名，便于在多条
    /// sink 指向同一 logstore 时仍能在调试器/panic 日志中区分各 worker 及其职责
    pub(crate) fn spawn(
        index: usize,
        level_tag: &str,
        config: SlsConfig,
    ) -> Result<(SlsSink, SinkHandle), SlsBuildError> {
        // 先规整配置，确保后续所有数值都落在有效范围
        let config = config.normalized();
        // 从凭据提供器取一组初始凭据并据此构建 client，任一步失败直接上抛
        let creds = config
            .credentials_provider
            .provide()
            .map_err(|source| SlsBuildError::Client { source })?;
        let client = build_client_with(&config, &creds)
            .map_err(|source| SlsBuildError::Client { source })?;
        // 有界队列：容量即背压点，满时按溢出策略丢弃
        let queue = Arc::new(SinkQueue::new(config.queue_capacity));
        // 关闭信号通道：广播 true 触发优雅关闭
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        // 完成通知通道：worker 排空结束后发一个 ()，guard 据此等待
        let (done_tx, done_rx) = std_mpsc::channel::<()>();
        let dropped = Arc::new(DropCounters::default());

        // worker 线程需要独立持有 config 与计数器的克隆/引用
        let worker_cfg = config.clone();
        let worker_dropped = Arc::clone(&dropped);
        let worker_queue = Arc::clone(&queue);
        let thread_name = thread_name_for_sink(index, level_tag, &config.logstore);
        // 为投递管线起一条命名 OS 线程；线程内部再建 current-thread runtime
        let handle = std::thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                run_sink(
                    worker_cfg,
                    client,
                    creds,
                    worker_queue,
                    shutdown_rx,
                    done_tx,
                    worker_dropped,
                )
            })
            .map_err(SlsBuildError::WorkerThread)?;

        // 入队句柄交给上层 Layer
        let sink = SlsSink {
            queue,
            policy: config.overflow_policy,
            dropped,
        };
        // 关闭句柄交给 guard
        let sink_handle = SinkHandle {
            shutdown: shutdown_tx,
            done: Some(done_rx),
            handle: Some(handle),
        };
        Ok((sink, sink_handle))
    }

    /// 非阻塞入队一条日志；队列满按溢出策略、worker 关闭时按原因分类计为丢弃
    pub(crate) fn enqueue(&self, record: LogRecord) {
        // 已关闭：不再入队，计为 worker 关闭丢弃
        let Some(_active) = ActiveEnqueue::enter(&self.queue) else {
            self.dropped.inc_worker_closed(1);
            return;
        };
        match self.policy {
            // 丢新：队列满则丢弃这条新日志，保留队列中较早的日志
            OverflowPolicy::DropNewest => {
                if self.queue.ring.push(record).is_err() {
                    self.dropped.inc_queue_full();
                    return;
                }
            }
            // 丢旧：队列满则挤掉队首最早的一条，容纳这条新日志
            OverflowPolicy::DropOldest => {
                if self.queue.ring.force_push(record).is_some() {
                    self.dropped.inc_queue_full();
                }
            }
        }
        // 唤醒 worker 处理新入队的日志
        self.queue.notify.notify_one();
    }

    /// 本 sink 的丢弃统计快照
    pub fn dropped_snapshot(&self) -> DropSnapshot {
        self.dropped.snapshot()
    }

    /// 取得丢弃计数的共享引用，供 guard 在退出时统一汇总
    pub(crate) fn drop_counters(&self) -> Arc<DropCounters> {
        Arc::clone(&self.dropped)
    }
}

/// 单条 sink 的后台句柄：关闭信号 + 完成通知 + 线程句柄
pub(crate) struct SinkHandle {
    /// 关闭信号发送端
    shutdown: watch::Sender<bool>,
    /// worker 完成通知接收端（取走后置 None）
    done: Option<std_mpsc::Receiver<()>>,
    /// worker 线程句柄（join 或放弃后置 None）
    handle: Option<JoinHandle<()>>,
}

/// 持有全部 sink 的后台句柄；drop（或显式 [`SlsGuard::shutdown_blocking`]）时
/// 并行触发所有 sink 的优雅关闭，并在超时内等待其冲刷完成
pub struct SlsGuard {
    /// 单条 sink 的关闭时限（取所有 sink 中的最大值）
    shutdown_timeout: Duration,
    /// 各 sink 的后台句柄
    sinks: Vec<SinkHandle>,
    /// 各 sink 的丢弃计数，用于退出时汇总打印
    drop_counters: Vec<Arc<DropCounters>>,
}

impl SlsGuard {
    /// 由构建器装配：传入统一关闭时限、各 sink 句柄与各 sink 计数器
    pub(crate) fn new(
        shutdown_timeout: Duration,
        sinks: Vec<SinkHandle>,
        drop_counters: Vec<Arc<DropCounters>>,
    ) -> Self {
        Self {
            shutdown_timeout,
            sinks,
            drop_counters,
        }
    }

    /// 显式触发阻塞式优雅关闭（与 drop 等价，但可在确定时机主动调用）
    pub fn shutdown_blocking(mut self) {
        self.shutdown_inner();
    }

    /// 关闭实现：并行广播关闭信号，再在统一截止时间内等待各 sink 收尾
    fn shutdown_inner(&mut self) {
        // 已无 sink（已关闭或本就为空）则直接返回，保证幂等
        if self.sinks.is_empty() {
            return;
        }
        // 取走 sinks，使后续再次调用（如 drop）成为 no-op
        let mut sinks = std::mem::take(&mut self.sinks);

        // 先并行广播关闭信号，让所有 sink 同时开始排空
        for sink in &sinks {
            let _ = sink.shutdown.send(true);
        }

        // 再在统一的截止时间内依次等待各 sink 收尾；因并行排空，总耗时约一个 shutdown_timeout
        // 额外 250ms 余量给 worker 收尾与通道传递留缓冲
        let deadline = Instant::now() + self.shutdown_timeout + Duration::from_millis(250);
        let mut timed_out = false;
        for sink in &mut sinks {
            // 取走完成通知接收端；没有则跳过（理论上不会发生）
            let Some(done) = sink.done.take() else {
                continue;
            };
            // 计算到统一 deadline 的剩余等待时长
            let remaining = deadline.saturating_duration_since(Instant::now());
            if !remaining.is_zero() && done.recv_timeout(remaining).is_ok() {
                // 收到完成通知：正常 join 回收线程
                if let Some(handle) = sink.handle.take() {
                    let _ = handle.join();
                }
            } else {
                // 超时或剩余为 0：放弃等待
                timed_out = true;
                // 超出 guard 等待上限：不再 join，worker 会在完成或进程退出时自行结束
                let _ = sink.handle.take();
            }
        }

        // 任一 sink 超时则给出整体告警
        if timed_out {
            eprintln!(
                "[sls-tracing] 退出冲刷超过 {:?}，部分后台 sink 未完成；剩余日志可能丢失",
                self.shutdown_timeout
            );
        }

        // 最后汇总并打印所有 sink 的累计丢弃情况
        report_dropped_if_any(&self.drop_counters);
    }
}

impl Drop for SlsGuard {
    /// 守卫离开作用域时自动触发优雅关闭，保证残留日志尽量被冲刷
    fn drop(&mut self) {
        self.shutdown_inner();
    }
}

/// worker 完成通知的兜底守卫：无论 worker 正常结束、runtime 创建失败还是 `block_on`
/// 内部 panic 展开，都在 drop 时向 guard 发出一次完成通知，避免 [`SlsGuard`] 一直等到超时
struct DoneOnDrop(Option<std_mpsc::Sender<()>>);

impl DoneOnDrop {
    fn new(tx: std_mpsc::Sender<()>) -> Self {
        Self(Some(tx))
    }
}

impl Drop for DoneOnDrop {
    fn drop(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(());
        }
    }
}

/// 单条 sink 的后台 worker 线程入口：在专属的 current-thread runtime 上运行投递管线
fn run_sink(
    config: SlsConfig,
    client: Client,
    initial_creds: SlsCredentials,
    queue: Arc<SinkQueue>,
    shutdown_rx: watch::Receiver<bool>,
    done_tx: std_mpsc::Sender<()>,
    dropped: Arc<DropCounters>,
) {
    // 兜底：无论正常结束、runtime 创建失败，还是 block_on 内部 panic 展开，
    // 都在函数退出时经由 drop 向 guard 发出完成通知，避免 guard 一直等到超时
    let _done = DoneOnDrop::new(done_tx);

    // 每条 sink 独占一个单线程 runtime，彼此资源隔离、互不争用
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            // runtime 都建不起来：放弃投递，但仍要把队列里的残留排空计数（完成通知由 _done 兜底）
            eprintln!("[sls-tracing] 无法创建运行时，停止该 sink 的日志投递: {e}");
            queue.close();
            let mut drained = 0;
            // 排空队列中已入队的记录并计为 worker_closed 丢弃
            while queue.ring.pop().is_some() {
                drained += 1;
            }
            dropped.inc_worker_closed(drained);
            return;
        }
    };

    // 在该 runtime 上阻塞运行投递管线；结束或 panic 展开后均由 _done 发出完成通知
    runtime.block_on(async move {
        // config/client 在多个并发发送任务间共享，用 Arc 包裹
        let config = Arc::new(config);
        let client = Arc::new(client);

        run_pipeline(
            client,
            queue,
            config,
            Arc::clone(&dropped),
            shutdown_rx,
            initial_creds,
        )
        .await;
    });
}

/// 当前累积批次：日志记录与其累计字节数一并维护，便于同时按条数与字节数判定冲刷
struct Batch {
    /// 已累积的日志记录
    records: Vec<LogRecord>,
    /// 已累积记录的估算字节数（各字段 key+value 长度之和）
    bytes: usize,
}

impl Batch {
    /// 预留一批容量创建空批次
    fn with_capacity(cap: usize) -> Self {
        Self {
            records: Vec::with_capacity(cap),
            bytes: 0,
        }
    }

    /// 测试辅助：追加一条记录并累加其估算字节数
    #[cfg(test)]
    fn push(&mut self, record: LogRecord) {
        let bytes = record_byte_size(&record);
        self.push_sized(record, bytes);
    }

    /// 追加一条已计算大小的记录，避免热路径重复估算
    fn push_sized(&mut self, record: LogRecord, bytes: usize) {
        self.bytes += bytes;
        self.records.push(record);
    }

    /// 当前批次若再追加指定大小的记录，是否会超过字节阈值
    fn would_exceed_bytes(&self, record_bytes: usize, config: &SlsConfig) -> bool {
        !self.records.is_empty()
            && self
                .bytes
                .saturating_add(record_bytes)
                .saturating_add(LOG_GROUP_PROTO_OVERHEAD_BYTES)
                > config.max_batch_bytes
    }

    /// 当前批次记录条数
    fn len(&self) -> usize {
        self.records.len()
    }

    /// 批次是否为空
    fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// 清空批次（记录与字节计数一并复位）
    fn clear(&mut self) {
        self.records.clear();
        self.bytes = 0;
    }

    /// 取走全部记录并把批次复位为空（字节计数同时归零）
    fn take(&mut self) -> Vec<LogRecord> {
        self.bytes = 0;
        std::mem::take(&mut self.records)
    }

    /// 是否达到冲刷阈值：条数或字节数任一达标即可
    fn ready(&self, config: &SlsConfig) -> bool {
        self.records.len() >= config.max_batch_size || self.bytes >= config.max_batch_bytes
    }
}

/// 估算一条记录的字节数：各字段 key 与 value 的 UTF-8 长度之和，再加上 protobuf 保守余量
///
/// 用于批量冲刷阈值判定，避免接近服务端单批上限
fn record_byte_size(record: &LogRecord) -> usize {
    LOG_PROTO_OVERHEAD_BYTES
        + record
            .fields
            .iter()
            .map(|(k, v)| k.len() + v.len() + CONTENT_PROTO_OVERHEAD_BYTES)
            .sum::<usize>()
}

/// 单条投递管线运行所需的共享句柄：把它们收进一处，避免在
/// `run_pipeline` / `drain_into_buf` / `spawn_flush` 之间逐个透传
struct Pipeline {
    /// SLS 客户端槽位；STS 刷新时整体替换 `Arc<Client>`，发送任务在派发瞬间取快照。
    /// 用 [`ArcSwap`] 实现无锁读写，避免每批发送都竞争互斥锁
    client: Arc<ArcSwap<Client>>,
    /// 本 sink 的运行期配置
    config: Arc<SlsConfig>,
    /// 本 sink 的丢弃计数
    dropped: Arc<DropCounters>,
    /// 限制并发 in-flight 请求数的信号量
    semaphore: Arc<Semaphore>,
    /// 已派发但尚未完成的记录数，用于 shutdown abort 前准确补记丢弃
    pending_records: Arc<AtomicU64>,
}

impl Pipeline {
    /// 取当前 client 的快照（clone `Arc<Client>`），供单个发送任务在其生命周期内稳定使用
    fn client_snapshot(&self) -> Arc<Client> {
        self.client.load_full()
    }

    /// 取走 `batch` 中的日志，组装为 `LogGroup` 并派发一个并发发送任务
    ///
    /// 通过信号量限制 in-flight 请求数：达到上限时 `acquire_owned` 会在此 await，
    /// 从而对采集侧形成有界背压；获取到许可后任务被 spawn，主循环可立即继续收集
    async fn spawn_flush(
        &self,
        batch: &mut Batch,
        inflight: &mut JoinSet<()>,
        shutdown_rx: &watch::Receiver<bool>,
        deadline: Option<Instant>,
    ) -> FlushResult {
        // 空批次无需发送
        if batch.is_empty() {
            return FlushResult::Flushed;
        }
        let count = batch.len() as u64;

        // 先获取并发许可再取走整批：正常模式下等待许可期间仍能响应关闭信号；收到关闭时保留
        // batch 交由收尾流程带 deadline 冲刷，而非在此直接丢弃。达到 max_inflight 时此处 await
        // 会暂停外层 select! 主循环，从而对采集侧形成有界背压；关闭阶段则受 deadline 约束
        let permit = match deadline {
            Some(d) => {
                let remaining = d.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    self.dropped.inc_shutdown_dropped(count);
                    batch.clear();
                    return FlushResult::Dropped;
                }
                match tokio::time::timeout(remaining, Arc::clone(&self.semaphore).acquire_owned())
                    .await
                {
                    Ok(Ok(permit)) => permit,
                    Ok(Err(_)) => {
                        self.dropped.inc_send_failed(count);
                        batch.clear();
                        return FlushResult::Dropped;
                    }
                    Err(_) => {
                        self.dropped.inc_shutdown_dropped(count);
                        batch.clear();
                        return FlushResult::Dropped;
                    }
                }
            }
            None => {
                // 正常模式：许可获取与关闭信号二选一，避免故障期（in-flight 全部占满）在此
                // 无限等待而错过关闭；收到关闭则保留 batch，返回让主循环进入带 deadline 的收尾
                let mut shutdown = shutdown_rx.clone();
                tokio::select! {
                    permit = Arc::clone(&self.semaphore).acquire_owned() => match permit {
                        Ok(permit) => permit,
                        Err(_) => {
                            // 信号量被关闭（理论上不会发生），保守计为发送失败
                            self.dropped.inc_send_failed(count);
                            batch.clear();
                            return FlushResult::Dropped;
                        }
                    },
                    changed = shutdown.changed() => {
                        // 收到关闭信号：不在此丢弃，保留 batch 交由收尾流程带 deadline 冲刷
                        let _ = changed;
                        return FlushResult::DeferredForShutdown;
                    }
                }
            }
        };

        // 已拿到许可，取走整批记录，batch 复位以继续累积下一批
        let records = batch.take();

        // 为发送任务克隆所需共享句柄（Arc/watch 克隆都很廉价）；client 取当前快照
        let client = self.client_snapshot();
        let config = Arc::clone(&self.config);
        let dropped = Arc::clone(&self.dropped);
        let pending_records = Arc::clone(&self.pending_records);
        pending_records.fetch_add(count, Ordering::Relaxed);
        let mut shutdown_rx = shutdown_rx.clone();
        inflight.spawn(async move {
            let _pending = PendingBatch::new(pending_records, count);
            // 持有 permit 到任务结束，drop 时自动归还并发额度
            let _permit = permit;
            // 持有 records（protobuf 编码在阻塞线程池进行）；happy-path 不做整批 clone
            send_with_retry(
                client,
                config,
                records,
                count,
                &dropped,
                deadline,
                &mut shutdown_rx,
            )
            .await;
        });
        FlushResult::Flushed
    }

    /// 在 deadline 之前持续从队列中取出日志放入 `batch`，达到批量阈值即派发发送任务
    async fn drain_into_buf(
        &self,
        queue: &SinkQueue,
        batch: &mut Batch,
        inflight: &mut JoinSet<()>,
        shutdown_rx: &watch::Receiver<bool>,
        deadline: Instant,
    ) {
        loop {
            // 超过截止时间立即停止排空，剩余交由上层计为丢弃
            if Instant::now() >= deadline {
                return;
            }
            // 非阻塞取：队列空即返回
            match queue.ring.pop() {
                Some(record) => {
                    if self
                        .push_or_drop(record, batch, inflight, shutdown_rx, Some(deadline))
                        .await
                        .is_some()
                    {
                        return;
                    }
                    // 攒满一批（条数或字节任一达标）就派发，避免单批过大或内存堆积
                    if batch.ready(&self.config) {
                        match self
                            .spawn_flush(batch, inflight, shutdown_rx, Some(deadline))
                            .await
                        {
                            FlushResult::Flushed => {}
                            FlushResult::DeferredForShutdown | FlushResult::Dropped => return,
                        }
                    }
                }
                // 队列已空：排空结束
                None => return,
            }
        }
    }

    /// 根据批量字节阈值安全追加一条记录；必要时先冲刷当前批次，单条超限则丢弃并计数
    async fn push_or_drop(
        &self,
        record: LogRecord,
        batch: &mut Batch,
        inflight: &mut JoinSet<()>,
        shutdown_rx: &watch::Receiver<bool>,
        deadline: Option<Instant>,
    ) -> Option<LogRecord> {
        let record_bytes = record_byte_size(&record);
        if record_bytes.saturating_add(LOG_GROUP_PROTO_OVERHEAD_BYTES) > self.config.max_batch_bytes
        {
            self.dropped.inc_send_failed(1);
            eprintln!(
                "[sls-tracing] 单条日志估算大小 {}B 超过单批上限 {}B，已丢弃 1 条日志",
                record_bytes, self.config.max_batch_bytes
            );
            return None;
        }

        if batch.would_exceed_bytes(record_bytes, &self.config) {
            match self
                .spawn_flush(batch, inflight, shutdown_rx, deadline)
                .await
            {
                FlushResult::Flushed => {}
                FlushResult::DeferredForShutdown => return Some(record),
                FlushResult::Dropped => {
                    if deadline.is_some() {
                        // 关闭收尾模式：整批已在 spawn_flush 内计入退出丢弃，这条也一并计入
                        self.dropped.inc_shutdown_dropped(1);
                    } else {
                        self.dropped.inc_send_failed(1);
                    }
                    return None;
                }
            }
        }
        batch.push_sized(record, record_bytes);
        None
    }
}

/// 批次冲刷尝试的结果，用于区分“已派发”“因关闭延后”和“已计数丢弃”三种路径
enum FlushResult {
    /// 批次为空或已派发发送任务
    Flushed,
    /// 正常运行期收到关闭信号，保留批次交由 shutdown deadline 路径处理
    DeferredForShutdown,
    /// 批次未能派发，且已经按对应原因计入丢弃
    Dropped,
}

/// in-flight 批次计数守卫：任务正常完成或被 abort 时都会归还 pending 记录数
struct PendingBatch {
    pending_records: Arc<AtomicU64>,
    count: u64,
}

impl PendingBatch {
    fn new(pending_records: Arc<AtomicU64>, count: u64) -> Self {
        Self {
            pending_records,
            count,
        }
    }
}

impl Drop for PendingBatch {
    fn drop(&mut self) {
        self.pending_records
            .fetch_sub(self.count, Ordering::Relaxed);
    }
}

/// 单条投递管线：消费一个队列，批量/定时冲刷，并发发送，优雅关闭时在 deadline 内排空
async fn run_pipeline(
    client: Arc<Client>,
    queue: Arc<SinkQueue>,
    config: Arc<SlsConfig>,
    dropped: Arc<DropCounters>,
    mut shutdown_rx: watch::Receiver<bool>,
    initial_creds: SlsCredentials,
) {
    // 每条管线持有自己的并发额度，与共享句柄一并收进 Pipeline
    let semaphore = Arc::new(Semaphore::new(config.max_inflight));
    let pipeline = Pipeline {
        // client 放入 ArcSwap 槽位，供 STS 刷新时无锁整体替换
        client: Arc::new(ArcSwap::new(client)),
        config,
        dropped,
        semaphore,
        pending_records: Arc::new(AtomicU64::new(0)),
    };

    // 后台自调度凭据刷新任务：按凭据有效期/回退间隔到点刷新并热替换 client。首个凭据已在
    // spawn 中同步获取，故据其安排「下一次」刷新而非立即拉取；静态 AccessKey（无有效期且未设
    // 间隔）会直接结束、永不刷新。该任务随 worker runtime 释放而取消，无需在主循环里参与调度
    spawn_credentials_refresh_loop(
        Arc::clone(&pipeline.config),
        Arc::clone(&pipeline.client),
        initial_creds,
    );

    // 已派发但未完成的发送任务集合，用于回收与退出等待
    let mut inflight: JoinSet<()> = JoinSet::new();
    // 当前累积批次的缓冲，预留一批容量减少扩容
    let mut batch = Batch::with_capacity(pipeline.config.max_batch_size);
    let mut shutting_down = false;
    let mut deferred_record = None;
    // 下一次定时冲刷的截止时刻
    let mut flush_deadline = tokio::time::Instant::now() + pipeline.config.flush_interval;

    // ===== 正常运行：多路 select 同时驱动收取、定时冲刷、任务回收与关闭 =====
    while !shutting_down {
        tokio::select! {
            // 收到关闭信号：标记进入收尾流程
            changed = shutdown_rx.changed() => {
                // changed 出错说明发送端已 drop，同样视作关闭
                if changed.is_err() || *shutdown_rx.borrow() {
                    shutting_down = true;
                }
            }

            // 从队列取到一条日志
            maybe = queue.recv() => {
                match maybe {
                    Some(record) => {
                        if let Some(record) = pipeline
                            .push_or_drop(
                                record,
                                &mut batch,
                                &mut inflight,
                                &shutdown_rx,
                                None,
                            )
                            .await
                        {
                            deferred_record = Some(record);
                            shutting_down = true;
                        }
                        // 攒满一批（条数或字节任一达标）立即冲刷，并重置定时器
                        if !shutting_down && batch.ready(&pipeline.config) {
                            match pipeline
                                .spawn_flush(&mut batch, &mut inflight, &shutdown_rx, None)
                                .await
                            {
                                FlushResult::Flushed | FlushResult::Dropped => {
                                    flush_deadline = tokio::time::Instant::now()
                                        + pipeline.config.flush_interval;
                                }
                                FlushResult::DeferredForShutdown => shutting_down = true,
                            }
                        }
                    }
                    // 队列关闭（所有发送端已 drop）：进入收尾
                    None => shutting_down = true,
                }
            }

            // 到达定时冲刷时刻：即使未满也冲刷一次，并重置定时器
            _ = tokio::time::sleep_until(flush_deadline) => {
                match pipeline
                    .spawn_flush(&mut batch, &mut inflight, &shutdown_rx, None)
                    .await
                {
                    FlushResult::Flushed | FlushResult::Dropped => {
                        flush_deadline = tokio::time::Instant::now()
                            + pipeline.config.flush_interval;
                    }
                    FlushResult::DeferredForShutdown => shutting_down = true,
                }
            }

            // 回收已完成的发送任务，避免 JoinSet 无限堆积
            Some(_) = inflight.join_next(), if !inflight.is_empty() => {}
        }
    }

    // ===== 优雅关闭：在 deadline 内排空本管线队列并冲刷残留 =====
    let deadline = Instant::now() + pipeline.config.shutdown_timeout;
    // 关闭入队侧，确保不再有新日志进入，且能继续 pop 排空已有
    queue.close();
    if let Some(record) = deferred_record.take() {
        pipeline
            .push_or_drop(
                record,
                &mut batch,
                &mut inflight,
                &shutdown_rx,
                Some(deadline),
            )
            .await;
        if batch.ready(&pipeline.config) {
            pipeline
                .spawn_flush(&mut batch, &mut inflight, &shutdown_rx, Some(deadline))
                .await;
        }
    }
    // 在截止时间内把队列里剩余日志取入 batch，达到批量阈值即派发
    pipeline
        .drain_into_buf(&queue, &mut batch, &mut inflight, &shutdown_rx, deadline)
        .await;

    // 处理排空后 batch 中不足一批的尾部数据
    if !batch.is_empty() {
        if Instant::now() < deadline {
            // 仍在时限内：派发最后一批，并带上 deadline 让发送任务自我约束
            pipeline
                .spawn_flush(&mut batch, &mut inflight, &shutdown_rx, Some(deadline))
                .await;
        } else {
            // 时限已过：直接计为退出丢弃
            pipeline.dropped.inc_shutdown_dropped(batch.len() as u64);
            batch.clear();
        }
    }

    // 仍滞留在队列中的（drain 超时未取出的）也计为退出丢弃
    let queued = queue.ring.len();
    if queued > 0 {
        pipeline.dropped.inc_shutdown_dropped(queued as u64);
    }

    // 等待已派发的发送任务在 deadline 内完成
    // 每个任务自身也带 deadline，会按时放弃并正确计数；这里仅做总时限兜底
    while !inflight.is_empty() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        // 时限耗尽则不再等待
        if remaining.is_zero() {
            break;
        }
        // 在剩余时间内等待下一个任务结束；超时则跳出
        if tokio::time::timeout(remaining, inflight.join_next())
            .await
            .is_err()
        {
            break;
        }
    }
    // 强制中止仍未结束的任务，确保函数能干净返回
    if !inflight.is_empty() {
        let pending = pipeline.pending_records.load(Ordering::Relaxed);
        if pending > 0 {
            pipeline.dropped.inc_shutdown_dropped(pending);
            eprintln!("[sls-tracing] 退出冲刷中止 {pending} 条仍在发送的日志");
        }
    }
    inflight.shutdown().await;
}

/// 把一批 [`LogRecord`] 组装为 SDK 的 [`LogGroup`]，套用 topic/source
fn build_log_group(config: &SlsConfig, records: &[LogRecord]) -> LogGroup {
    let mut group = LogGroup::new();
    // 可选的 topic/source 只在配置存在时写入
    if let Some(topic) = &config.topic {
        group.set_topic(topic.clone());
    }
    if let Some(source) = &config.source {
        group.set_source(source.clone());
    }
    // 逐条转换：Arc<str> 在此处转为 SDK 需要的 owned String
    for record in records {
        let mut log = Log::from_unixtime(record.time);
        log.set_time_ns(record.time_ns);
        for (key, value) in &record.fields {
            log.add_content_kv(key.to_string(), value.to_string());
        }
        group.add_log(log);
    }
    group
}

/// 依据配置与一组凭据构建 SLS 客户端：`creds` 不带 SecurityToken 时以长期 AccessKey 鉴权，
/// 带 SecurityToken 时以 STS 方式鉴权（供启动构建与刷新热替换 client 共用）
fn build_client_with(config: &SlsConfig, creds: &SlsCredentials) -> Result<Client, BoxError> {
    // 基础项：接入点与单次请求超时
    let builder = Config::builder()
        .endpoint(config.endpoint.clone())
        .request_timeout(config.request_timeout);

    // 无 SecurityToken 视为长期 AccessKey，否则用 STS 临时凭据
    let builder = if creds.security_token.is_empty() {
        builder.access_key(creds.access_key_id.clone(), creds.access_key_secret.clone())
    } else {
        builder.sts(
            creds.access_key_id.clone(),
            creds.access_key_secret.clone(),
            creds.security_token.clone(),
        )
    };

    // 构建配置并据此创建客户端，任一步失败均上抛
    let cfg = builder.build()?;
    let client = Client::from_config(cfg)?;
    Ok(client)
}

/// 依据刷新到的凭据计算「距下一次刷新的等待时长」，`None` 表示无需再刷新：
///
/// - 凭据带 `expire_time`：在其到期前 `credentials_refresh_ahead` 刷新；已经进入提前刷新窗口时，
///   使用快速刷新间隔，避免继续沿用临期或过期 client；
/// - 凭据不带 `expire_time`：回退到 `credentials_refresh_interval`；未设置该项（长期
///   AccessKey 场景）则返回 `None`，不再周期刷新。
fn refresh_delay_for(config: &SlsConfig, creds: &SlsCredentials) -> Option<Duration> {
    match creds.expire_time {
        Some(expire) => {
            // 距到期还有多久（已过期或时钟回拨则视为 0）
            let until_expire = expire
                .duration_since(SystemTime::now())
                .unwrap_or(Duration::ZERO);
            // 已进入提前刷新窗口时快速刷新，避免继续沿用即将/已经过期的凭据
            let delay = until_expire.saturating_sub(config.credentials_refresh_ahead);
            Some(if delay.is_zero() {
                URGENT_CREDENTIALS_REFRESH_INTERVAL
            } else {
                delay
            })
        }
        // 无有效期信息：按固定回退间隔（若设置，已在 normalized 中不低于最小间隔）
        None => config.credentials_refresh_interval,
    }
}

/// 派发一个自调度的后台凭据刷新任务：按当前凭据的有效期/回退间隔 sleep 到点后，在
/// `spawn_blocking` 中调用提供器拉取新凭据，成功则重建并热替换 client，再据新凭据安排
/// 下一次刷新，如此循环。顺序循环天然不会重叠刷新，故无需额外的「刷新中」标记。
///
/// - 凭据无有效期且未设回退间隔（如长期 AccessKey）：[`refresh_delay_for`] 返回 `None`，
///   直接不派发后台任务、永不刷新；
/// - 提供器出错 / 重建失败 / 阻塞任务 panic：保留旧 client，按最短间隔（60s）重试；
/// - 管线收尾、worker runtime 释放时该任务随之取消。
fn spawn_credentials_refresh_loop(
    config: Arc<SlsConfig>,
    client_slot: Arc<ArcSwap<Client>>,
    initial_creds: SlsCredentials,
) {
    // 以启动时的凭据算出首次刷新等待；None（如长期 AccessKey 无有效期且未设间隔）表示无需刷新，
    // 直接不派发后台任务
    let Some(mut delay) = refresh_delay_for(&config, &initial_creds) else {
        return;
    };

    tokio::spawn(async move {
        loop {
            tokio::time::sleep(delay).await;

            // 提供器可能进行阻塞式网络请求，放到阻塞线程池执行，避免拖住 worker runtime
            let provider = Arc::clone(&config.credentials_provider);
            delay = match tokio::task::spawn_blocking(move || provider.provide()).await {
                // 拿到新凭据：重建 client 并原子替换槽位，据新凭据安排下一次刷新
                Ok(Ok(creds)) => match build_client_with(&config, &creds) {
                    Ok(client) => {
                        client_slot.store(Arc::new(client));
                        match refresh_delay_for(&config, &creds) {
                            Some(d) => d,
                            // 新凭据表明无需再刷新（无有效期且未设回退间隔）
                            None => return,
                        }
                    }
                    Err(e) => {
                        eprintln!("[sls-tracing] 凭据刷新后重建 client 失败，继续沿用旧凭据: {e}");
                        MIN_CREDENTIALS_REFRESH_INTERVAL
                    }
                },
                // 提供器返回错误：保留旧 client，最短间隔后再试
                Ok(Err(e)) => {
                    eprintln!("[sls-tracing] 凭据提供器返回错误，继续沿用旧凭据: {e}");
                    MIN_CREDENTIALS_REFRESH_INTERVAL
                }
                // 阻塞任务 panic：同样保留旧 client，最短间隔后再试
                Err(e) => {
                    eprintln!("[sls-tracing] 凭据刷新任务异常终止，继续沿用旧凭据: {e}");
                    MIN_CREDENTIALS_REFRESH_INTERVAL
                }
            };
        }
    });
}

/// 发送一批日志，失败按指数退避重试；关闭/超时场景下受 deadline 约束并正确计数
async fn send_with_retry(
    client: Arc<Client>,
    config: Arc<SlsConfig>,
    records: Vec<LogRecord>,
    count: u64,
    dropped: &DropCounters,
    mut deadline: Option<Instant>,
    shutdown_rx: &mut watch::Receiver<bool>,
) {
    // 已发送尝试次数（不含首发，首发为 attempt 0）
    let mut attempt = 0u32;
    // 整批记录共享给（可能多次的）编码任务，避免每次 attempt 移动/整批拷贝
    let records = Arc::new(records);

    loop {
        // 进入每次尝试前先检查 deadline，超时直接按退出丢弃计数
        if deadline_expired(deadline) {
            drop_shutdown_batch(count, dropped, "退出冲刷时间耗尽");
            return;
        }

        // protobuf 编码为 CPU 密集操作：放到阻塞线程池执行，避免占用 worker 的单线程
        // runtime，使一批的编码可与其它批次的网络发送在不同线程上重叠。put_logs 会消费
        // group，故每次 attempt 都重新编码；happy-path 仅编码一次
        let group = {
            let config = Arc::clone(&config);
            let records = Arc::clone(&records);
            match tokio::task::spawn_blocking(move || build_log_group(&config, &records)).await {
                Ok(group) => group,
                Err(e) => {
                    // 编码任务异常终止：无法发送，计为发送失败
                    dropped.inc_send_failed(count);
                    eprintln!("[sls-tracing] 日志编码任务异常终止，丢弃 {count} 条日志: {e}");
                    return;
                }
            }
        };

        // 组装并发起一次 PutLogs 请求
        let send_fut = client
            .put_logs(config.project.as_str(), config.logstore.as_str())
            .log_group(group)
            .send();

        // 等待请求结果；期间若收到关闭信号会切换到带 deadline 的冲刷模式
        let result = await_put_logs(
            send_fut,
            &mut deadline,
            config.shutdown_timeout,
            shutdown_rx,
        )
        .await;

        match result {
            // 发送成功
            Ok(()) => return,
            // 关闭冲刷期间请求超时：计为退出丢弃
            Err(PutLogsOutcome::DeadlineExceeded) => {
                drop_shutdown_batch(count, dropped, "退出冲刷请求超时");
                return;
            }
            // 发送失败：尝试退避重试
            Err(PutLogsOutcome::Failed(e)) => {
                // 命中不重试状态码（如 400/404）：重试无意义，直接计为发送失败丢弃
                if is_no_retry_error(&e, &config.no_retry_status) {
                    dropped.inc_send_failed(count);
                    eprintln!("[sls-tracing] PutLogs 返回不可重试的错误，丢弃 {count} 条日志: {e}");
                    return;
                }
                attempt += 1;
                // 重试次数用尽：计为发送失败丢弃并告警
                if attempt > config.max_retries {
                    dropped.inc_send_failed(count);
                    eprintln!(
                        "[sls-tracing] PutLogs 重试 {attempt} 次仍失败，丢弃 {count} 条日志: {e}"
                    );
                    return;
                }
                // 退避等待；返回 false 表示 deadline 已耗尽，按退出丢弃处理
                if !sleep_backoff(&config, attempt, &mut deadline, shutdown_rx).await {
                    drop_shutdown_batch(count, dropped, "退出冲刷时间耗尽");
                    return;
                }
            }
        }
    }
}

/// 一次 PutLogs 等待的失败结果：要么 deadline 超时，要么底层发送出错
enum PutLogsOutcome {
    /// 在关闭冲刷的 deadline 前未完成
    DeadlineExceeded,
    /// 底层请求返回错误
    Failed(BoxError),
}

/// 判断可选 deadline 是否已到期；`None` 视为永不到期
fn deadline_expired(deadline: Option<Instant>) -> bool {
    deadline.is_some_and(|d| Instant::now() >= d)
}

/// 判断一次发送错误是否命中「不重试」名单：仅对服务端返回且 HTTP 状态码在 `no_retry_status`
/// 中的错误生效；网络错误、配置错误等仍走正常重试
fn is_no_retry_error(err: &BoxError, no_retry_status: &[u32]) -> bool {
    // 名单为空表示所有失败都参与重试
    if no_retry_status.is_empty() {
        return false;
    }
    // 还原为 SDK 的具体错误类型，取出服务端返回的 HTTP 状态码
    match err.downcast_ref::<SlsError>() {
        Some(SlsError::Server { http_status, .. }) => no_retry_status.contains(http_status),
        _ => false,
    }
}

/// 把一整批计为“退出冲刷丢弃”并打印原因，统一收尾路径的丢弃处理
fn drop_shutdown_batch(count: u64, dropped: &DropCounters, reason: &str) {
    dropped.inc_shutdown_dropped(count);
    eprintln!("[sls-tracing] {reason}，丢弃 {count} 条日志");
}

/// 等待 PutLogs 完成；若收到 shutdown 信号则切换到带 deadline 的冲刷模式
async fn await_put_logs<F, T, E>(
    send_fut: F,
    deadline: &mut Option<Instant>,
    shutdown_timeout: Duration,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> Result<(), PutLogsOutcome>
where
    F: std::future::Future<Output = Result<T, E>>,
    E: Into<BoxError>,
{
    // 固定 future 以便在 loop 中多次 &mut 轮询而不重启请求
    pin!(send_fut);

    loop {
        match *deadline {
            // 已有 deadline（关闭冲刷模式）：发送与 deadline 二选一，不再监听关闭信号
            Some(d) => {
                return tokio::select! {
                    result = &mut send_fut => match result {
                        Ok(_) => Ok(()),
                        Err(e) => Err(PutLogsOutcome::Failed(e.into())),
                    },
                    // 到达 deadline 即放弃本次请求
                    _ = tokio::time::sleep_until(tokio::time::Instant::from_std(d)) => {
                        Err(PutLogsOutcome::DeadlineExceeded)
                    }
                };
            }
            // 尚无 deadline（正常运行）：发送与关闭信号二选一
            None => {
                tokio::select! {
                    result = &mut send_fut => {
                        return match result {
                            Ok(_) => Ok(()),
                            Err(e) => Err(PutLogsOutcome::Failed(e.into())),
                        };
                    }
                    // 收到关闭信号：设定 deadline 后回到 loop，转入带时限的等待
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            *deadline = Some(Instant::now() + shutdown_timeout);
                        }
                    }
                }
            }
        }
    }
}

/// 指数退避；返回 `false` 表示 deadline 已耗尽
async fn sleep_backoff(
    config: &SlsConfig,
    attempt: u32,
    deadline: &mut Option<Instant>,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> bool {
    // 计算本次退避时长（随 attempt 指数增长，封顶 retry_max）
    let backoff = jittered_backoff_delay(backoff_delay(config, attempt));
    match *deadline {
        // 关闭冲刷模式：退避不得超过剩余时限
        Some(d) => {
            let remaining = d.saturating_duration_since(Instant::now());
            // 剩余为 0 说明时限耗尽，放弃重试
            if remaining.is_zero() {
                return false;
            }
            // 取退避与剩余时间的较小值，避免睡过 deadline
            tokio::time::sleep(backoff.min(remaining)).await;
            true
        }
        // 正常模式：退避期间仍监听关闭信号以尽快进入收尾
        None => {
            tokio::select! {
                _ = tokio::time::sleep(backoff) => true,
                changed = shutdown_rx.changed() => {
                    // 收到关闭信号则设定 deadline，使后续尝试受时限约束
                    if changed.is_err() || *shutdown_rx.borrow() {
                        *deadline = Some(Instant::now() + config.shutdown_timeout);
                    }
                    true
                }
            }
        }
    }
}

/// 拼出 sink 的 worker 线程名：`sls-sink-{index}-{level_tag}-{logstore}`
///
/// `index` 保证唯一（多条 sink 指向同一 logstore 时线程名也不重复），`level_tag`
/// 标明该 sink 负责的级别（如 `EW`=ERROR/WARN），使线程名自解释、便于排障
fn thread_name_for_sink(index: usize, level_tag: &str, logstore: &str) -> String {
    // logstore 可能含非法字符，截断至 32 字符并把非 [字母数字-_] 替换为 _
    let sanitized: String = logstore
        .chars()
        .take(32)
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_') {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!("sls-sink-{index}-{level_tag}-{sanitized}")
}

/// 计算第 `attempt` 次重试的退避时长：`retry_base * 2^(attempt-1)`，封顶 `retry_max`
fn backoff_delay(config: &SlsConfig, attempt: u32) -> Duration {
    // 移位次数封顶 16，防止 1u64 << shift 溢出
    let shift = (attempt - 1).min(16);
    let factor = 1u64 << shift;
    // 用 u128 + saturating 计算，避免毫秒乘法溢出
    let millis = config.retry_base.as_millis().saturating_mul(factor as u128);
    // 不超过配置的退避上限
    let capped = millis.min(config.retry_max.as_millis());
    Duration::from_millis(capped as u64)
}

/// equal jitter：保留一半退避下限，另一半随机打散，避免多实例同步重试冲击 SLS
fn jittered_backoff_delay(base: Duration) -> Duration {
    let base_millis = base.as_millis() as u64;
    if base_millis <= 1 {
        return base;
    }
    let half = base_millis / 2;
    Duration::from_millis(half + next_jitter_u64(base_millis - half + 1))
}

fn next_jitter_u64(upper_exclusive: u64) -> u64 {
    if upper_exclusive == 0 {
        return 0;
    }
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let seed = JITTER_SEED.fetch_add(0x9e37_79b9_7f4a_7c15, Ordering::Relaxed) ^ now;
    let mixed = seed
        .wrapping_mul(0xbf58_476d_1ce4_e5b9)
        .rotate_left(17)
        .wrapping_mul(0x94d0_49bb_1331_11eb);
    mixed % upper_exclusive
}

/// 把多条 sink 的丢弃计数合并为一个总快照
fn aggregate_drop_snapshot(counters: &[Arc<DropCounters>]) -> DropSnapshot {
    let mut agg = DropSnapshot::default();
    // 逐 sink 累加四类计数与总数
    for counter in counters {
        agg.merge(&counter.snapshot());
    }
    agg
}

/// 若存在任何丢弃，按分类打印一条汇总告警；无丢弃时静默
pub(crate) fn report_dropped_if_any(counters: &[Arc<DropCounters>]) {
    let snapshot = aggregate_drop_snapshot(counters);
    // 仅在确有丢弃时输出，避免无谓噪声
    if snapshot.total > 0 {
        eprintln!(
            "[sls-tracing] 已累计丢弃 {} 条日志（queue_full={}, worker_closed={}, send_failed={}, shutdown_dropped={}）",
            snapshot.total,
            snapshot.queue_full,
            snapshot.worker_closed,
            snapshot.send_failed,
            snapshot.shutdown_dropped,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_is_capped() {
        // 验证退避序列：200ms、400ms…… 并在 retry_max 处封顶为 10s
        let config = SlsConfig::new(
            "endpoint",
            "project",
            "logstore",
            StaticCredentialsProvider::new("ak", "sk"),
        );
        assert_eq!(backoff_delay(&config, 1), Duration::from_millis(200));
        assert_eq!(backoff_delay(&config, 2), Duration::from_millis(400));
        assert_eq!(backoff_delay(&config, 20), Duration::from_secs(10));
    }

    /// 用给定字段构造一条测试记录
    fn record_with_fields(fields: &[(&str, &str)]) -> LogRecord {
        LogRecord {
            time: 0,
            time_ns: 0,
            fields: fields
                .iter()
                .map(|(k, v)| (Arc::from(*k), Arc::from(*v)))
                .collect(),
        }
    }

    #[test]
    fn batch_ready_by_count_or_bytes() {
        // 条数阈值 2、字节阈值 2048（高于 normalized 下限）：任一达标即 ready
        let config = SlsConfig::new("e", "p", "l", StaticCredentialsProvider::new("ak", "sk"))
            .max_batch_size(2)
            .max_batch_bytes(2048)
            .normalized();

        // 单条小日志：都未达标
        let mut batch = Batch::with_capacity(4);
        batch.push(record_with_fields(&[("k", "v")]));
        assert!(!batch.ready(&config));

        // 第二条使条数达标
        batch.push(record_with_fields(&[("k", "v")]));
        assert!(batch.ready(&config));

        // 单条大日志（字节 >= 2048）立即达标
        let mut big = Batch::with_capacity(4);
        big.push(record_with_fields(&[("key", &"x".repeat(3000))]));
        assert!(big.ready(&config));

        // take 后计数与字节归零
        let taken = big.take();
        assert_eq!(taken.len(), 1);
        assert!(big.is_empty());
        assert!(!big.ready(&config));
    }

    #[test]
    fn record_byte_size_sums_key_value_len() {
        let record = record_with_fields(&[("ab", "cde"), ("f", "gh")]);
        // protobuf 余量 + 两个字段的 key/value 长度与字段开销
        assert_eq!(
            record_byte_size(&record),
            LOG_PROTO_OVERHEAD_BYTES + 8 + 2 * CONTENT_PROTO_OVERHEAD_BYTES
        );
    }

    #[test]
    fn normalized_clamps_batch_bytes_and_refresh_interval() {
        // 过小 batch_bytes 被抬到下限，过大被压到上限
        let low = SlsConfig::new("e", "p", "l", StaticCredentialsProvider::new("ak", "sk"))
            .max_batch_bytes(0)
            .normalized();
        assert_eq!(low.max_batch_bytes, MIN_BATCH_BYTES);

        let high = SlsConfig::new("e", "p", "l", StaticCredentialsProvider::new("ak", "sk"))
            .max_batch_bytes(usize::MAX)
            .normalized();
        assert_eq!(high.max_batch_bytes, MAX_BATCH_BYTES_LIMIT);

        // 过短的刷新间隔被抬到下限（闭包自动实现 CredentialsProvider）
        let cfg = SlsConfig::new("e", "p", "l", || {
            Ok(SlsCredentials::new("id", "secret", "token"))
        })
        .credentials_refresh_interval(Duration::from_secs(1))
        .normalized();
        assert_eq!(
            cfg.credentials_refresh_interval,
            Some(MIN_CREDENTIALS_REFRESH_INTERVAL)
        );
    }

    #[test]
    fn refresh_delay_uses_expire_time_when_present() {
        // 提前刷新量 5 分钟，凭据 1 小时后到期：应在到期前 5 分钟刷新（约 55 分钟）
        let cfg = SlsConfig::new("e", "p", "l", StaticCredentialsProvider::new("ak", "sk"))
            .credentials_refresh_ahead(Duration::from_secs(5 * 60))
            .normalized();
        let creds = SlsCredentials::new("id", "secret", "token")
            .expire_time(SystemTime::now() + Duration::from_secs(60 * 60));
        let delay = refresh_delay_for(&cfg, &creds).expect("带有效期应返回刷新间隔");
        // 允许调度/计算耗时带来的少量偏差
        assert!(delay <= Duration::from_secs(55 * 60));
        assert!(delay >= Duration::from_secs(55 * 60 - 5));
    }

    #[test]
    fn refresh_delay_clamps_to_min_when_near_expiry() {
        // 凭据即将/已经到期：进入快速刷新，避免继续使用临期 client
        let cfg = SlsConfig::new("e", "p", "l", StaticCredentialsProvider::new("ak", "sk"))
            .credentials_refresh_ahead(Duration::from_secs(5 * 60))
            .normalized();
        let creds = SlsCredentials::new("id", "secret", "token")
            .expire_time(SystemTime::now() + Duration::from_secs(10));
        assert_eq!(
            refresh_delay_for(&cfg, &creds),
            Some(URGENT_CREDENTIALS_REFRESH_INTERVAL)
        );
    }

    #[test]
    fn batch_flushes_before_exceeding_bytes() {
        let config = SlsConfig::new("e", "p", "l", StaticCredentialsProvider::new("ak", "sk"))
            .max_batch_bytes(2048)
            .normalized();
        let mut batch = Batch::with_capacity(4);
        let first = record_with_fields(&[("key", &"x".repeat(1400))]);
        let second = record_with_fields(&[("key", &"y".repeat(500))]);
        let first_size = record_byte_size(&first);
        let second_size = record_byte_size(&second);

        batch.push_sized(first, first_size);
        assert!(batch.would_exceed_bytes(second_size, &config));
    }

    #[tokio::test]
    async fn push_record_defers_shutdown_without_overgrowing_batch() {
        let config = SlsConfig::new(
            "cn-hangzhou.log.aliyuncs.com",
            "project",
            "logstore",
            StaticCredentialsProvider::new("ak", "sk"),
        )
        .max_batch_bytes(2048)
        .normalized();
        let creds = config
            .credentials_provider
            .provide()
            .expect("静态提供器不应失败");
        let client = build_client_with(&config, &creds).expect("测试 client 应可构建");
        let pipeline = Pipeline {
            client: Arc::new(ArcSwap::new(Arc::new(client))),
            config: Arc::new(config),
            dropped: Arc::new(DropCounters::default()),
            semaphore: Arc::new(Semaphore::new(0)),
            pending_records: Arc::new(AtomicU64::new(0)),
        };
        let mut batch = Batch::with_capacity(4);
        let first = record_with_fields(&[("key", &"x".repeat(1400))]);
        let second = record_with_fields(&[("key", &"y".repeat(500))]);
        let first_size = record_byte_size(&first);
        batch.push_sized(first, first_size);
        let original_bytes = batch.bytes;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        shutdown_tx.send(true).expect("应能发送关闭信号");
        let mut inflight = JoinSet::new();

        let deferred = pipeline
            .push_or_drop(second, &mut batch, &mut inflight, &shutdown_rx, None)
            .await
            .expect("关闭信号应延后新记录而非追加到当前批次");

        assert_eq!(deferred.fields[0].1.as_ref(), "y".repeat(500));
        assert_eq!(batch.len(), 1);
        assert_eq!(batch.bytes, original_bytes);
        assert!(inflight.is_empty());
    }

    #[test]
    fn queue_close_waits_for_in_progress_enqueue() {
        let queue = Arc::new(SinkQueue::new(4));
        queue.active_enqueues.fetch_add(1, Ordering::SeqCst);
        let closer_queue = Arc::clone(&queue);
        let (done_tx, done_rx) = std_mpsc::channel();
        let closer = std::thread::spawn(move || {
            closer_queue.close();
            done_tx.send(()).expect("应能发送关闭完成信号");
        });

        while !queue.is_closed() {
            std::thread::yield_now();
        }
        assert!(done_rx.try_recv().is_err());

        assert!(queue.ring.push(record_with_fields(&[("k", "v")])).is_ok());
        queue.active_enqueues.fetch_sub(1, Ordering::SeqCst);
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("关闭应等待进行中的入队结束后完成");
        closer.join().expect("关闭线程不应 panic");

        assert!(queue.is_closed());
        assert_eq!(queue.ring.len(), 1);
    }

    #[test]
    fn jittered_backoff_stays_within_equal_jitter_range() {
        let base = Duration::from_millis(200);
        for _ in 0..16 {
            let jittered = jittered_backoff_delay(base);
            assert!(jittered >= Duration::from_millis(100));
            assert!(jittered <= base);
        }
    }

    #[test]
    fn refresh_delay_falls_back_to_interval_without_expiry() {
        // 凭据不带有效期但显式设了回退间隔：回退到固定刷新间隔
        let cfg = SlsConfig::new("e", "p", "l", || {
            Ok(SlsCredentials::new("id", "secret", "token"))
        })
        .credentials_refresh_interval(Duration::from_secs(20 * 60))
        .normalized();
        let creds = SlsCredentials::new("id", "secret", "token");
        assert_eq!(
            refresh_delay_for(&cfg, &creds),
            Some(Duration::from_secs(20 * 60))
        );
    }

    #[test]
    fn static_provider_yields_no_refresh_without_expiry() {
        // 长期 AccessKey（无有效期且未设回退间隔）：不再周期刷新
        let cfg =
            SlsConfig::new("e", "p", "l", StaticCredentialsProvider::new("ak", "sk")).normalized();
        let creds = cfg
            .credentials_provider
            .provide()
            .expect("静态提供器不应失败");
        // 静态凭据不带 SecurityToken，据此以长期 AccessKey 方式鉴权
        assert!(creds.security_token.is_empty());
        assert_eq!(refresh_delay_for(&cfg, &creds), None);
    }

    #[test]
    fn static_credentials_provider_returns_fixed_credentials() {
        // StaticCredentialsProvider 每次都返回同一组固定凭据，且不带 SecurityToken
        let provider = StaticCredentialsProvider::new("ak", "sk");
        let creds = provider.provide().expect("静态提供器不应失败");
        assert_eq!(creds.access_key_id, "ak");
        assert_eq!(creds.access_key_secret, "sk");
        assert!(creds.security_token.is_empty());
        assert!(creds.expire_time.is_none());
    }

    #[test]
    fn closure_provider_blanket_impl_is_invoked() {
        // 任意闭包经 blanket impl 自动实现 CredentialsProvider
        let provider = || Ok(SlsCredentials::new("id", "secret", "token"));
        let creds = provider.provide().expect("闭包提供器不应失败");
        assert_eq!(creds.access_key_id, "id");
        assert_eq!(creds.security_token, "token");
    }

    #[test]
    fn is_no_retry_error_handles_empty_list_and_non_sls_error() {
        // 名单为空：任何错误都参与重试
        let err: BoxError = "boom".into();
        assert!(!is_no_retry_error(&err, &[]));
        // 非 SDK 错误（无法取到 HTTP 状态码）：不命中不重试名单
        assert!(!is_no_retry_error(&err, &[400, 404]));
    }

    #[test]
    fn drop_snapshot_tracks_causes() {
        // 验证四类计数各自累加且 total 正确求和
        let counters = DropCounters::default();
        counters.inc_queue_full();
        counters.inc_worker_closed(2);
        counters.inc_send_failed(3);
        counters.inc_shutdown_dropped(4);

        assert_eq!(
            counters.snapshot(),
            DropSnapshot {
                total: 10,
                queue_full: 1,
                worker_closed: 2,
                send_failed: 3,
                shutdown_dropped: 4,
            }
        );
    }

    #[test]
    fn empty_guard_shutdown_is_idempotent_with_drop() {
        // 空 guard 显式关闭后再 drop 不应重复执行或 panic
        let guard = SlsGuard::new(Duration::from_millis(1), Vec::new(), Vec::new());
        guard.shutdown_blocking();
    }

    #[test]
    fn thread_name_for_sink_sanitizes_and_disambiguates() {
        // 普通 logstore 直接拼接
        assert_eq!(
            thread_name_for_sink(0, "EW", "app-logs"),
            "sls-sink-0-EW-app-logs"
        );
        // 非法字符（/）被替换为 _
        assert_eq!(
            thread_name_for_sink(2, "IDT", "prod/errors"),
            "sls-sink-2-IDT-prod_errors"
        );
        // 同一 logstore 的不同 sink 通过 index 区分，线程名不重复
        assert_ne!(
            thread_name_for_sink(0, "EW", "app-logs"),
            thread_name_for_sink(1, "I", "app-logs")
        );
    }

    #[test]
    fn report_dropped_if_any_aggregates_counters() {
        // 验证多 sink 计数能正确聚合
        let a = Arc::new(DropCounters::default());
        let b = Arc::new(DropCounters::default());
        a.inc_queue_full();
        b.inc_send_failed(2);

        report_dropped_if_any(&[Arc::clone(&a), Arc::clone(&b)]);

        assert_eq!(
            aggregate_drop_snapshot(&[a, b]),
            DropSnapshot {
                total: 3,
                queue_full: 1,
                worker_closed: 0,
                send_failed: 2,
                shutdown_dropped: 0,
            }
        );
    }

    #[test]
    fn overflow_policy_defaults_to_drop_newest_and_is_configurable() {
        // 默认策略为丢新，保持与旧版一致的语义
        let cfg = SlsConfig::new("e", "p", "l", StaticCredentialsProvider::new("ak", "sk"));
        assert_eq!(cfg.overflow_policy, OverflowPolicy::DropNewest);
        // setter 可切换到丢旧
        let cfg = cfg.overflow_policy(OverflowPolicy::DropOldest);
        assert_eq!(cfg.overflow_policy, OverflowPolicy::DropOldest);
    }

    #[test]
    fn sink_queue_push_drops_newest_when_full() {
        // 容量 2：填满后 push 失败（丢新），队列内容保持最早入队的两条
        let queue = SinkQueue::new(2);
        assert!(queue.ring.push(record_with_fields(&[("i", "0")])).is_ok());
        assert!(queue.ring.push(record_with_fields(&[("i", "1")])).is_ok());
        assert!(queue.ring.push(record_with_fields(&[("i", "2")])).is_err());
        // 队首仍是最早的 "0"
        let first = queue.ring.pop().expect("应有记录");
        assert_eq!(first.fields[0].1.as_ref(), "0");
    }

    #[test]
    fn sink_queue_force_push_drops_oldest_when_full() {
        // 容量 2：填满后 force_push 挤掉最早的一条并返回它
        let queue = SinkQueue::new(2);
        assert!(
            queue
                .ring
                .force_push(record_with_fields(&[("i", "0")]))
                .is_none()
        );
        assert!(
            queue
                .ring
                .force_push(record_with_fields(&[("i", "1")]))
                .is_none()
        );
        let evicted = queue
            .ring
            .force_push(record_with_fields(&[("i", "2")]))
            .expect("满时应挤出最早一条");
        assert_eq!(evicted.fields[0].1.as_ref(), "0");
        // 剩余为 "1"、"2"
        assert_eq!(queue.ring.pop().unwrap().fields[0].1.as_ref(), "1");
        assert_eq!(queue.ring.pop().unwrap().fields[0].1.as_ref(), "2");
    }

    #[tokio::test]
    async fn sink_queue_recv_returns_item_then_none_after_close() {
        // 有数据时 recv 立即返回；关闭并排空后返回 None
        let queue = SinkQueue::new(4);
        assert!(queue.ring.push(record_with_fields(&[("k", "v")])).is_ok());
        assert!(queue.recv().await.is_some());
        queue.close();
        assert!(queue.is_closed());
        assert!(queue.recv().await.is_none());
    }
}
