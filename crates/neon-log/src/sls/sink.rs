//! 单条 SLS 投递管线（sink）的完整封装
//!
//! 一个 [`SlsSink`] 自带 client、队列与后台 worker 线程，内部实现批量/定时冲刷、
//! 有界并发发送、指数退避重试与优雅关闭。上层（[`crate::sls::tracing`]）负责
//! 把 tracing 事件转成 [`LogRecord`] 并按级别路由到某个 sink

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc as std_mpsc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use aliyun_log_rust_sdk::{Client, Config, FromConfig};
use aliyun_log_sdk_protobuf::{Log, LogGroup};
use tokio::pin;
use tokio::sync::{Semaphore, mpsc, watch};
use tokio::task::JoinSet;

// ===== 各配置项的默认值，集中在此便于统一调整 =====
/// 单批最多日志条数的默认值
const DEFAULT_MAX_BATCH_SIZE: usize = 1024;
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

/// 一条 sink 的完整配置：包含连接凭据、目标 project/logstore 以及批量、并发、重试、
/// 关闭等运行期参数。通过链式 setter 覆盖默认值
#[derive(Clone)]
pub struct SlsConfig {
    /// SLS 服务接入点，例如 `cn-hangzhou.log.aliyuncs.com`
    endpoint: String,
    /// 阿里云 AccessKey ID
    access_key_id: String,
    /// 阿里云 AccessKey Secret
    access_key_secret: String,

    /// SLS 工程名（Project）
    project: String,
    /// SLS 日志库名（Logstore）
    logstore: String,
    /// 日志 topic，写入 `LogGroup.topic`；`None` 表示不设置
    topic: Option<String>,
    /// 日志来源标识，写入 `LogGroup.source`（通常为主机名/IP）；`None` 表示不设置
    source: Option<String>,
    /// STS 临时凭据的安全令牌；`None` 时使用长期 AccessKey 鉴权
    security_token: Option<String>,

    /// 单批最多日志条数，达到该阈值即触发一次冲刷，默认 1024
    max_batch_size: usize,
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
    /// 单次 PutLogs 请求的超时时间，默认 30s
    request_timeout: Duration,
    /// 优雅关闭的总时限：guard drop 时在该时限内排空队列并冲刷残留日志，默认 15s
    shutdown_timeout: Duration,
}

impl SlsConfig {
    /// 用必填项创建配置，其余项取默认值（可链式调用 setter 覆盖）
    ///
    /// - `endpoint`：SLS 服务接入点，例如 `cn-hangzhou.log.aliyuncs.com`
    /// - `access_key_id` / `access_key_secret`：阿里云访问凭据
    /// - `project`：SLS 工程名
    /// - `logstore`：SLS 日志库名
    pub fn new(
        endpoint: impl Into<String>,
        access_key_id: impl Into<String>,
        access_key_secret: impl Into<String>,
        project: impl Into<String>,
        logstore: impl Into<String>,
    ) -> Self {
        Self {
            // 必填连接与目标项：调用方提供的任意 Into<String> 统一转 owned String
            endpoint: endpoint.into(),
            access_key_id: access_key_id.into(),
            access_key_secret: access_key_secret.into(),

            project: project.into(),
            logstore: logstore.into(),
            // 可选项默认不设置，后续可用链式 setter 补充
            topic: None,
            source: None,
            security_token: None,

            // 运行期参数全部取模块级默认常量
            max_batch_size: DEFAULT_MAX_BATCH_SIZE,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
            max_inflight: DEFAULT_MAX_INFLIGHT,
            max_retries: DEFAULT_MAX_RETRIES,
            retry_base: DEFAULT_RETRY_BASE,
            retry_max: DEFAULT_RETRY_MAX,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
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

    /// 设置 STS 临时凭据的安全令牌（SecurityToken）
    /// 设置后将以 STS 方式鉴权，否则使用长期 AccessKey
    pub fn security_token(mut self, token: impl Into<String>) -> Self {
        self.security_token = Some(token.into());
        self
    }

    /// 设置单批最多日志条数；达到该阈值即触发一次冲刷，最小为 1，默认 1024
    ///
    /// 越界值在构建时由 [`SlsConfig::normalized`] 统一钳制
    pub fn max_batch_size(mut self, n: usize) -> Self {
        self.max_batch_size = n;
        self
    }

    /// 设置定时冲刷间隔；即使未攒满一批，也会在该间隔后冲刷，最小 1ms，默认 2s
    ///
    /// 越界值在构建时由 [`SlsConfig::normalized`] 统一钳制
    pub fn flush_interval(mut self, d: Duration) -> Self {
        self.flush_interval = d;
        self
    }

    /// 设置普通日志队列容量；队列满时新日志按 `queue_full` 丢弃并计数，最小为 1，默认 65536
    ///
    /// 越界值在构建时由 [`SlsConfig::normalized`] 统一钳制
    pub fn queue_capacity(mut self, n: usize) -> Self {
        self.queue_capacity = n;
        self
    }

    /// 返回当前配置的队列容量（构建前未经 [`normalized`] 钳制）
    pub fn configured_queue_capacity(&self) -> usize {
        self.queue_capacity
    }

    /// 设置并发投递 PutLogs 请求的上限。提高可在网络抖动/慢响应时维持吞吐，
    /// 同时仍对发送速率形成有界背压（达到上限后才会反压采集），最小为 1，默认 4
    ///
    /// 越界值在构建时由 [`SlsConfig::normalized`] 统一钳制
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
    /// 越界值在构建时由 [`SlsConfig::normalized`] 统一钳制
    pub fn retry_base(mut self, d: Duration) -> Self {
        self.retry_base = d;
        self
    }

    /// 设置重试退避的上限间隔，最小 1ms，默认 10s
    ///
    /// 构建时由 [`SlsConfig::normalized`] 统一钳制，并规整为不小于 `retry_base`
    pub fn retry_max(mut self, d: Duration) -> Self {
        self.retry_max = d;
        self
    }

    /// 设置单次 PutLogs 请求的超时时间，最小 1ms，默认 30s
    ///
    /// 越界值在构建时由 [`SlsConfig::normalized`] 统一钳制
    pub fn request_timeout(mut self, d: Duration) -> Self {
        self.request_timeout = d;
        self
    }

    /// 设置优雅关闭的总时限：guard drop 时在该时限内排空队列并冲刷残留日志，最小 1ms，默认 15s
    ///
    /// 越界值在构建时由 [`SlsConfig::normalized`] 统一钳制
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
        // 空字符串的 topic/source 视为未设置，避免写出空字段
        self.topic = self.topic.filter(|s| !s.is_empty());
        self.source = self.source.filter(|s| !s.is_empty());
        self
    }
}

impl fmt::Debug for SlsConfig {
    /// 手写 Debug 以控制字段输出顺序与格式（凭据按调用方要求明文输出）
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SlsConfig")
            .field("endpoint", &self.endpoint)
            .field("access_key_id", &self.access_key_id)
            .field("access_key_secret", &self.access_key_secret)
            .field("security_token", &self.security_token)
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

/// 一条完整、独立的 SLS 投递管线：自带 client、队列与后台 worker 线程，
/// 内部实现批量/定时冲刷、有界并发发送、指数退避重试与优雅关闭
///
/// 通过 [`crate::sls::tracing::SlsLayerBuilder`] 组合多个 `SlsSink`，
/// 即可按级别把日志路由到不同 sink
pub struct SlsSink {
    /// 向后台 worker 投递日志的非阻塞发送端
    tx: mpsc::Sender<LogRecord>,
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
        // 客户端创建失败直接上抛，由调用方处理
        let client = build_client(&config).map_err(|source| SlsBuildError::Client { source })?;
        // 有界队列：容量即背压点，满时入队侧丢弃
        let (tx, rx) = mpsc::channel::<LogRecord>(config.queue_capacity);
        // 关闭信号通道：广播 true 触发优雅关闭
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        // 完成通知通道：worker 排空结束后发一个 ()，guard 据此等待
        let (done_tx, done_rx) = std_mpsc::channel::<()>();
        let dropped = Arc::new(DropCounters::default());

        // worker 线程需要独立持有 config 与计数器的克隆/引用
        let worker_cfg = config.clone();
        let worker_dropped = Arc::clone(&dropped);
        let thread_name = thread_name_for_sink(index, level_tag, &config.logstore);
        // 为投递管线起一条命名 OS 线程；线程内部再建 current-thread runtime
        let handle = std::thread::Builder::new()
            .name(thread_name)
            .spawn(move || run_sink(worker_cfg, client, rx, shutdown_rx, done_tx, worker_dropped))
            .map_err(SlsBuildError::WorkerThread)?;

        // 入队句柄交给上层 Layer
        let sink = SlsSink { tx, dropped };
        // 关闭句柄交给 guard
        let sink_handle = SinkHandle {
            shutdown: shutdown_tx,
            done: Some(done_rx),
            handle: Some(handle),
        };
        Ok((sink, sink_handle))
    }

    /// 非阻塞入队一条日志；队列满或 worker 关闭时按原因分类计为丢弃
    pub(crate) fn enqueue(&self, record: LogRecord) {
        // try_send 不阻塞业务线程；失败按错误类型归类丢弃
        if let Err(e) = self.tx.try_send(record) {
            match e {
                mpsc::error::TrySendError::Full(_) => self.dropped.inc_queue_full(),
                mpsc::error::TrySendError::Closed(_) => self.dropped.inc_worker_closed(1),
            }
        }
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

/// 单条 sink 的后台 worker 线程入口：在专属的 current-thread runtime 上运行投递管线
fn run_sink(
    config: SlsConfig,
    client: Client,
    mut rx: mpsc::Receiver<LogRecord>,
    shutdown_rx: watch::Receiver<bool>,
    done_tx: std_mpsc::Sender<()>,
    dropped: Arc<DropCounters>,
) {
    // 每条 sink 独占一个单线程 runtime，彼此资源隔离、互不争用
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            // runtime 都建不起来：放弃投递，但仍要把队列里的残留排空计数后通知完成
            eprintln!("[sls-tracing] 无法创建运行时，停止该 sink 的日志投递: {e}");
            rx.close();
            let mut drained = 0;
            // 排空队列中已入队的记录并计为 worker_closed 丢弃
            while rx.try_recv().is_ok() {
                drained += 1;
            }
            dropped.inc_worker_closed(drained);
            // 即便失败也要发完成通知，避免 guard 一直等到超时
            let _ = done_tx.send(());
            return;
        }
    };

    // 在该 runtime 上阻塞运行投递管线，结束后发出完成通知
    runtime.block_on(async move {
        // config/client 在多个并发发送任务间共享，用 Arc 包裹
        let config = Arc::new(config);
        let client = Arc::new(client);

        run_pipeline(client, rx, config, Arc::clone(&dropped), shutdown_rx).await;

        // 管线结束（已排空或超时）后通知 guard
        let _ = done_tx.send(());
    });
}

/// 单条投递管线运行所需的共享只读句柄：把它们收进一处，避免在
/// `run_pipeline` / `drain_into_buf` / `spawn_flush` 之间逐个透传
struct Pipeline {
    /// SLS 客户端
    client: Arc<Client>,
    /// 本 sink 的运行期配置
    config: Arc<SlsConfig>,
    /// 本 sink 的丢弃计数
    dropped: Arc<DropCounters>,
    /// 限制并发 in-flight 请求数的信号量
    semaphore: Arc<Semaphore>,
}

impl Pipeline {
    /// 取走 `buf` 中的日志，组装为 `LogGroup` 并派发一个并发发送任务
    ///
    /// 通过信号量限制 in-flight 请求数：达到上限时 `acquire_owned` 会在此 await，
    /// 从而对采集侧形成有界背压；获取到许可后任务被 spawn，主循环可立即继续收集
    async fn spawn_flush(
        &self,
        buf: &mut Vec<LogRecord>,
        inflight: &mut JoinSet<()>,
        shutdown_rx: &watch::Receiver<bool>,
        deadline: Option<Instant>,
    ) {
        // 空批次无需发送
        if buf.is_empty() {
            return;
        }
        // 取走整批记录，buf 复位以继续累积下一批
        let records = std::mem::take(buf);
        let count = records.len() as u64;

        // 达到 max_inflight 时此处 await 会暂停外层 select! 主循环：期间不再收取队列、
        // 不回收已完成的发送任务，从而对采集侧形成有界背压（许可由完成的发送任务释放，不会死锁）
        let permit = match Arc::clone(&self.semaphore).acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => {
                // 信号量被关闭（理论上不会发生），保守计为发送失败
                self.dropped.inc_send_failed(count);
                return;
            }
        };

        // 为发送任务克隆所需共享句柄（Arc/watch 克隆都很廉价）
        let client = Arc::clone(&self.client);
        let config = Arc::clone(&self.config);
        let dropped = Arc::clone(&self.dropped);
        let mut shutdown_rx = shutdown_rx.clone();
        inflight.spawn(async move {
            // 持有 permit 到任务结束，drop 时自动归还并发额度
            let _permit = permit;
            // 持有 records，仅在确实需要重试时才从中重建 LogGroup；happy-path 不做整批 clone
            send_with_retry(
                &client,
                &config,
                records,
                count,
                &dropped,
                deadline,
                &mut shutdown_rx,
            )
            .await;
        });
    }

    /// 在 deadline 之前持续从队列中取出日志放入 `buf`，达到批量阈值即派发发送任务
    async fn drain_into_buf(
        &self,
        rx: &mut mpsc::Receiver<LogRecord>,
        buf: &mut Vec<LogRecord>,
        inflight: &mut JoinSet<()>,
        shutdown_rx: &watch::Receiver<bool>,
        deadline: Instant,
    ) {
        loop {
            // 超过截止时间立即停止排空，剩余交由上层计为丢弃
            if Instant::now() >= deadline {
                return;
            }
            // 非阻塞取：队列空或关闭即返回
            match rx.try_recv() {
                Ok(record) => {
                    buf.push(record);
                    // 攒满一批就派发，避免单批过大或内存堆积
                    if buf.len() >= self.config.max_batch_size {
                        self.spawn_flush(buf, inflight, shutdown_rx, Some(deadline))
                            .await;
                    }
                }
                // 队列已空或已关闭：排空结束
                Err(_) => return,
            }
        }
    }
}

/// 单条投递管线：消费一个队列，批量/定时冲刷，并发发送，优雅关闭时在 deadline 内排空
async fn run_pipeline(
    client: Arc<Client>,
    mut rx: mpsc::Receiver<LogRecord>,
    config: Arc<SlsConfig>,
    dropped: Arc<DropCounters>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    // 每条管线持有自己的并发额度，与共享只读句柄一并收进 Pipeline
    let semaphore = Arc::new(Semaphore::new(config.max_inflight));
    let pipeline = Pipeline {
        client,
        config,
        dropped,
        semaphore,
    };

    // 已派发但未完成的发送任务集合，用于回收与退出等待
    let mut inflight: JoinSet<()> = JoinSet::new();
    // 当前累积批次的缓冲，预留一批容量减少扩容
    let mut buf = Vec::with_capacity(pipeline.config.max_batch_size);
    let mut shutting_down = false;
    // 下一次定时冲刷的截止时刻
    let mut flush_deadline = tokio::time::Instant::now() + pipeline.config.flush_interval;

    // ===== 正常运行：四路 select 同时驱动收取、定时冲刷、任务回收与关闭 =====
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
            maybe = rx.recv() => {
                match maybe {
                    Some(record) => {
                        buf.push(record);
                        // 攒满一批立即冲刷，并重置定时器
                        if buf.len() >= pipeline.config.max_batch_size {
                            pipeline.spawn_flush(&mut buf, &mut inflight, &shutdown_rx, None).await;
                            flush_deadline =
                                tokio::time::Instant::now() + pipeline.config.flush_interval;
                        }
                    }
                    // 队列关闭（所有发送端已 drop）：进入收尾
                    None => shutting_down = true,
                }
            }

            // 到达定时冲刷时刻：即使未满也冲刷一次，并重置定时器
            _ = tokio::time::sleep_until(flush_deadline) => {
                pipeline.spawn_flush(&mut buf, &mut inflight, &shutdown_rx, None).await;
                flush_deadline = tokio::time::Instant::now() + pipeline.config.flush_interval;
            }

            // 回收已完成的发送任务，避免 JoinSet 无限堆积
            Some(_) = inflight.join_next(), if !inflight.is_empty() => {}
        }
    }

    // ===== 优雅关闭：在 deadline 内排空本管线队列并冲刷残留 =====
    let deadline = Instant::now() + pipeline.config.shutdown_timeout;
    // 关闭入队侧，确保不再有新日志进入，且能用 try_recv 排空已有
    rx.close();
    // 在截止时间内把队列里剩余日志取入 buf，达到批量阈值即派发
    pipeline
        .drain_into_buf(&mut rx, &mut buf, &mut inflight, &shutdown_rx, deadline)
        .await;

    // 处理排空后 buf 中不足一批的尾部数据
    if !buf.is_empty() {
        if Instant::now() < deadline {
            // 仍在时限内：派发最后一批，并带上 deadline 让发送任务自我约束
            pipeline
                .spawn_flush(&mut buf, &mut inflight, &shutdown_rx, Some(deadline))
                .await;
        } else {
            // 时限已过：直接计为退出丢弃
            pipeline.dropped.inc_shutdown_dropped(buf.len() as u64);
            buf.clear();
        }
    }

    // 仍滞留在队列中的（drain 超时未取出的）也计为退出丢弃
    let queued = rx.len();
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

/// 依据配置构建 SLS 客户端：按是否提供 STS token 选择鉴权方式
fn build_client(config: &SlsConfig) -> Result<Client, BoxError> {
    // 基础项：接入点与单次请求超时
    let mut builder = Config::builder()
        .endpoint(config.endpoint.clone())
        .request_timeout(config.request_timeout);

    // 有 security_token 用 STS 临时凭据，否则用长期 AccessKey
    builder = match &config.security_token {
        Some(token) => builder.sts(
            config.access_key_id.clone(),
            config.access_key_secret.clone(),
            token.clone(),
        ),
        None => builder.access_key(
            config.access_key_id.clone(),
            config.access_key_secret.clone(),
        ),
    };

    // 构建配置并据此创建客户端，任一步失败均上抛
    let cfg = builder.build()?;
    let client = Client::from_config(cfg)?;
    Ok(client)
}

/// 发送一批日志，失败按指数退避重试；关闭/超时场景下受 deadline 约束并正确计数
async fn send_with_retry(
    client: &Client,
    config: &SlsConfig,
    records: Vec<LogRecord>,
    count: u64,
    dropped: &DropCounters,
    mut deadline: Option<Instant>,
    shutdown_rx: &mut watch::Receiver<bool>,
) {
    // 已发送尝试次数（不含首发，首发为 attempt 0）
    let mut attempt = 0u32;

    loop {
        // 进入每次尝试前先检查 deadline，超时直接按退出丢弃计数
        if deadline_expired(deadline) {
            drop_shutdown_batch(count, dropped, "退出冲刷时间耗尽");
            return;
        }

        // 每次 attempt 从 records 重建 LogGroup（put_logs 会消费 group）
        // happy-path 仅构建一次，相比此前“构建 + 整批 clone”省掉一次全量拷贝
        let batch = build_log_group(config, &records);

        // 组装并发起一次 PutLogs 请求
        let send_fut = client
            .put_logs(config.project.as_str(), config.logstore.as_str())
            .log_group(batch)
            .send();

        // 等待请求结果；期间若收到关闭信号会切换到带 deadline 的冲刷模式
        let result = await_put_logs(send_fut, &mut deadline, config.shutdown_timeout, shutdown_rx)
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
                if !sleep_backoff(config, attempt, &mut deadline, shutdown_rx).await {
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
    let backoff = backoff_delay(config, attempt);
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
        let config = SlsConfig::new("endpoint", "ak", "sk", "project", "logstore");
        assert_eq!(backoff_delay(&config, 1), Duration::from_millis(200));
        assert_eq!(backoff_delay(&config, 2), Duration::from_millis(400));
        assert_eq!(backoff_delay(&config, 20), Duration::from_secs(10));
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
}
