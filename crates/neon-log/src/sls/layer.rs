//! 基于阿里云 SLS 官方 Rust SDK 的 tracing 日志投递层
//!
//! 本模块负责 tracing 集成：把事件与 span 上下文提取成结构化字段，并按日志级别
//! 路由到一条独立的投递管线（[`SlsSink`]，定义见 [`crate::sls::sink`]）

use std::cell::RefCell;
use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;

use super::sink::{DropSnapshot, LogRecord, SlsBuildError, SlsConfig, SlsGuard, SlsSink};

/// 默认 [`build`] 中 ERROR/WARN sink 的队列容量
pub const DEFAULT_EW_QUEUE_CAPACITY: usize = 8_192;

/// 便捷构建：兼容旧版双管线语义——`ERROR`/`WARN` 一条 sink；
/// `INFO`/`DEBUG`/`TRACE` 一条 sink，各自独立 client
///
/// client 创建失败时返回 [`SlsBuildError`]。
/// 需要自定义路由（例如每个级别一条独立 sink、或不同级别写入不同 logstore）时，
/// 请改用 [`SlsLayerBuilder`]
pub fn build(config: SlsConfig) -> Result<(SlsLayer, SlsGuard), SlsBuildError> {
    // ERROR/WARN sink 队列不超过 DEFAULT_EW_QUEUE_CAPACITY，且不超过传入 config 的 queue_capacity
    let ew_capacity = config
        .configured_queue_capacity()
        .min(DEFAULT_EW_QUEUE_CAPACITY);
    let ew_config = config.clone().queue_capacity(ew_capacity);

    SlsLayerBuilder::new()
        .add_sink([Level::ERROR, Level::WARN], ew_config)
        .add_sink([Level::INFO, Level::DEBUG, Level::TRACE], config)
        .build()
}

/// 把日志按级别路由到若干条独立投递管线（[`SlsSink`]）的构建器
///
/// 每次 `add_sink` 注册一条独立 sink（独立 client / 队列 / 后台 worker 线程）并声明它负责的级别
/// 同一级别若被多条 sink 声明，以**先注册者**为准；未被任何 sink 覆盖的级别会被忽略（不投递）
///
/// # 示例
/// ```ignore
/// use tracing::Level;
///
/// // 方案一：error/warn 一条，info/debug/trace 一条
/// let (layer, guard) = SlsLayerBuilder::new()
///     .add_sink([Level::ERROR, Level::WARN], err_config)
///     .add_sink([Level::INFO, Level::DEBUG, Level::TRACE], info_config)
///     .build()?;
///
/// // 方案二：每个级别一条独立 sink（可分别指向不同 logstore）
/// let (layer, guard) = SlsLayerBuilder::new()
///     .add_sink([Level::ERROR], err_cfg)
///     .add_sink([Level::WARN], warn_cfg)
///     .add_sink([Level::INFO], info_cfg)
///     .build()?;
/// ```
#[derive(Default)]
pub struct SlsLayerBuilder {
    /// 已注册的 sink 规格列表：每项为 (负责级别集合, 该 sink 的独立配置)
    specs: Vec<(Vec<Level>, SlsConfig)>,
}

impl SlsLayerBuilder {
    /// 创建一个空构建器，后续用 `add_sink` 注册 sink
    pub fn new() -> Self {
        Self { specs: Vec::new() }
    }

    /// 注册一条 sink：`levels` 是它负责的日志级别，`config` 是它独立的配置
    /// （含 endpoint/凭据/project/logstore、队列容量、批量与并发参数等）
    pub fn add_sink(mut self, levels: impl IntoIterator<Item = Level>, config: SlsConfig) -> Self {
        // 把级别迭代器收集为 Vec 后与配置一并入栈
        self.specs.push((levels.into_iter().collect(), config));
        self
    }

    /// 构建并启动全部 sink。任一 sink 的 client 创建失败即返回错误，此前已启动的 sink 会被正确关闭
    pub fn build(self) -> Result<(SlsLayer, SlsGuard), SlsBuildError> {
        // 至少要有一条 sink
        if self.specs.is_empty() {
            return Err(SlsBuildError::NoSinks);
        }

        // 预分配各容器：sink 入队句柄、后台句柄、级别→sink 下标的路由表
        let mut sinks = Vec::with_capacity(self.specs.len());
        let mut handles = Vec::with_capacity(self.specs.len());
        let mut routing: [Option<usize>; LEVEL_COUNT] = [None; LEVEL_COUNT];
        // guard 的关闭时限取所有 sink 中的最大值，保证慢 sink 也有足够时间
        let mut shutdown_timeout = Duration::from_millis(1);

        for (levels, config) in self.specs {
            // 累计取最大关闭时限
            shutdown_timeout = shutdown_timeout.max(config.shutdown_timeout_duration());
            let level_tag = level_tag(&levels);
            // 当前 sink 的下标即现有 sink 数量
            let idx = sinks.len();
            match SlsSink::spawn(idx, &level_tag, config) {
                // 启动成功：保存入队句柄与后台句柄
                Ok((sink, handle)) => {
                    sinks.push(sink);
                    handles.push(handle);
                }
                Err(e) => {
                    // 回收已启动的 sink 后直接上抛错误
                    let drop_counters: Vec<_> = sinks.iter().map(SlsSink::drop_counters).collect();
                    drop(SlsGuard::new(shutdown_timeout, handles, drop_counters));
                    return Err(e);
                }
            }
            assign_routes(&mut routing, &levels, idx);
        }

        // 汇总所有 sink 的计数器供 guard 退出时打印
        let drop_counters: Vec<_> = sinks.iter().map(SlsSink::drop_counters).collect();

        Ok((
            SlsLayer { sinks, routing },
            SlsGuard::new(shutdown_timeout, handles, drop_counters),
        ))
    }
}

/// 把一条 sink 负责的级别登记进路由表，遵循“先注册者优先”
fn assign_routes(routing: &mut [Option<usize>; LEVEL_COUNT], levels: &[Level], idx: usize) {
    for level in levels {
        let li = level_index(level);
        // 仅当该级别尚未被占用时才登记，从而保证先注册者优先
        if routing[li].is_none() {
            routing[li] = Some(idx);
        }
    }
}

/// tracing 层：按级别把事件路由到对应的 [`SlsSink`]
pub struct SlsLayer {
    /// 各 sink 的入队句柄，下标与构建顺序一致
    sinks: Vec<SlsSink>,
    /// 级别→sink 下标的路由表；`None` 表示该级别不投递
    routing: [Option<usize>; LEVEL_COUNT],
}

impl SlsLayer {
    /// 所有 sink 累计的丢弃总数
    pub fn dropped_count(&self) -> u64 {
        self.dropped_snapshot().total
    }

    /// 所有 sink 聚合后的丢弃统计快照
    pub fn dropped_snapshot(&self) -> DropSnapshot {
        let mut agg = DropSnapshot::default();
        // 逐 sink 累加四类计数
        for sink in &self.sinks {
            agg.merge(&sink.dropped_snapshot());
        }
        agg
    }
}

impl<S> Layer<S> for SlsLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    /// span 创建时：提取其初始字段并存入 span 扩展，供其下的事件复用
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id) {
            // 以 span 名构造字段容器（含 key 前缀），再记录初始字段
            let mut sf = SpanFields::new(span.name());
            attrs.record(&mut SpanFieldVisitor(&mut sf));
            span.extensions_mut().insert(sf);
        }
    }

    /// span 字段后续被 `record` 更新时：upsert 到已存在的字段容器
    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id) {
            let mut ext = span.extensions_mut();
            if let Some(sf) = ext.get_mut::<SpanFields>() {
                // 容器已存在：直接更新
                values.record(&mut SpanFieldVisitor(sf));
            } else {
                // 容器缺失（理论少见）：补建后记录
                let mut sf = SpanFields::new(span.name());
                values.record(&mut SpanFieldVisitor(&mut sf));
                ext.insert(sf);
            }
        }
    }

    /// span 关闭时：移除其字段容器，释放扩展内存
    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(&id) {
            span.extensions_mut().remove::<SpanFields>();
        }
    }

    /// 事件发生时：过滤内部噪声、按级别选 sink、组装字段并入队
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let meta = event.metadata();
        // 屏蔽网络栈/SDK 自身日志，避免“发日志触发日志”的回环
        if is_internal_target(meta.target()) {
            return;
        }

        // 先按级别确定目标 sink；无对应 sink 则直接跳过，省去字段拼装开销
        let sink_idx = match self.routing[level_index(meta.level())] {
            Some(idx) => idx,
            None => return,
        };

        // 取一次时间戳，事件的所有字段共用
        let (time, time_ns) = now_unix();

        // thread_local 缓冲在 Layer 重入（同线程嵌套 logging）时 try_borrow 会失败，降级为栈上临时缓冲
        let mut fallback_scratch = None;
        EVENT_BUFFERS.with(|buffers| match buffers.try_borrow_mut() {
            // 正常路径：复用线程本地缓冲
            Ok(mut scratch) => {
                dispatch_event(self, sink_idx, event, &ctx, time, time_ns, &mut scratch);
            }
            // 重入路径：标记需要用栈上临时缓冲（不能在 borrow 闭包里再次 dispatch）
            Err(_) => {
                fallback_scratch = Some(EventScratch::new());
            }
        });
        // 重入降级：在闭包外用新建的栈上缓冲完成派发
        if let Some(mut scratch) = fallback_scratch {
            dispatch_event(self, sink_idx, event, &ctx, time, time_ns, &mut scratch);
        }
    }
}

/// 把 sink 负责的级别压成紧凑标签，用于线程名：
/// ERROR→E、WARN→W、INFO→I、DEBUG→D、TRACE→T（如 ERROR/WARN → `EW`）
/// 未声明任何级别时返回 `_`
fn level_tag(levels: &[Level]) -> String {
    let mut tag = String::with_capacity(levels.len().max(1));
    // 逐级别取首字母拼接
    for level in levels {
        tag.push(match *level {
            Level::ERROR => 'E',
            Level::WARN => 'W',
            Level::INFO => 'I',
            Level::DEBUG => 'D',
            Level::TRACE => 'T',
        });
    }
    // 空级别集合用 _ 占位，保证线程名格式完整
    if tag.is_empty() {
        tag.push('_');
    }
    tag
}

/// 把一个事件转换为 [`LogRecord`] 并入队到指定 sink
///
/// 字段拼装顺序：保留键（level/target/message/file/line）→ span 路径与 span 字段 → 事件自身字段
/// 全程复用传入的 `scratch` 缓冲以减少分配
fn dispatch_event<S>(
    layer: &SlsLayer,
    sink_idx: usize,
    event: &Event<'_>,
    ctx: &Context<'_, S>,
    time: u32,
    time_ns: u32,
    scratch: &mut EventScratch,
) where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    // 复位缓冲，清掉上一条事件的残留
    scratch.reset();
    let meta = event.metadata();

    // 解构出各子缓冲，便于分别借用
    let EventScratch {
        fields,
        seen,
        visitor,
        span_path,
    } = scratch;

    // 先访问事件自身字段，结果暂存在 visitor 中
    event.record(visitor);

    // 写入级别（用静态缓存的 Arc）
    push_unique_field_prepared(
        fields,
        seen,
        Arc::clone(&RESERVED_KEYS.level),
        level_value(meta.level()),
    );
    // 写入 target
    push_unique_field_prepared(
        fields,
        seen,
        Arc::clone(&RESERVED_KEYS.target),
        shared_str(meta.target()),
    );
    // 写入 message（若有）
    if let Some(msg) = visitor.message.take() {
        push_unique_field_prepared(fields, seen, Arc::clone(&RESERVED_KEYS.message), msg);
    }
    // 写入源文件
    if let Some(file) = meta.file() {
        push_unique_field_prepared(
            fields,
            seen,
            Arc::clone(&RESERVED_KEYS.file),
            shared_str(file),
        );
    }
    // 写入行号
    if let Some(line) = meta.line() {
        push_unique_field_prepared(
            fields,
            seen,
            Arc::clone(&RESERVED_KEYS.line),
            shared_string(line.to_string()),
        );
    }

    // 遍历从根到当前的 span 作用域，拼出 span 路径并并入各 span 的字段
    if let Some(scope) = ctx.event_scope(event) {
        for span in scope.from_root() {
            // 用 " > " 连接形成可读的 span 调用链
            if !span_path.is_empty() {
                span_path.push_str(" > ");
            }
            span_path.push_str(span.name());
            // 并入该 span 预拼接好的字段（仅 clone Arc，无分配）
            if let Some(sf) = span.extensions().get::<SpanFields>() {
                for (key, value) in &sf.entries {
                    push_unique_field_prepared(fields, seen, Arc::clone(key), Arc::clone(value));
                }
            }
        }
    }
    // 把拼好的 span 路径作为一个字段写入（take 以避免再次分配）
    if !span_path.is_empty() {
        push_unique_field_prepared(
            fields,
            seen,
            Arc::clone(&RESERVED_KEYS.span),
            shared_string(std::mem::take(span_path)),
        );
    }

    // 最后写入事件自身的非 message 字段（需 sanitize 与去重）
    for (key, value) in visitor.fields.drain(..) {
        push_unique_field(fields, seen, key, value);
    }

    // 取走字段构造记录并入队（take 让 scratch 的 fields 复位为空，供下次复用）
    let record = LogRecord {
        time,
        time_ns,
        fields: std::mem::take(fields),
    };
    layer.sinks[sink_idx].enqueue(record);
}

/// 判断 target 是否属于应屏蔽的内部库，避免投递自身/网络栈日志造成回环或噪声
fn is_internal_target(target: &str) -> bool {
    // 需要屏蔽的 crate 前缀清单
    const BLOCKED_PREFIXES: &[&str] = &[
        "aliyun_log",
        "hyper",
        "h2",
        "reqwest",
        "rustls",
        "tower",
        "tokio_util",
        "want",
        "mio",
    ];
    // 命中条件：完全相等，或以 `prefix::` 开头（避免误伤 reqwest_extra 这类同前缀 crate）
    BLOCKED_PREFIXES.iter().any(|p| {
        target == *p
            || target
                .strip_prefix(p)
                .is_some_and(|rest| rest.starts_with("::"))
    })
}

thread_local! {
    /// 每线程一份的事件拼装缓冲，避免高 QPS 下反复分配
    static EVENT_BUFFERS: RefCell<EventScratch> = RefCell::new(EventScratch::new());
}

/// 预先缓存好的保留字段 key，避免每条事件重复构造 `Arc<str>`
struct ReservedKeys {
    /// 级别字段 key
    level: Arc<str>,
    /// target 字段 key
    target: Arc<str>,
    /// 消息字段 key
    message: Arc<str>,
    /// 源文件字段 key
    file: Arc<str>,
    /// 行号字段 key
    line: Arc<str>,
    /// span 路径字段 key
    span: Arc<str>,
}

/// 进程级单例的保留 key 缓存，首次使用时初始化
static RESERVED_KEYS: LazyLock<ReservedKeys> = LazyLock::new(|| ReservedKeys {
    level: Arc::from("__level__"),
    target: Arc::from("__target__"),
    message: Arc::from("message"),
    file: Arc::from("__file__"),
    line: Arc::from("__line__"),
    span: Arc::from("__span__"),
});

/// 进程级单例的级别字符串缓存，下标与 [`level_index`] 一致
static LEVEL_VALUES: LazyLock<[Arc<str>; LEVEL_COUNT]> = LazyLock::new(|| {
    [
        Arc::from("ERROR"),
        Arc::from("WARN"),
        Arc::from("INFO"),
        Arc::from("DEBUG"),
        Arc::from("TRACE"),
    ]
});

/// 取某级别对应的静态级别字符串（仅 clone Arc，无分配）
fn level_value(level: &Level) -> Arc<str> {
    Arc::clone(&LEVEL_VALUES[level_index(level)])
}

/// 每条 event 复用的临时缓冲，避免高 QPS 下反复分配 Vec / HashSet
struct EventScratch {
    /// 待入队的字段列表
    fields: Vec<(Arc<str>, Arc<str>)>,
    /// 已出现的 key 集合，用于去重与加后缀
    seen: HashSet<Arc<str>>,
    /// 事件字段访问器
    visitor: EventVisitor,
    /// 拼接中的 span 路径
    span_path: String,
}

impl EventScratch {
    /// 新建缓冲并预留常见规模容量，减少扩容
    fn new() -> Self {
        Self {
            fields: Vec::with_capacity(16),
            seen: HashSet::with_capacity(16),
            visitor: EventVisitor::new(),
            span_path: String::with_capacity(64),
        }
    }

    /// 复位各子缓冲以复用（清空但保留已分配容量）
    fn reset(&mut self) {
        self.fields.clear();
        self.seen.clear();
        self.visitor.clear();
        self.span_path.clear();
    }
}

/// 从 `&str` 构造共享的 `Arc<str>`
#[inline]
fn shared_str(value: &str) -> Arc<str> {
    Arc::from(value)
}

/// 从 owned `String` 构造共享的 `Arc<str>`（经 boxed_str 避免多余拷贝）
#[inline]
fn shared_string(value: String) -> Arc<str> {
    Arc::from(value.into_boxed_str())
}

/// 写入一个需 sanitize 的事件字段；key 冲突时追加 `_2`、`_3`…… 后缀保证唯一
fn push_unique_field(
    fields: &mut Vec<(Arc<str>, Arc<str>)>,
    seen: &mut HashSet<Arc<str>>,
    key: impl AsRef<str>,
    value: Arc<str>,
) {
    // 先 sanitize 再转 Arc<str>
    let key = shared_string(sanitize_field_key(key.as_ref()));
    // insert 返回 true 说明 key 尚未出现，可直接使用
    if seen.insert(Arc::clone(&key)) {
        fields.push((key, value));
        return;
    }

    // key 已存在：从 _2 开始尝试带后缀的候选直到不冲突
    let mut suffix = 2;
    loop {
        let candidate = shared_string(format!("{key}_{suffix}"));
        if seen.insert(Arc::clone(&candidate)) {
            fields.push((candidate, value));
            return;
        }
        suffix += 1;
    }
}

/// 键已在 span 创建/更新时预拼接并 sanitize，event 热路径直接复用
fn push_unique_field_prepared(
    fields: &mut Vec<(Arc<str>, Arc<str>)>,
    seen: &mut HashSet<Arc<str>>,
    key: Arc<str>,
    value: Arc<str>,
) {
    // 未出现过则直接使用（无需再次 sanitize）
    if seen.insert(Arc::clone(&key)) {
        fields.push((key, value));
        return;
    }

    // 冲突时同样追加 _2、_3…… 后缀
    let base = key.as_ref();
    let mut suffix = 2;
    loop {
        let candidate = shared_string(format!("{base}_{suffix}"));
        if seen.insert(Arc::clone(&candidate)) {
            fields.push((candidate, value));
            return;
        }
        suffix += 1;
    }
}

/// 规整字段 key：仅保留 `[A-Za-z0-9_.-]`，其余替换为 `_`，空结果回退为 `_`
fn sanitize_field_key(key: &str) -> String {
    let mut sanitized = String::with_capacity(key.len().max(1));
    // 逐字符过滤，非法字符替换为下划线
    for c in key.chars() {
        if c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.') {
            sanitized.push(c);
        } else {
            sanitized.push('_');
        }
    }
    // 全部被过滤掉时给一个占位 key，避免空字段名
    if sanitized.is_empty() {
        "_".to_string()
    } else {
        sanitized
    }
}

/// tracing 的日志级别数量，用作按级别路由的数组长度
const LEVEL_COUNT: usize = 5;

/// 把日志级别映射为路由数组下标（ERROR=0 .. TRACE=4）
fn level_index(level: &Level) -> usize {
    match *level {
        Level::ERROR => 0,
        Level::WARN => 1,
        Level::INFO => 2,
        Level::DEBUG => 3,
        Level::TRACE => 4,
    }
}

/// 返回 (UNIX 秒, 纳秒余数)
///
/// 秒部分为 `u32`，受阿里云 SLS `Log::from_unixtime` 接口约束（其入参即 `u32`）
/// 这意味着会在 2106 年溢出（u32 秒上限），属于上游 SDK 的限制而非本层可控
fn now_unix() -> (u32, u32) {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        // 正常：秒截断为 u32，纳秒取余数
        Ok(d) => (d.as_secs() as u32, d.subsec_nanos()),
        // 系统时间早于 UNIX 纪元（极罕见）：回退为 0
        Err(_) => (0, 0),
    }
}

/// 事件字段访问器：把各类型字段统一收集为 (key, Arc<str>)，并单独捕获 message
#[derive(Default)]
struct EventVisitor {
    /// 除 message 外的普通字段
    fields: Vec<(String, Arc<str>)>,
    /// 单独捕获的 message 字段
    message: Option<Arc<str>>,
}

impl EventVisitor {
    /// 新建访问器并预留常见字段数容量
    fn new() -> Self {
        Self {
            fields: Vec::with_capacity(8),
            message: None,
        }
    }

    /// 复位以复用（清空字段并丢弃旧 message）
    fn clear(&mut self) {
        self.fields.clear();
        self.message = None;
    }
}

impl Visit for EventVisitor {
    /// 兜底类型：用 Debug 格式化；message 字段去掉外层引号后单独保存
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        let name = field.name();
        let formatted = format!("{value:?}");
        if name == "message" {
            self.message = Some(shared_string(strip_debug_quotes(&formatted)));
        } else {
            self.fields
                .push((name.to_string(), shared_string(formatted)));
        }
    }

    /// 字符串字段：message 单独保存，其余进字段列表
    fn record_str(&mut self, field: &Field, value: &str) {
        let name = field.name();
        let value = shared_str(value);
        if name == "message" {
            self.message = Some(value);
        } else {
            self.fields.push((name.to_string(), value));
        }
    }

    /// i64 字段：转字符串存入
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.fields
            .push((field.name().to_string(), shared_string(value.to_string())));
    }

    /// u64 字段：转字符串存入
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.fields
            .push((field.name().to_string(), shared_string(value.to_string())));
    }

    /// i128 字段：转字符串存入
    fn record_i128(&mut self, field: &Field, value: i128) {
        self.fields
            .push((field.name().to_string(), shared_string(value.to_string())));
    }

    /// u128 字段：转字符串存入
    fn record_u128(&mut self, field: &Field, value: u128) {
        self.fields
            .push((field.name().to_string(), shared_string(value.to_string())));
    }

    /// f64 字段：转字符串存入
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.fields
            .push((field.name().to_string(), shared_string(value.to_string())));
    }

    /// bool 字段：转字符串存入
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.fields
            .push((field.name().to_string(), shared_string(value.to_string())));
    }
}

/// 去掉 `Debug` 格式化给字符串值加的外层引号
fn strip_debug_quotes(value: &str) -> String {
    // 仅当首尾都是引号时剥离，否则原样返回
    if value.len() >= 2 && value.starts_with('"') && value.ends_with('"') {
        value[1..value.len() - 1].to_string()
    } else {
        value.to_string()
    }
}

/// span 字段在创建/更新时即预拼接完整 key，值用 [`Arc<str>`] 共享，event 时仅 clone Arc
struct SpanFields {
    /// 字段 key 的公共前缀，形如 `span.<span_name>.`
    key_prefix: String,
    /// 已拼接好的 (完整 key, 值) 列表
    entries: Vec<(Arc<str>, Arc<str>)>,
    /// 原始字段名 → entries 下标，用于 upsert 去重
    by_name: BTreeMap<String, usize>,
}

impl SpanFields {
    /// 以 span 名构造字段容器，预先生成 key 前缀
    fn new(span_name: &str) -> Self {
        Self {
            key_prefix: format!("span.{}.", sanitize_field_key(span_name)),
            entries: Vec::new(),
            by_name: BTreeMap::new(),
        }
    }

    /// 插入或更新一个字段：已存在则改值，不存在则预拼接完整 key 后追加
    fn upsert(&mut self, name: &str, value: Arc<str>) {
        if let Some(&idx) = self.by_name.get(name) {
            // 已有该字段：仅替换值
            self.entries[idx].1 = value;
        } else {
            // 新字段：用前缀 + sanitize 后的名拼出完整 key
            let full_key =
                shared_string(format!("{}{}", self.key_prefix, sanitize_field_key(name)));
            let idx = self.entries.len();
            self.entries.push((full_key, value));
            self.by_name.insert(name.to_owned(), idx);
        }
    }
}

/// span 字段访问器：把各类型字段 upsert 进 [`SpanFields`]
struct SpanFieldVisitor<'a>(&'a mut SpanFields);

impl Visit for SpanFieldVisitor<'_> {
    /// 兜底类型：用 Debug 格式化后 upsert
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.0
            .upsert(field.name(), shared_string(format!("{value:?}")));
    }

    /// 字符串字段：直接共享后 upsert
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.upsert(field.name(), shared_str(value));
    }

    /// i64 字段：转字符串后 upsert
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.0
            .upsert(field.name(), shared_string(value.to_string()));
    }

    /// u64 字段：转字符串后 upsert
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0
            .upsert(field.name(), shared_string(value.to_string()));
    }

    /// i128 字段：转字符串后 upsert
    fn record_i128(&mut self, field: &Field, value: i128) {
        self.0
            .upsert(field.name(), shared_string(value.to_string()));
    }

    /// u128 字段：转字符串后 upsert
    fn record_u128(&mut self, field: &Field, value: u128) {
        self.0
            .upsert(field.name(), shared_string(value.to_string()));
    }

    /// f64 字段：转字符串后 upsert
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.0
            .upsert(field.name(), shared_string(value.to_string()));
    }

    /// bool 字段：转字符串后 upsert
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.0
            .upsert(field.name(), shared_string(value.to_string()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internal_target_check_does_not_match_prefix_only() {
        // 完全相等与 `prefix::` 前缀都应命中，但同前缀的别的 crate 不应误伤
        assert!(is_internal_target("reqwest"));
        assert!(is_internal_target("reqwest::connect"));
        assert!(!is_internal_target("reqwest_extra"));
    }

    #[test]
    fn push_unique_field_sanitizes_and_suffixes_duplicates() {
        // 三个 key 经 sanitize 后都变为 request_id，应分别加 _2、_3 后缀
        let mut fields = Vec::new();
        let mut seen = HashSet::new();
        push_unique_field(&mut fields, &mut seen, "request id", shared_str("a"));
        push_unique_field(&mut fields, &mut seen, "request/id", shared_str("b"));
        push_unique_field(&mut fields, &mut seen, "request_id", shared_str("c"));

        assert_eq!(
            fields,
            vec![
                (shared_str("request_id"), shared_str("a")),
                (shared_str("request_id_2"), shared_str("b")),
                (shared_str("request_id_3"), shared_str("c")),
            ]
        );
    }

    #[test]
    fn span_fields_prebuilds_full_keys_and_shares_values() {
        // 同名字段 upsert 应改值而非新增，并保持预拼接的完整 key
        let mut sf = SpanFields::new("http.request");
        sf.upsert("request_id", shared_str("abc"));
        sf.upsert("request_id", shared_str("def"));

        assert_eq!(sf.entries.len(), 1);
        assert_eq!(sf.entries[0].0.as_ref(), "span.http.request.request_id");
        assert_eq!(sf.entries[0].1.as_ref(), "def");

        // 不同名字段应新增一项
        sf.upsert("status", shared_str("200"));
        assert_eq!(sf.entries.len(), 2);
        assert_eq!(sf.entries[1].0.as_ref(), "span.http.request.status");
    }

    #[test]
    fn sanitize_empty_field_key() {
        // 空 key 应回退为占位 _
        assert_eq!(sanitize_field_key(""), "_");
    }

    #[test]
    fn level_index_round_trips_with_str() {
        // 所有级别下标都应落在数组范围内
        for level in [
            Level::ERROR,
            Level::WARN,
            Level::INFO,
            Level::DEBUG,
            Level::TRACE,
        ] {
            assert!(level_index(&level) < LEVEL_COUNT);
        }
        // 边界：ERROR=0，TRACE=末位
        assert_eq!(level_index(&Level::ERROR), 0);
        assert_eq!(level_index(&Level::TRACE), LEVEL_COUNT - 1);
    }

    #[test]
    fn strip_debug_quotes_removes_outer_quotes() {
        // 带引号剥离，无引号原样返回
        assert_eq!(strip_debug_quotes("\"hello\""), "hello");
        assert_eq!(strip_debug_quotes("plain"), "plain");
    }

    #[test]
    fn level_tag_compacts_levels() {
        // 级别压缩为首字母标签；空集合回退为 _
        assert_eq!(level_tag(&[Level::ERROR, Level::WARN]), "EW");
        assert_eq!(level_tag(&[Level::INFO, Level::DEBUG, Level::TRACE]), "IDT");
        assert_eq!(level_tag(&[]), "_");
    }

    #[test]
    fn builder_empty_returns_no_sinks_error() {
        // 未注册任何 sink 时 build 应返回 NoSinks
        assert!(matches!(
            SlsLayerBuilder::new().build(),
            Err(SlsBuildError::NoSinks)
        ));
    }

    #[test]
    fn builder_routes_levels_first_registration_wins() {
        // 不启动真实 sink，仅验证路由表装配逻辑
        let mut routing: [Option<usize>; LEVEL_COUNT] = [None; LEVEL_COUNT];
        assign_routes(&mut routing, &[Level::ERROR, Level::WARN], 0);
        assign_routes(&mut routing, &[Level::INFO, Level::DEBUG, Level::TRACE], 1);
        // 重复声明 ERROR 应被忽略（先注册者优先）
        assign_routes(&mut routing, &[Level::ERROR], 2);

        assert_eq!(routing[level_index(&Level::ERROR)], Some(0));
        assert_eq!(routing[level_index(&Level::WARN)], Some(0));
        assert_eq!(routing[level_index(&Level::INFO)], Some(1));
        assert_eq!(routing[level_index(&Level::DEBUG)], Some(1));
        assert_eq!(routing[level_index(&Level::TRACE)], Some(1));
    }
}
