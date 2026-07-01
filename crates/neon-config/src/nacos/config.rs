use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use config::{Config, Map, Value};
use nacos_sdk::api::config::{
    ConfigChangeListener, ConfigResponse, ConfigService, ConfigServiceBuilder,
};
use nacos_sdk::api::props::ClientProps;
use neon_core::traits::IntoStrVec;
use serde::Deserialize;
use tokio::sync::{Mutex, Notify, watch};
use tokio::time;

use crate::nacos::env::Env;
use crate::nacos::error::{ConfigError, Result};
use crate::nacos::format::Format;

/// nacos 默认分组
pub const DEFAULT_GROUP: &str = "DEFAULT_GROUP";
/// nacos 初始化默认超时时间
pub const DEFAULT_STARTUP_TIMEOUT: Duration = Duration::from_secs(30);

/// 本地配置来源
#[derive(Debug, Clone)]
enum LocalSource {
    /// 本地文件，可选显式格式（缺省时按扩展名推断）
    File {
        path: PathBuf,
        format: Option<Format>,
    },
    /// 内联配置内容（需指定格式）
    Inline { content: String, format: Format },
}

/// nacos 配置数据源
///
/// 内部映射到 `nacos-sdk` 的 [`ClientProps`]，统一配置连接地址、鉴权与 data-id 等参数
/// 默认通过直连 `server_addr` 连接；如需 endpoint 寻址可使用 [`NacosSource::from_endpoint`]
#[derive(Clone)]
pub struct NacosSource {
    /// 直连服务地址列表，例如 `["127.0.0.1:8848"]`
    server_addr: Vec<String>,
    /// 命名空间 id，`public` 对应空字符串
    namespace: String,
    /// 分组，缺省 `DEFAULT_GROUP`
    group: String,
    /// 配置 data-ids，例如 `["app.yaml", "app.json"]`
    data_id: Vec<String>,
    /// 应用名（可选）
    app_name: Option<String>,
    /// 鉴权用户名（可选）
    username: Option<String>,
    /// 鉴权密码（可选）
    password: Option<String>,
    /// 寻址模式 endpoint，用于动态解析服务列表
    endpoint: Option<String>,
    /// 阿里云 region id（可选）
    region_id: Option<String>,
    /// 阿里云 RAM/ACM access key（可选）
    access_key: Option<String>,
    /// 阿里云 RAM/ACM secret key（可选）
    secret_key: Option<String>,
    /// 显式指定格式；缺省时按 nacos 返回的 content_type 或 data-id 扩展名推断
    format: Option<Format>,
    /// 按 data-id 指定格式，优先级高于 [`Self::format`]
    data_id_formats: HashMap<String, Format>,
    /// 启动时加载本地缓存（映射 `ClientProps::load_cache_at_start`）
    load_cache_at_start: Option<bool>,
    /// 远程 gRPC 端口（映射 `ClientProps::remote_grpc_port`）
    grpc_port: Option<u16>,
    /// 初始化阶段超时时间
    startup_timeout: Duration,
}

impl std::fmt::Debug for NacosSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NacosSource")
            .field("server_addr", &self.server_addr)
            .field("namespace", &self.namespace)
            .field("group", &self.group)
            .field("data_id", &self.data_id)
            .field("app_name", &self.app_name)
            .field("username", &self.username)
            .field("password", &self.password)
            .field("endpoint", &self.endpoint)
            .field("region_id", &self.region_id)
            .field("access_key", &self.access_key)
            .field("secret_key", &self.secret_key)
            .field("format", &self.format)
            .field("data_id_formats", &self.data_id_formats)
            .field("load_cache_at_start", &self.load_cache_at_start)
            .field("grpc_port", &self.grpc_port)
            .field("startup_timeout", &self.startup_timeout)
            .finish()
    }
}

impl NacosSource {
    /// 以直连服务地址创建数据源
    ///
    /// `server_addr` 支持单地址字符串或地址列表，例如：
    /// - `"127.0.0.1:8848"`
    /// - `["127.0.0.1:8848", "192.168.0.1:8848"]`
    pub fn new(server_addr: impl IntoStrVec, namespace: impl Into<String>) -> Self {
        Self {
            server_addr: server_addr.into_str_vec(),
            namespace: namespace.into(),
            group: DEFAULT_GROUP.to_string(),
            data_id: Vec::new(),
            app_name: None,
            username: None,
            password: None,
            endpoint: None,
            region_id: None,
            access_key: None,
            secret_key: None,
            format: None,
            data_id_formats: HashMap::new(),
            load_cache_at_start: None,
            grpc_port: None,
            startup_timeout: DEFAULT_STARTUP_TIMEOUT,
        }
    }

    /// 以 endpoint 寻址方式创建数据源
    pub fn from_endpoint(endpoint: impl Into<String>, namespace: impl Into<String>) -> Self {
        Self {
            server_addr: Vec::new(),
            namespace: namespace.into(),
            group: DEFAULT_GROUP.to_string(),
            data_id: Vec::new(),
            app_name: None,
            username: None,
            password: None,
            endpoint: Some(endpoint.into()),
            region_id: None,
            access_key: None,
            secret_key: None,
            format: None,
            data_id_formats: HashMap::new(),
            load_cache_at_start: None,
            grpc_port: None,
            startup_timeout: DEFAULT_STARTUP_TIMEOUT,
        }
    }

    /// 设置分组
    pub fn group(mut self, group: impl Into<String>) -> Self {
        self.group = group.into();
        self
    }

    /// 配置 data-id，例如 `["app.yaml", "shared.yaml"]`
    ///
    /// 多个 data-id 会按顺序合并，后者覆盖前者同名键
    pub fn data_id(mut self, data_id: impl IntoStrVec) -> Self {
        self.data_id = data_id.into_str_vec();
        self
    }

    /// 设置应用名
    pub fn app_name(mut self, app_name: impl Into<String>) -> Self {
        self.app_name = Some(app_name.into());
        self
    }

    /// 设置鉴权账号密码
    pub fn auth(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.username = Some(username.into());
        self.password = Some(password.into());
        self
    }

    /// 设置阿里云 region id
    pub fn region_id(mut self, region_id: impl Into<String>) -> Self {
        self.region_id = Some(region_id.into());
        self
    }

    /// 设置阿里云 RAM/ACM access key
    pub fn access_key(mut self, access_key: impl Into<String>) -> Self {
        self.access_key = Some(access_key.into());
        self
    }

    /// 设置阿里云 RAM/ACM secret key
    pub fn secret_key(mut self, secret_key: impl Into<String>) -> Self {
        self.secret_key = Some(secret_key.into());
        self
    }

    /// 显式指定格式
    pub fn format(mut self, format: Format) -> Self {
        self.format = Some(format);
        self
    }

    fn validate_data_id_list(data_ids: &[String], label: &str) -> Result<()> {
        if data_ids.is_empty() || data_ids.iter().any(|id| id.trim().is_empty()) {
            return Err(invalid_nacos_source(format!("{label} 不能为空")));
        }
        if data_ids.iter().collect::<HashSet<_>>().len() != data_ids.len() {
            return Err(invalid_nacos_source(format!("{label} 不能包含重复项")));
        }
        Ok(())
    }

    /// 为指定 data-id 设置格式（优先级高于 [`Self::format`]）
    pub fn format_for(mut self, data_id: impl Into<String>, format: Format) -> Self {
        self.data_id_formats.insert(data_id.into(), format);
        self
    }

    /// 启动时加载 nacos 本地缓存
    pub fn load_cache_at_start(mut self, enabled: bool) -> Self {
        self.load_cache_at_start = Some(enabled);
        self
    }

    /// 设置远程 gRPC 端口
    pub fn grpc_port(mut self, port: u16) -> Self {
        self.grpc_port = Some(port);
        self
    }

    /// 设置 nacos 初始化阶段超时时间
    pub fn startup_timeout(mut self, timeout: Duration) -> Self {
        self.startup_timeout = timeout;
        self
    }

    /// data-id 合并顺序
    fn merge_order(&self) -> Vec<String> {
        self.data_id.clone()
    }

    fn explicit_format_for(&self, data_id: &str) -> Option<Format> {
        self.data_id_formats.get(data_id).copied().or(self.format)
    }

    fn validate(&self) -> Result<()> {
        if self.data_id.is_empty() {
            return Err(invalid_nacos_source("data_id 不能为空"));
        }
        Self::validate_data_id_list(&self.data_id, "data_id")?;
        if self.group.trim().is_empty() {
            return Err(invalid_nacos_source("group 不能为空"));
        }

        let has_endpoint = self
            .endpoint
            .as_deref()
            .is_some_and(|endpoint| !endpoint.trim().is_empty());
        let has_server_addr = self.server_addr.iter().any(|addr| !addr.trim().is_empty());
        if !has_endpoint && !has_server_addr {
            return Err(invalid_nacos_source(
                "endpoint 和 server_addr 至少需要配置一个",
            ));
        }
        if self.server_addr.iter().any(|addr| addr.trim().is_empty()) {
            return Err(invalid_nacos_source("server_addr 不能包含空地址"));
        }
        if self.startup_timeout.is_zero() {
            return Err(invalid_nacos_source("startup_timeout 必须大于 0"));
        }

        let has_http_auth = self.username.is_some() || self.password.is_some();
        let has_access_key = self
            .access_key
            .as_deref()
            .is_some_and(|v| !v.trim().is_empty());
        let has_secret_key = self
            .secret_key
            .as_deref()
            .is_some_and(|v| !v.trim().is_empty());
        let has_region_id = self
            .region_id
            .as_deref()
            .is_some_and(|v| !v.trim().is_empty());
        let has_aliyun_auth = has_access_key || has_secret_key || has_region_id;
        if has_http_auth && has_aliyun_auth {
            return Err(invalid_nacos_source(
                "username/password 与 access_key/secret_key/region_id 不能同时配置",
            ));
        }

        match (&self.username, &self.password) {
            (Some(username), Some(password))
                if !username.trim().is_empty() && !password.trim().is_empty() =>
            {
                return Ok(());
            }
            (None, None) => {}
            _ => Err(invalid_nacos_source(
                "username 和 password 必须同时配置且不能为空",
            ))?,
        }

        if has_region_id && !has_access_key && !has_secret_key {
            return Err(invalid_nacos_source(
                "region_id 不能单独配置，需同时提供 access_key 和 secret_key",
            ));
        }

        match (has_access_key, has_secret_key) {
            (true, true) => Ok(()),
            (false, false) => Ok(()),
            _ => Err(invalid_nacos_source(
                "access_key 和 secret_key 必须同时配置且不能为空",
            )),
        }
    }
}

/// 远程 nacos 连接句柄，用于移除监听器并释放连接
struct RemoteHandle {
    service: ConfigService,
    group: String,
    listener: Arc<dyn ConfigChangeListener>,
    data_ids: Vec<String>,
    reload_worker: tokio::task::JoinHandle<()>,
}

/// Nacos 配置
///
/// 内部以 [`tokio::sync::watch`] 通道保存最新的 [`config::Config`]：读取廉价、且支持订阅热更新
/// 在远程环境下，会持有 nacos [`ConfigService`] 以维持长连接与变更监听
///
/// 取值方式：
/// - 动态取值：[`NacosConfig::get_int`] / [`NacosConfig::get_string`] 等，或先 [`NacosConfig::get`] 拿到快照再 `cfg.get_int("a.b")`
/// - 结构化反序列化：[`NacosConfig::get_as`]（按 key）或 [`NacosConfig::try_deserialize`]（整体）
pub struct NacosConfig {
    tx: watch::Sender<Arc<Config>>,
    /// 按 data-id 细分的变更通知；仅在该 data-id 变更时推送
    data_id_txs: HashMap<String, watch::Sender<Arc<Config>>>,
    /// data-id 合并顺序（远程环境有效）
    data_id_order: Vec<String>,
    env: Env,
    remote: Option<RemoteHandle>,
}

impl NacosConfig {
    /// 创建构建器
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder::default()
    }

    /// 获取当前配置快照（[`config::Config`]），可直接调用 `cfg.get_int("a.b")` 等方法
    pub fn get(&self) -> Arc<Config> {
        self.tx.borrow().clone()
    }

    /// 订阅合并后配置的变更
    ///
    /// 任一 data-id 变更都会触发（推送的是合并后的完整快照）
    /// 若需仅感知某个 data-id 的变更，请使用 [`Self::subscribe_data_id`]
    pub fn subscribe(&self) -> watch::Receiver<Arc<Config>> {
        self.tx.subscribe()
    }

    /// 订阅指定 data-id 的变更
    ///
    /// 仅当该 data-id 发生热更新时才会通知；返回的仍是合并后的完整配置快照
    /// 若 data-id 不存在，返回 `None`
    pub fn subscribe_data_id(&self, data_id: &str) -> Option<watch::Receiver<Arc<Config>>> {
        self.data_id_txs.get(data_id).map(|tx| tx.subscribe())
    }

    /// 当前配置的 data-id 列表（可用于 [`Self::subscribe_data_id`]），顺序与合并顺序一致
    pub fn data_ids(&self) -> Vec<&str> {
        self.data_id_order.iter().map(String::as_str).collect()
    }

    /// 当前运行环境
    pub fn env(&self) -> &Env {
        &self.env
    }

    /// 是否处于本地环境
    pub fn is_local(&self) -> bool {
        self.env.is_local()
    }

    /// 按路径读取整数，例如 `get_int("server.port")`
    pub fn get_int(&self, key: &str) -> Result<i64> {
        Ok(self.get().get_int(key)?)
    }

    /// 按路径读取浮点数
    pub fn get_float(&self, key: &str) -> Result<f64> {
        Ok(self.get().get_float(key)?)
    }

    /// 按路径读取布尔值
    pub fn get_bool(&self, key: &str) -> Result<bool> {
        Ok(self.get().get_bool(key)?)
    }

    /// 按路径读取字符串
    pub fn get_string(&self, key: &str) -> Result<String> {
        Ok(self.get().get_string(key)?)
    }

    /// 按路径读取数组
    pub fn get_array(&self, key: &str) -> Result<Vec<Value>> {
        Ok(self.get().get_array(key)?)
    }

    /// 按路径读取表（对象）
    pub fn get_table(&self, key: &str) -> Result<Map<String, Value>> {
        Ok(self.get().get_table(key)?)
    }

    /// 按路径反序列化为任意类型，例如 `get_as::<ServerConfig>("server")`
    pub fn get_as<'de, T: Deserialize<'de>>(&self, key: &str) -> Result<T> {
        Ok(self.get().get::<T>(key)?)
    }

    /// 将整份配置反序列化为目标结构体
    pub fn try_deserialize<'de, T: Deserialize<'de>>(&self) -> Result<T> {
        Ok(self.get().as_ref().clone().try_deserialize::<T>()?)
    }

    /// 移除 nacos 监听器并释放远程连接；调用后仍可通过 [`Self::get`] 读取最后快照
    pub async fn shutdown(&mut self) -> Result<()> {
        let Some(remote) = self.remote.take() else {
            return Ok(());
        };
        let mut first_error = None;
        for data_id in &remote.data_ids {
            if let Err(err) = remote
                .service
                .remove_listener(
                    data_id.clone(),
                    remote.group.clone(),
                    remote.listener.clone(),
                )
                .await
            {
                tracing::warn!(data_id = %data_id, error = %err, "移除 nacos 配置监听器失败");
                if first_error.is_none() {
                    first_error = Some(ConfigError::from(err));
                }
            }
        }
        abort_reload_worker(remote.reload_worker).await;
        tracing::info!(data_ids = ?remote.data_ids, "nacos 配置监听器已移除");
        if let Some(err) = first_error {
            Err(err)
        } else {
            Ok(())
        }
    }

    /// 是否仍持有远程 nacos 连接与监听器
    pub fn is_remote_active(&self) -> bool {
        self.remote.is_some()
    }
}

/// 配置汇聚构建器
#[derive(Default)]
pub struct ConfigBuilder {
    local: Option<LocalSource>,
    nacos: Option<NacosSource>,
}

impl ConfigBuilder {
    /// 设置本地配置文件路径（按扩展名推断格式）
    pub fn local_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.local = Some(LocalSource::File {
            path: path.into(),
            format: None,
        });
        self
    }

    /// 设置本地配置文件路径，并显式指定格式
    pub fn local_file_with_format(mut self, path: impl Into<PathBuf>, format: Format) -> Self {
        self.local = Some(LocalSource::File {
            path: path.into(),
            format: Some(format),
        });
        self
    }

    /// 设置本地内联配置内容
    pub fn local_inline(mut self, content: impl Into<String>, format: Format) -> Self {
        self.local = Some(LocalSource::Inline {
            content: content.into(),
            format,
        });
        self
    }

    /// 设置 nacos 数据源（远程环境必填）
    pub fn nacos(mut self, source: NacosSource) -> Self {
        self.nacos = Some(source);
        self
    }

    /// 构建配置汇聚并完成首次加载（无需指定类型）
    ///
    /// `env` 决定配置来源：本地环境加载本地文件/内联内容；远程环境连接 nacos 并注册热更新
    pub async fn build(self, env: Env) -> Result<NacosConfig> {
        tracing::info!(env = env.name(), "初始化配置");

        if env.is_local() {
            self.build_local(env)
        } else {
            self.build_remote(env).await
        }
    }

    fn build_local(self, env: Env) -> Result<NacosConfig> {
        let source = self.local.ok_or(ConfigError::MissingLocalSource)?;
        let config = match source {
            LocalSource::Inline { content, format } => parse_content(&content, format)?,
            LocalSource::File { path, format } => parse_file(&path, format)?,
        };
        let config = Arc::new(config);
        let (tx, _rx) = watch::channel(config);
        tracing::info!(env = env.name(), "本地配置加载完成");
        Ok(NacosConfig {
            tx,
            data_id_txs: HashMap::new(),
            data_id_order: Vec::new(),
            env,
            remote: None,
        })
    }

    async fn build_remote(self, env: Env) -> Result<NacosConfig> {
        let source = self.nacos.ok_or_else(|| ConfigError::MissingNacosSource {
            env: env.name().to_string(),
        })?;
        source.validate()?;

        let mut props = ClientProps::new().namespace(source.namespace.clone());
        if let Some(endpoint) = source
            .endpoint
            .as_deref()
            .filter(|endpoint| !endpoint.trim().is_empty())
        {
            props = props.endpoint(endpoint.trim().to_string());
        }
        if !source.server_addr.is_empty() {
            props = props.server_addr(
                source
                    .server_addr
                    .iter()
                    .map(|addr| addr.trim())
                    .collect::<Vec<_>>()
                    .join(","),
            );
        }
        if let Some(app_name) = &source.app_name {
            props = props.app_name(app_name.clone());
        }
        if let (Some(username), Some(password)) = (&source.username, &source.password) {
            props = props
                .auth_username(username.clone())
                .auth_password(password.clone());
        }
        if let (Some(access_key), Some(secret_key)) = (&source.access_key, &source.secret_key) {
            props = props
                .auth_access_key(access_key.clone())
                .auth_access_secret(secret_key.clone());
        }
        if let Some(region_id) = &source.region_id {
            props = props.auth_signature_region_id(region_id.clone());
        }
        if let Some(enabled) = source.load_cache_at_start {
            props = props.load_cache_at_start(enabled);
        }
        if let Some(grpc_port) = source.grpc_port {
            props = props.remote_grpc_port(grpc_port);
        }

        let mut builder = ConfigServiceBuilder::new(props);
        if source.username.is_some() {
            builder = builder.enable_auth_plugin_http();
        }
        if source.access_key.is_some() {
            builder = builder.enable_auth_plugin_aliyun();
        }
        let service =
            with_startup_timeout(source.startup_timeout, "build_service", builder.build()).await?;
        let merge_order = source.merge_order();

        let mut entries = HashMap::with_capacity(merge_order.len());
        for data_id in &merge_order {
            let resp = with_startup_timeout(
                source.startup_timeout,
                "get_config",
                service.get_config(data_id.clone(), source.group.clone()),
            )
            .await?;
            let format = resolve_format(&source, data_id, Some(&resp))?;
            entries.insert(
                data_id.clone(),
                ConfigEntry {
                    content: resp.content().to_string(),
                    format,
                },
            );
        }
        let config = merge_configs(&merge_order, &entries)?;
        let config = Arc::new(config);
        let (tx, _rx) = watch::channel(config.clone());
        let mut data_id_txs = HashMap::with_capacity(merge_order.len());
        for data_id in &merge_order {
            let (data_tx, _data_rx) = watch::channel(tx.borrow().clone());
            data_id_txs.insert(data_id.clone(), data_tx);
        }
        tracing::info!(
            data_ids = ?merge_order,
            group = %source.group,
            "Nacos配置加载完成"
        );

        let listener = Arc::new(ReloadListener {
            tx: tx.clone(),
            data_id_txs: Arc::new(data_id_txs.clone()),
            default_format: source.format,
            data_id_formats: source.data_id_formats.clone(),
            data_id_order: merge_order.clone(),
            entries: Arc::new(Mutex::new(entries)),
            pending_updates: Arc::new(StdMutex::new(HashMap::new())),
            update_notify: Arc::new(Notify::new()),
        });
        let worker_listener = listener.clone();
        let reload_worker = tokio::spawn(async move {
            run_reload_worker(worker_listener).await;
        });
        let listener_trait: Arc<dyn ConfigChangeListener> = listener;
        let mut registered_data_ids = Vec::with_capacity(merge_order.len());
        for data_id in &merge_order {
            if let Err(err) = with_startup_timeout(
                source.startup_timeout,
                "add_listener",
                service.add_listener(
                    data_id.clone(),
                    source.group.clone(),
                    listener_trait.clone(),
                ),
            )
            .await
            {
                cleanup_registered_listeners(
                    &service,
                    &source.group,
                    listener_trait.clone(),
                    &registered_data_ids,
                )
                .await;
                abort_reload_worker(reload_worker).await;
                return Err(err);
            }
            registered_data_ids.push(data_id.clone());
        }
        tracing::info!(data_ids = ?merge_order, "已注册 nacos 配置热更新监听器");

        Ok(NacosConfig {
            tx,
            data_id_txs,
            data_id_order: merge_order.clone(),
            env,
            remote: Some(RemoteHandle {
                service,
                group: source.group.clone(),
                listener: listener_trait,
                data_ids: merge_order,
                reload_worker,
            }),
        })
    }
}

/// 单个 data-id 对应的原始配置内容与格式
#[derive(Debug, Clone)]
struct ConfigEntry {
    content: String,
    format: Format,
}

/// 按 data-id 顺序合并多个配置；后者覆盖前者同名键
///
/// `merge_order` 中的每个 data-id 都必须在 `entries` 中存在，缺失任一条目都会返回
/// [`ConfigError::MissingDataIdEntry`]，避免静默合并出不完整的配置
fn merge_configs(merge_order: &[String], entries: &HashMap<String, ConfigEntry>) -> Result<Config> {
    if merge_order.is_empty() {
        return Err(invalid_nacos_source("没有可合并的配置内容"));
    }
    let mut builder = Config::builder();
    for data_id in merge_order {
        let entry = entries
            .get(data_id)
            .ok_or_else(|| ConfigError::MissingDataIdEntry {
                data_id: data_id.clone(),
            })?;
        builder = builder.add_source(config::File::from_str(
            &entry.content,
            config::FileFormat::from(entry.format),
        ));
    }
    Ok(builder.build()?)
}

/// nacos 配置变更监听器：按 data-id 合并待处理变更，再推送到 watch 通道
struct ReloadListener {
    tx: watch::Sender<Arc<Config>>,
    data_id_txs: Arc<HashMap<String, watch::Sender<Arc<Config>>>>,
    default_format: Option<Format>,
    data_id_formats: HashMap<String, Format>,
    data_id_order: Vec<String>,
    entries: Arc<Mutex<HashMap<String, ConfigEntry>>>,
    pending_updates: Arc<StdMutex<HashMap<String, ConfigEntry>>>,
    update_notify: Arc<Notify>,
}

impl ReloadListener {
    fn enqueue_update(&self, data_id: String, content: String, format: Format) {
        let entry = ConfigEntry { content, format };
        match self.pending_updates.lock() {
            Ok(mut pending) => {
                pending.insert(data_id, entry);
            }
            Err(err) => {
                tracing::warn!("配置热更新待处理区锁已被污染，继续保留最新变更");
                err.into_inner().insert(data_id, entry);
            }
        }
        self.update_notify.notify_one();
    }

    async fn apply_batch(&self, batch: HashMap<String, ConfigEntry>) {
        if batch.is_empty() {
            return;
        }

        let mut entries = self.entries.lock().await;

        let mut rollback: HashMap<String, Option<ConfigEntry>> = HashMap::new();
        for (data_id, entry) in &batch {
            rollback.insert(data_id.clone(), entries.get(data_id).cloned());
            entries.insert(data_id.clone(), entry.clone());
        }

        let changed_data_ids: Vec<String> = self
            .data_id_order
            .iter()
            .filter(|data_id| batch.contains_key(data_id.as_str()))
            .cloned()
            .collect();

        match merge_configs(&self.data_id_order, &entries) {
            Ok(config) => {
                let config = Arc::new(config);
                self.tx.send_replace(config.clone());
                for data_id in &changed_data_ids {
                    if let Some(data_tx) = self.data_id_txs.get(data_id) {
                        data_tx.send_replace(config.clone());
                    }
                }
                tracing::info!(count = batch.len(), "配置热更新成功");
            }
            Err(err) => {
                for (data_id, previous) in rollback {
                    restore_entry(&mut entries, &data_id, previous);
                }
                tracing::error!(
                    data_ids = ?batch.keys().collect::<Vec<_>>(),
                    error = %err,
                    "配置热更新合并失败，已回滚 entries 并沿用旧配置"
                );
            }
        }
    }
}

async fn run_reload_worker(listener: Arc<ReloadListener>) {
    loop {
        listener.update_notify.notified().await;
        loop {
            let batch = match listener.pending_updates.lock() {
                Ok(mut pending) => std::mem::take(&mut *pending),
                Err(err) => {
                    tracing::warn!("配置热更新待处理区锁已被污染，继续处理待合并变更");
                    std::mem::take(&mut *err.into_inner())
                }
            };
            if batch.is_empty() {
                break;
            }
            listener.apply_batch(batch).await;
        }
    }
}

fn restore_entry(
    entries: &mut HashMap<String, ConfigEntry>,
    data_id: &str,
    previous: Option<ConfigEntry>,
) {
    match previous {
        Some(entry) => {
            entries.insert(data_id.to_string(), entry);
        }
        None => {
            entries.remove(data_id);
        }
    }
}

async fn abort_reload_worker(reload_worker: tokio::task::JoinHandle<()>) {
    reload_worker.abort();
    let _ = reload_worker.await;
}

async fn cleanup_registered_listeners(
    service: &ConfigService,
    group: &str,
    listener: Arc<dyn ConfigChangeListener>,
    data_ids: &[String],
) {
    for data_id in data_ids {
        if let Err(err) = service
            .remove_listener(data_id.clone(), group.to_string(), listener.clone())
            .await
        {
            tracing::warn!(data_id = %data_id, error = %err, "回滚 nacos 配置监听器注册失败");
        }
    }
}

async fn with_startup_timeout<T, F>(
    timeout: Duration,
    operation: &'static str,
    future: F,
) -> Result<T>
where
    F: std::future::Future<Output = std::result::Result<T, nacos_sdk::api::error::Error>>,
{
    time::timeout(timeout, future)
        .await
        .map_err(|_| ConfigError::NacosStartupTimeout { operation, timeout })?
        .map_err(ConfigError::from)
}

impl ConfigChangeListener for ReloadListener {
    fn notify(&self, config_resp: ConfigResponse) {
        let data_id = config_resp.data_id().to_string();
        let format = resolve_format_for_entry(
            self.data_id_formats
                .get(&data_id)
                .copied()
                .or(self.default_format),
            &data_id,
            Some(config_resp.content_type()),
        );
        let Some(format) = format else {
            tracing::error!(
                data_id = %data_id,
                content_type = %config_resp.content_type(),
                "无法推断变更配置的格式，跳过本次热更新"
            );
            return;
        };

        self.enqueue_update(data_id, config_resp.content().to_string(), format);
    }
}

/// 推断单个 data-id 的配置格式：显式 > content_type > data-id 扩展名
fn resolve_format(
    source: &NacosSource,
    data_id: &str,
    resp: Option<&ConfigResponse>,
) -> Result<Format> {
    resolve_format_for_entry(
        source.explicit_format_for(data_id),
        data_id,
        resp.map(|resp| resp.content_type().as_str()),
    )
    .ok_or_else(|| ConfigError::UnknownFormat {
        hint: data_id.to_string(),
    })
}

fn resolve_format_for_entry(
    explicit_format: Option<Format>,
    data_id: &str,
    content_type: Option<&str>,
) -> Option<Format> {
    if let Some(format) = explicit_format {
        return Some(format);
    }
    if let Some(content_type) = content_type
        && let Some(format) = Format::from_content_type(content_type)
    {
        return Some(format);
    }
    Format::from_path(Path::new(data_id))
}

fn invalid_nacos_source(reason: impl Into<String>) -> ConfigError {
    ConfigError::InvalidNacosSource {
        reason: reason.into(),
    }
}

/// 使用 `config` crate 从本地文件解析配置
fn parse_file(path: &Path, format: Option<Format>) -> Result<Config> {
    let content = std::fs::read_to_string(path).map_err(|source| ConfigError::ReadFile {
        path: path.to_path_buf(),
        source,
    })?;
    let format =
        format
            .or_else(|| Format::from_path(path))
            .ok_or_else(|| ConfigError::UnknownFormat {
                hint: path.display().to_string(),
            })?;
    parse_content(&content, format)
}

/// 使用 `config` crate 将文本解析为 [`config::Config`]
fn parse_content(content: &str, format: Format) -> Result<Config> {
    let config = Config::builder()
        .add_source(config::File::from_str(
            content,
            config::FileFormat::from(format),
        ))
        .build()?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_response(content: &str, content_type: &str) -> ConfigResponse {
        ConfigResponse::new(
            "app.toml".to_string(),
            DEFAULT_GROUP.to_string(),
            String::new(),
            content.to_string(),
            content_type.to_string(),
            "md5".to_string(),
        )
    }

    #[test]
    fn parse_content_supports_nested_values() {
        let cfg = parse_content(
            r#"
name = "demo"

[server]
port = 8080
"#,
            Format::Toml,
        )
        .expect("toml config should parse");

        assert_eq!(cfg.get_string("name").unwrap(), "demo");
        assert_eq!(cfg.get_int("server.port").unwrap(), 8080);
    }

    #[test]
    fn parse_file_uses_config_file_source() {
        let path =
            std::env::temp_dir().join(format!("nacos-rs-config-test-{}.toml", std::process::id()));
        std::fs::write(
            &path,
            r#"
name = "file-demo"

[server]
port = 7070
"#,
        )
        .expect("test config file should be written");

        let cfg = parse_file(&path, None).expect("file config should parse");
        std::fs::remove_file(&path).expect("test config file should be removed");

        assert_eq!(cfg.get_string("name").unwrap(), "file-demo");
        assert_eq!(cfg.get_int("server.port").unwrap(), 7070);
    }

    fn apply_batch_sync(listener: &ReloadListener, batch: HashMap<String, ConfigEntry>) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("apply batch runtime")
            .block_on(listener.apply_batch(batch));
    }

    fn test_reload_listener(
        tx: watch::Sender<Arc<Config>>,
        data_id_txs: HashMap<String, watch::Sender<Arc<Config>>>,
        data_id_order: Vec<String>,
        entries: HashMap<String, ConfigEntry>,
    ) -> ReloadListener {
        ReloadListener {
            tx,
            data_id_txs: Arc::new(data_id_txs),
            default_format: Some(Format::Toml),
            data_id_formats: HashMap::new(),
            data_id_order,
            entries: Arc::new(Mutex::new(entries)),
            pending_updates: Arc::new(StdMutex::new(HashMap::new())),
            update_notify: Arc::new(Notify::new()),
        }
    }

    fn spawn_test_reload_listener(
        tx: watch::Sender<Arc<Config>>,
        data_id_txs: HashMap<String, watch::Sender<Arc<Config>>>,
        data_id_order: Vec<String>,
        entries: HashMap<String, ConfigEntry>,
    ) -> Arc<ReloadListener> {
        let listener = Arc::new(ReloadListener {
            tx,
            data_id_txs: Arc::new(data_id_txs),
            default_format: Some(Format::Toml),
            data_id_formats: HashMap::new(),
            data_id_order,
            entries: Arc::new(Mutex::new(entries)),
            pending_updates: Arc::new(StdMutex::new(HashMap::new())),
            update_notify: Arc::new(Notify::new()),
        });
        let worker_listener = listener.clone();
        tokio::spawn(async move {
            run_reload_worker(worker_listener).await;
        });
        listener
    }

    async fn wait_for_reload() {
        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    #[tokio::test]
    async fn reload_updates_snapshot_without_subscribers() {
        let initial = parse_content(
            r#"
[server]
port = 8080
"#,
            Format::Toml,
        )
        .expect("initial config should parse");
        let (tx, rx) = watch::channel(Arc::new(initial));
        drop(rx);

        let mut entries = HashMap::new();
        entries.insert(
            "app.toml".to_string(),
            ConfigEntry {
                content: r#"
[server]
port = 8080
"#
                .to_string(),
                format: Format::Toml,
            },
        );
        let (data_tx, _data_rx) = watch::channel(tx.borrow().clone());
        let mut data_id_txs = HashMap::new();
        data_id_txs.insert("app.toml".to_string(), data_tx);
        let listener = spawn_test_reload_listener(
            tx.clone(),
            data_id_txs,
            vec!["app.toml".to_string()],
            entries,
        );
        listener.notify(config_response(
            r#"
[server]
port = 9090
"#,
            "toml",
        ));
        wait_for_reload().await;

        assert_eq!(tx.borrow().get_int("server.port").unwrap(), 9090);
    }

    #[tokio::test]
    async fn reload_keeps_old_snapshot_when_new_config_is_invalid() {
        let initial = parse_content(
            r#"
[server]
port = 8080
"#,
            Format::Toml,
        )
        .expect("initial config should parse");
        let (tx, rx) = watch::channel(Arc::new(initial));
        drop(rx);

        let mut entries = HashMap::new();
        entries.insert(
            "app.toml".to_string(),
            ConfigEntry {
                content: r#"
[server]
port = 8080
"#
                .to_string(),
                format: Format::Toml,
            },
        );
        let (data_tx, _data_rx) = watch::channel(tx.borrow().clone());
        let mut data_id_txs = HashMap::new();
        data_id_txs.insert("app.toml".to_string(), data_tx);
        let listener = spawn_test_reload_listener(
            tx.clone(),
            data_id_txs,
            vec!["app.toml".to_string()],
            entries,
        );
        listener.notify(config_response("not = [valid", "toml"));
        wait_for_reload().await;

        assert_eq!(tx.borrow().get_int("server.port").unwrap(), 8080);
    }

    #[test]
    fn merge_configs_later_data_id_overrides_earlier_keys() {
        let mut entries = HashMap::new();
        entries.insert(
            "base.toml".to_string(),
            ConfigEntry {
                content: r#"
name = "base"
[server]
port = 8080
"#
                .to_string(),
                format: Format::Toml,
            },
        );
        entries.insert(
            "override.toml".to_string(),
            ConfigEntry {
                content: r#"
name = "override"
[server]
port = 9090
"#
                .to_string(),
                format: Format::Toml,
            },
        );

        let cfg = merge_configs(
            &["base.toml".to_string(), "override.toml".to_string()],
            &entries,
        )
        .expect("merged config should parse");

        assert_eq!(cfg.get_string("name").unwrap(), "override");
        assert_eq!(cfg.get_int("server.port").unwrap(), 9090);
    }

    #[test]
    fn nacos_source_supports_single_and_multiple_data_id() {
        let single = NacosSource::new("127.0.0.1:8848", "").data_id("app.toml");
        assert_eq!(single.data_id, ["app.toml"]);
        assert!(single.validate().is_ok());

        let multi = NacosSource::new("127.0.0.1:8848", "").data_id(["app.toml", "app-extra.yaml"]);
        assert_eq!(multi.data_id.len(), 2);
        assert!(multi.validate().is_ok());

        let via_builder = NacosSource::new("127.0.0.1:8848", "")
            .data_id("app.toml")
            .data_id(vec!["app.toml", "shared.yaml"]);
        assert_eq!(via_builder.data_id.len(), 2);
        assert!(via_builder.validate().is_ok());
    }

    #[tokio::test]
    async fn reload_notifies_only_matching_data_id_channel() {
        let initial = parse_content("name = \"base\"", Format::Toml).expect("initial config");
        let (tx, _rx) = watch::channel(Arc::new(initial));
        let (app_tx, mut app_rx) = watch::channel(tx.borrow().clone());
        let (shared_tx, mut shared_rx) = watch::channel(tx.borrow().clone());
        let mut data_id_txs = HashMap::new();
        data_id_txs.insert("app.toml".to_string(), app_tx);
        data_id_txs.insert("shared.toml".to_string(), shared_tx);

        let mut entries = HashMap::new();
        entries.insert(
            "app.toml".to_string(),
            ConfigEntry {
                content: "name = \"app\"".to_string(),
                format: Format::Toml,
            },
        );
        entries.insert(
            "shared.toml".to_string(),
            ConfigEntry {
                content: "extra = true".to_string(),
                format: Format::Toml,
            },
        );

        let listener = spawn_test_reload_listener(
            tx.clone(),
            data_id_txs,
            vec!["app.toml".to_string(), "shared.toml".to_string()],
            entries,
        );

        let shared_before = shared_rx.borrow().clone();
        listener.notify(config_response("name = \"app-v2\"", "toml"));
        wait_for_reload().await;

        assert_eq!(
            app_rx.borrow_and_update().get_string("name").unwrap(),
            "app-v2"
        );
        assert!(Arc::ptr_eq(&shared_before, &shared_rx.borrow()));

        let shared_resp = ConfigResponse::new(
            "shared.toml".to_string(),
            DEFAULT_GROUP.to_string(),
            String::new(),
            "extra = false".to_string(),
            "toml".to_string(),
            "md5".to_string(),
        );
        listener.notify(shared_resp);
        wait_for_reload().await;

        assert!(!shared_rx.borrow_and_update().get_bool("extra").unwrap());
    }

    #[tokio::test]
    async fn reload_coalesces_pending_updates_into_single_merge() {
        let initial = parse_content("[server]\nport = 8080", Format::Toml).expect("initial");
        let (tx, _rx) = watch::channel(Arc::new(initial));

        let mut entries = HashMap::new();
        entries.insert(
            "app.toml".to_string(),
            ConfigEntry {
                content: "[server]\nport = 8080".to_string(),
                format: Format::Toml,
            },
        );
        entries.insert(
            "shared.toml".to_string(),
            ConfigEntry {
                content: "extra = true".to_string(),
                format: Format::Toml,
            },
        );

        let listener = spawn_test_reload_listener(
            tx.clone(),
            HashMap::new(),
            vec!["app.toml".to_string(), "shared.toml".to_string()],
            entries,
        );

        listener.notify(config_response("[server]\nport = 7070", "toml"));
        listener.notify(config_response("[server]\nport = 9090", "toml"));
        listener.notify(ConfigResponse::new(
            "shared.toml".to_string(),
            DEFAULT_GROUP.to_string(),
            String::new(),
            "extra = false".to_string(),
            "toml".to_string(),
            "md5".to_string(),
        ));
        wait_for_reload().await;

        assert_eq!(tx.borrow().get_int("server.port").unwrap(), 9090);
        assert!(!tx.borrow().get_bool("extra").unwrap());
    }

    #[test]
    fn reload_rolls_back_entries_when_merge_fails() {
        let initial = parse_content("[server]\nport = 8080", Format::Toml).expect("initial");
        let (tx, _rx) = watch::channel(Arc::new(initial));

        let mut entries = HashMap::new();
        entries.insert(
            "app.toml".to_string(),
            ConfigEntry {
                content: "[server]\nport = 8080".to_string(),
                format: Format::Toml,
            },
        );
        entries.insert(
            "shared.toml".to_string(),
            ConfigEntry {
                content: "extra = true".to_string(),
                format: Format::Toml,
            },
        );

        let listener = test_reload_listener(
            tx.clone(),
            HashMap::new(),
            vec!["app.toml".to_string(), "shared.toml".to_string()],
            entries,
        );

        let mut batch = HashMap::new();
        batch.insert(
            "app.toml".to_string(),
            ConfigEntry {
                content: "not = [valid".to_string(),
                format: Format::Toml,
            },
        );
        apply_batch_sync(&listener, batch);

        let entries = listener.entries.blocking_lock();
        assert_eq!(
            entries.get("app.toml").map(|e| e.content.as_str()),
            Some("[server]\nport = 8080")
        );
        assert_eq!(tx.borrow().get_int("server.port").unwrap(), 8080);
    }

    #[test]
    fn merge_configs_fails_on_missing_entry() {
        let mut entries = HashMap::new();
        entries.insert(
            "app.toml".to_string(),
            ConfigEntry {
                content: "name = \"app\"".to_string(),
                format: Format::Toml,
            },
        );

        let err = merge_configs(
            &["app.toml".to_string(), "missing.toml".to_string()],
            &entries,
        )
        .expect_err("missing entry should fail");

        assert!(matches!(
            err,
            ConfigError::MissingDataIdEntry { data_id } if data_id == "missing.toml"
        ));
    }

    #[test]
    fn nacos_source_rejects_region_id_without_access_keys() {
        let source = NacosSource::from_endpoint("acm.aliyun.com", "")
            .data_id("app.toml")
            .region_id("cn-hangzhou");

        assert!(matches!(
            source.validate(),
            Err(ConfigError::InvalidNacosSource { .. })
        ));
    }

    #[test]
    fn resolve_format_prefers_data_id_specific_format() {
        let source = NacosSource::new("127.0.0.1:8848", "")
            .data_id("app.cfg")
            .format(Format::Yaml)
            .format_for("app.cfg", Format::Ini);

        assert_eq!(
            resolve_format(&source, "app.cfg", None).expect("format"),
            Format::Ini
        );
    }

    #[test]
    fn parse_file_maps_io_error_to_read_file() {
        let path =
            std::env::temp_dir().join(format!("nacos-rs-missing-{}.yaml", std::process::id()));
        let err = parse_file(&path, None).expect_err("missing file should fail");
        assert!(matches!(err, ConfigError::ReadFile { .. }));
    }

    #[test]
    fn nacos_source_rejects_empty_or_duplicate_data_id() {
        let empty = NacosSource::new("127.0.0.1:8848", "");
        assert!(matches!(
            empty.validate(),
            Err(ConfigError::InvalidNacosSource { .. })
        ));

        let empty_list = NacosSource::new("127.0.0.1:8848", "").data_id(Vec::<String>::new());
        assert!(matches!(
            empty_list.validate(),
            Err(ConfigError::InvalidNacosSource { .. })
        ));

        let duplicate = NacosSource::new("127.0.0.1:8848", "").data_id(["app.toml", "app.toml"]);
        assert!(matches!(
            duplicate.validate(),
            Err(ConfigError::InvalidNacosSource { .. })
        ));
    }

    #[test]
    fn nacos_source_rejects_invalid_ha_controls() {
        let zero_timeout = NacosSource::new("127.0.0.1:8848", "")
            .data_id("app.toml")
            .startup_timeout(Duration::ZERO);
        assert!(matches!(
            zero_timeout.validate(),
            Err(ConfigError::InvalidNacosSource { .. })
        ));
    }

    #[tokio::test]
    async fn startup_timeout_maps_to_config_error() {
        let result = with_startup_timeout(
            Duration::from_millis(1),
            "test_operation",
            std::future::pending::<std::result::Result<(), nacos_sdk::api::error::Error>>(),
        )
        .await;

        assert!(matches!(
            result,
            Err(ConfigError::NacosStartupTimeout {
                operation: "test_operation",
                ..
            })
        ));
    }

    #[test]
    fn reload_listener_keeps_latest_pending_update_per_data_id() {
        let initial = parse_content("name = \"base\"", Format::Toml).expect("initial config");
        let (tx, _rx) = watch::channel(Arc::new(initial));
        let listener = ReloadListener {
            tx,
            data_id_txs: Arc::new(HashMap::new()),
            default_format: Some(Format::Toml),
            data_id_formats: HashMap::new(),
            data_id_order: vec!["app.toml".to_string()],
            entries: Arc::new(Mutex::new(HashMap::new())),
            pending_updates: Arc::new(StdMutex::new(HashMap::new())),
            update_notify: Arc::new(Notify::new()),
        };

        listener.notify(config_response("name = \"first\"", "toml"));
        listener.notify(config_response("name = \"second\"", "toml"));

        let pending = listener.pending_updates.lock().expect("pending updates");
        assert_eq!(pending.len(), 1);
        assert_eq!(
            pending.get("app.toml").map(|entry| entry.content.as_str()),
            Some("name = \"second\"")
        );
    }

    #[test]
    fn nacos_source_merge_order_matches_data_id() {
        let source = NacosSource::new("127.0.0.1:8848", "").data_id(["app.yaml", "shared.yaml"]);

        assert_eq!(source.merge_order(), ["app.yaml", "shared.yaml"]);
    }

    #[test]
    fn nacos_source_validates_required_address() {
        let source = NacosSource::new(Vec::<String>::new(), "").data_id("app.toml");

        assert!(matches!(
            source.validate(),
            Err(ConfigError::InvalidNacosSource { .. })
        ));
    }

    #[test]
    fn nacos_source_supports_single_and_multiple_server_addr() {
        let single_source = NacosSource::new("127.0.0.1:8848", "").data_id("app.toml");
        assert!(single_source.endpoint.is_none());
        assert_eq!(single_source.server_addr, ["127.0.0.1:8848"]);
        assert!(single_source.validate().is_ok());

        let multi_source =
            NacosSource::new(["127.0.0.1:8848", "192.168.0.1:8848"], "").data_id("app.toml");
        assert!(multi_source.endpoint.is_none());
        assert_eq!(multi_source.server_addr.len(), 2);
        assert!(multi_source.validate().is_ok());
    }

    #[test]
    fn nacos_source_supports_endpoint_mode() {
        let source = NacosSource::from_endpoint("acm.aliyun.com", "").data_id("app.toml");

        assert_eq!(source.endpoint.as_deref(), Some("acm.aliyun.com"));
        assert!(source.server_addr.is_empty());
        assert!(source.validate().is_ok());
    }

    #[test]
    fn nacos_source_rejects_partial_auth() {
        let source = NacosSource::new("127.0.0.1:8848", "")
            .data_id("app.toml")
            .auth("user", "");

        assert!(matches!(
            source.validate(),
            Err(ConfigError::InvalidNacosSource { .. })
        ));
    }

    #[test]
    fn nacos_source_supports_aliyun_auth() {
        let source = NacosSource::from_endpoint("acm.aliyun.com", "")
            .data_id("app.toml")
            .access_key("ak")
            .secret_key("sk")
            .region_id("cn-hangzhou");

        assert!(source.validate().is_ok());
    }

    #[test]
    fn nacos_source_rejects_partial_aliyun_auth() {
        let source = NacosSource::from_endpoint("acm.aliyun.com", "")
            .data_id("app.toml")
            .access_key("ak")
            .region_id("cn-hangzhou");

        assert!(matches!(
            source.validate(),
            Err(ConfigError::InvalidNacosSource { .. })
        ));
    }

    #[test]
    fn nacos_source_rejects_mixed_auth_modes() {
        let source = NacosSource::new("127.0.0.1:8848", "")
            .data_id("app.toml")
            .auth("user", "password")
            .access_key("ak")
            .secret_key("sk");

        assert!(matches!(
            source.validate(),
            Err(ConfigError::InvalidNacosSource { .. })
        ));
    }

    #[test]
    fn nacos_source_debug_includes_auth_values() {
        let source = NacosSource::new("127.0.0.1:8848", "")
            .data_id("app.toml")
            .auth("user", "http-secret")
            .access_key("aliyun-access")
            .secret_key("aliyun-secret");
        let debug = format!("{source:?}");

        assert!(debug.contains("http-secret"));
        assert!(debug.contains("aliyun-access"));
        assert!(debug.contains("aliyun-secret"));
    }
}
