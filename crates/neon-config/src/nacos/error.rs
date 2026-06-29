use std::path::PathBuf;

/// 配置汇聚统一错误类型。
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// 读取本地配置文件失败。
    #[error("读取本地配置文件失败 {path}: {source}")]
    ReadFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// 无法从文件名 / data-id 推断配置格式，且未显式指定。
    #[error("无法推断配置格式（来源: {hint}），请显式指定 format")]
    UnknownFormat { hint: String },

    /// 解析配置内容失败（格式错误或字段不匹配）。
    #[error("解析配置内容失败: {0}")]
    Parse(#[from] config::ConfigError),

    /// 处于非 local 环境，但没有提供 nacos 配置源。
    #[error("当前为远程环境 `{env}`，但未配置 nacos 数据源")]
    MissingNacosSource { env: String },

    /// 处于 local 环境，但没有提供本地配置源。
    #[error("当前为 local 环境，但未配置本地文件或内联配置")]
    MissingLocalSource,

    /// nacos 配置源参数不合法。
    #[error("nacos 配置源参数不合法: {reason}")]
    InvalidNacosSource { reason: String },

    /// nacos 客户端相关错误。
    #[error("nacos 客户端错误: {0}")]
    Nacos(#[from] nacos_sdk::api::error::Error),
}

/// 配置汇聚结果别名。
pub type Result<T> = std::result::Result<T, ConfigError>;
