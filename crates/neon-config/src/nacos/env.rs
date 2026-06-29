/// 读取运行环境的默认环境变量名。
pub const DEFAULT_ENV_VAR: &str = "APP_ENV";

/// 运行环境。
///
/// - [`Env::Local`]：本地开发环境，加载本地配置文件 / 内联配置。
/// - [`Env::Remote`]：其它环境（如 `dev` / `test` / `prod`），从 nacos 加载配置。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Env {
    Local,
    Remote(String),
}

impl Env {
    /// 根据环境名解析。空字符串或大小写不敏感的 `local` 视为本地环境。
    pub fn from_name(name: &str) -> Self {
        let name = name.trim();
        if name.is_empty() || name.eq_ignore_ascii_case("local") {
            Env::Local
        } else {
            Env::Remote(name.to_string())
        }
    }

    /// 从指定环境变量解析，变量缺失时回退为 [`Env::Local`]。
    pub fn from_var(var: &str) -> Self {
        match std::env::var(var) {
            Ok(value) => Self::from_name(&value),
            Err(_) => Env::Local,
        }
    }

    /// 是否为本地环境。
    pub fn is_local(&self) -> bool {
        matches!(self, Env::Local)
    }

    /// 环境名称（本地环境固定返回 `local`）。
    pub fn name(&self) -> &str {
        match self {
            Env::Local => "local",
            Env::Remote(name) => name,
        }
    }
}
