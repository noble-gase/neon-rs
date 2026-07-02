use std::path::Path;

/// 受支持的配置格式。底层借助 `config` crate 完成解析
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Toml,
    Yaml,
    Json,
    Json5,
    Ini,
    Ron,
}

impl Format {
    /// 根据文件扩展名推断格式，例如 `yaml` / `yml` / `toml`
    ///
    /// `.conf` 等内容格式不确定的扩展名不参与推断（可能是 nginx / HOCON / INI 等），
    /// 会返回 `None` 并由上层报 `UnknownFormat`，提示显式指定 format
    pub fn from_extension(ext: &str) -> Option<Self> {
        match ext.trim().to_ascii_lowercase().as_str() {
            "yaml" | "yml" => Some(Self::Yaml),
            "toml" => Some(Self::Toml),
            "json" => Some(Self::Json),
            "json5" => Some(Self::Json5),
            "ini" | "properties" => Some(Self::Ini),
            "ron" => Some(Self::Ron),
            _ => None,
        }
    }

    /// 根据文件路径（取扩展名）推断格式
    pub fn from_path(path: &Path) -> Option<Self> {
        path.extension()
            .and_then(|ext| ext.to_str())
            .and_then(Self::from_extension)
    }

    /// 根据 nacos 返回的 `content_type` 推断格式
    ///
    /// nacos 的取值通常为 `yaml` / `json` / `properties` / `text` / `html` / `xml` 等
    pub fn from_content_type(content_type: &str) -> Option<Self> {
        match content_type.trim().to_ascii_lowercase().as_str() {
            "yaml" => Some(Self::Yaml),
            "toml" => Some(Self::Toml),
            "json" => Some(Self::Json),
            "json5" => Some(Self::Json5),
            "properties" => Some(Self::Ini),
            "ron" => Some(Self::Ron),
            _ => None,
        }
    }
}

impl From<Format> for config::FileFormat {
    fn from(format: Format) -> Self {
        match format {
            Format::Yaml => config::FileFormat::Yaml,
            Format::Toml => config::FileFormat::Toml,
            Format::Json => config::FileFormat::Json,
            Format::Json5 => config::FileFormat::Json5,
            Format::Ini => config::FileFormat::Ini,
            Format::Ron => config::FileFormat::Ron,
        }
    }
}
