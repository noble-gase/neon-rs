//! 通过 HTTP Range 读取远程 ZIP：解析中央目录，按需拉取条目内容
//!
//! 打开流程概览：
//! 1. `HEAD` 获取远程文件大小；
//! 2. 读取文件尾部（最多 64 KiB）定位 EOCD；
//! 3. 必要时解析 ZIP64 Locator / ZIP64 EOCD；
//! 4. Range 拉取 Central Directory 并解析条目列表；
//! 5. 对单个条目再 Range 读取 Local Header 与压缩数据并解压

use std::io::{self, Read};
use std::sync::{Arc, OnceLock};

use anyhow::{Context, Result, bail, ensure};
use libflate::deflate::Decoder as DeflateDecoder;
use reqwest::blocking::Client;
use reqwest::header::{CONTENT_RANGE, RANGE};

/// EOCD 签名 `0x06054b50`（磁盘上小端字节序为 `50 4b 05 06`）
const EOCD_SIGNATURE: [u8; 4] = [0x50, 0x4b, 0x05, 0x06];
/// ZIP64 EOCD Locator 签名 `0x07064b50`
const ZIP64_LOCATOR_SIGNATURE: [u8; 4] = [0x50, 0x4b, 0x06, 0x07];
/// ZIP64 EOCD 签名 `0x06064b50`
const ZIP64_EOCD_SIGNATURE: [u8; 4] = [0x50, 0x4b, 0x06, 0x06];
/// Central Directory File Header 签名 `0x02014b50`
const CD_HEADER_SIGNATURE: u32 = 0x0201_4b50;
/// 32 位字段放不下时写入的 ZIP64 占位值 `0xFFFFFFFF`
const ZIP64_SENTINEL: u32 = 0xFFFF_FFFF;
/// 在文件尾部搜索 EOCD 的最大范围（ZIP 规范允许 comment 拖尾，通常不超过 64 KiB）
const TAIL_SEARCH_LIMIT: u64 = 64 * 1024;
/// Store：无压缩
const COMPRESSION_STORE: u16 = 0;
/// Deflate（ZIP 中为原始 deflate 流，非 zlib 包装）
const COMPRESSION_DEFLATE: u16 = 8;

static HTTP_CLIENT: OnceLock<Arc<Client>> = OnceLock::new();

/// 在首次 HTTP 请求前安装全局客户端（仅可调用一次）
pub fn set_http_client(client: Client) -> Result<()> {
    HTTP_CLIENT
        .set(Arc::new(client))
        .map_err(|_| anyhow::anyhow!("HTTP client already initialized"))
}

/// 默认 HTTP 客户端：跳过 TLS 证书校验，每主机较多空闲连接（适合大文件分片 Range）
fn build_default_client() -> Result<Client> {
    Client::builder()
        .danger_accept_invalid_certs(true)
        .pool_max_idle_per_host(1000)
        .build()
        .context("build HTTP client")
}

fn shared_http_client() -> Result<Arc<Client>> {
    if let Some(client) = HTTP_CLIENT.get() {
        return Ok(Arc::clone(client));
    }
    let default = Arc::new(build_default_client()?);
    Ok(match HTTP_CLIENT.set(Arc::clone(&default)) {
        Ok(()) => default,
        Err(installed) => installed,
    })
}

struct ArchiveInner {
    client: Arc<Client>,
    url: String,
}

/// 远程 ZIP 归档（中央目录已解析）
pub struct RemoteArchive {
    inner: Arc<ArchiveInner>,
    /// 远程 ZIP 总字节数（`Content-Length`）
    pub size: u64,
    /// 中央目录中的条目列表
    pub entries: Vec<ArchiveEntry>,
}

/// 中央目录里的一条文件记录
pub struct ArchiveEntry {
    /// 条目路径（相对路径，来自 Central Directory）
    pub name: String,
    /// 压缩后大小（字节），对应 CD 的 compressed size
    pub compressed_size: u64,
    /// 解压后大小（字节），对应 CD 的 uncompressed size
    pub uncompressed_size: u64,
    /// 压缩方式：`0` = Store，`8` = Deflate，其它见 ZIP 规范
    pub compression_method: u16,
    /// Local File Header 在 ZIP 内的起始偏移（相对文件头）
    local_header_offset: u64,
    inner: Arc<ArchiveInner>,
}

/// 条目正文流；`Drop` 时关闭底层 HTTP 响应体
pub struct EntryReader {
    body: Box<dyn Read + Send>,
}

impl Read for EntryReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.body.read(buf)
    }
}

impl RemoteArchive {
    /// 打开远程 ZIP 并解析中央目录
    ///
    /// ### EOCD（End of Central Directory）— 标准 ZIP
    ///
    /// | 偏移 | 长度 | 说明 |
    /// |------|------|------|
    /// | 0 | 4 | 签名 `0x06054b50` |
    /// | 4 | 2 | 当前磁盘编号 |
    /// | 6 | 2 | 中央目录起始磁盘编号 |
    /// | 8 | 2 | 本磁盘 CD 条目数 |
    /// | 10 | 2 | CD 总条目数 |
    /// | 12 | 4 | 中央目录大小（本实现用于 `cd_size`） |
    /// | 16 | 4 | 中央目录偏移（本实现用于 `cd_offset`） |
    /// | 20 | 2 | ZIP 注释长度 |
    ///
    /// ### ZIP64 EOCD（当 `cd_size` / `cd_offset` 为 `0xFFFFFFFF` 时）
    ///
    /// | 偏移 | 长度 | 说明 |
    /// |------|------|------|
    /// | 0 | 4 | 签名 `0x06064b50` |
    /// | 4 | 8 | 本记录大小 |
    /// | 12 | 2 | 创建版本 |
    /// | 14 | 2 | 解压所需版本 |
    /// | 16 | 4 | 当前磁盘编号 |
    /// | 20 | 4 | 中央目录起始磁盘编号 |
    /// | 24 | 8 | 本磁盘 CD 条目数 |
    /// | 32 | 8 | CD 总条目数 |
    /// | 40 | 8 | 中央目录大小 |
    /// | 48 | 8 | 中央目录偏移 |
    pub fn open(url: impl Into<String>) -> Result<Self> {
        let url = url.into();
        let client = shared_http_client()?;
        // Step 1: HEAD 获取远程文件大小
        let size = fetch_content_length(&client, &url)?;
        let inner = Arc::new(ArchiveInner { client, url });

        // Step 2: 读取尾部，定位 EOCD（目录在文件末尾）
        let tail_len = size.min(TAIL_SEARCH_LIMIT);
        let tail_start = size.saturating_sub(tail_len);
        let tail_end = size.checked_sub(1).context("remote zip is empty")?;
        let tail = inner.fetch_range(tail_start, tail_end)?;

        // Step 3–4: 解析 EOCD / ZIP64，得到中央目录范围
        let (cd_size, cd_offset) = locate_central_directory(&inner, &tail, size)?;

        // Step 5: Range 拉取并解析 Central Directory
        let cd_data = if cd_size == 0 {
            Vec::new()
        } else {
            let cd_end = cd_offset
                .checked_add(cd_size)
                .and_then(|n| n.checked_sub(1))
                .context("central directory range overflow")?;
            inner.fetch_range(cd_offset, cd_end)?
        };
        let entries = parse_central_directory(&inner, &cd_data)?;

        Ok(Self { inner, size, entries })
    }

    pub fn url(&self) -> &str {
        &self.inner.url
    }
}

impl ArchiveEntry {
    /// 打开条目正文，返回可读流（Store 或 Deflate）
    pub fn open(&self) -> Result<EntryReader> {
        // 读取 Local File Header（固定 30 字节 + 预留文件名/extra）
        let header = self
            .inner
            .fetch_range(self.local_header_offset, self.local_header_offset + 30 + 256)?;
        ensure!(header.len() >= 30, "local file header too short");

        let name_len = u16::from_le_bytes([header[26], header[27]]) as u64;
        let extra_len = u16::from_le_bytes([header[28], header[29]]) as u64;
        // 数据区起始 = Local Header 偏移 + 30 + 文件名 + extra
        let data_offset = self.local_header_offset + 30 + name_len + extra_len;

        let body: Box<dyn Read + Send> = if self.compressed_size == 0 {
            Box::new(std::io::empty())
        } else {
            let data_end = data_offset
                .checked_add(self.compressed_size)
                .and_then(|n| n.checked_sub(1))
                .context("entry data range overflow")?;
            self.inner.fetch_range_stream(data_offset, data_end)?
        };

        match self.compression_method {
            COMPRESSION_STORE => Ok(EntryReader { body }),
            COMPRESSION_DEFLATE => Ok(EntryReader {
                body: Box::new(DeflateDecoder::new(body)),
            }),
            method => bail!("unsupported compression method: {method}"),
        }
    }
}

/// 获取远程对象大小：优先 HEAD 的 `Content-Length`，否则用 `Range: bytes=0-0` 解析 `Content-Range`
fn fetch_content_length(client: &Client, url: &str) -> Result<u64> {
    let head = client.head(url).send().context("HEAD request")?;
    ensure!(head.status().is_success(), "HEAD failed: {}", head.status());
    if let Some(len) = head.content_length()
        && len > 0
    {
        return Ok(len);
    }

    let probe = client
        .get(url)
        .header(RANGE, "bytes=0-0")
        .send()
        .context("probe Content-Range")?;
    ensure!(probe.status().is_success(), "probe range failed: {}", probe.status());
    if let Some(total) = probe
        .headers()
        .get(CONTENT_RANGE)
        .and_then(|v| v.to_str().ok())
        .and_then(parse_content_range_total)
    {
        ensure!(total > 0, "remote zip size is zero");
        return Ok(total);
    }

    let len = probe
        .content_length()
        .filter(|&n| n > 0)
        .context("missing Content-Length and Content-Range total")?;
    Ok(len)
}

/// 解析 `Content-Range` 总长度，例如 `bytes 0-0/740472096`
fn parse_content_range_total(value: &str) -> Option<u64> {
    let (_, total) = value.trim().split_once('/')?;
    total.parse().ok()
}

/// 在尾部缓冲中定位 EOCD，返回中央目录的 `(size, offset)`
fn locate_central_directory(inner: &ArchiveInner, tail: &[u8], size: u64) -> Result<(u64, u64)> {
    let eocd_idx = find_signature_tail(tail, &EOCD_SIGNATURE).context("EOCD not found")?;
    let eocd = &tail[eocd_idx..];
    ensure!(eocd.len() >= 22, "EOCD record too short");

    // EOCD +12: 中央目录大小；+16: 中央目录偏移
    let mut cd_size = u64::from(u32::from_le_bytes(eocd[12..16].try_into()?));
    let mut cd_offset = u64::from(u32::from_le_bytes(eocd[16..20].try_into()?));

    // `0xFFFFFFFF` 表示需走 ZIP64 EOCD
    if cd_size == u64::from(ZIP64_SENTINEL) || cd_offset == u64::from(ZIP64_SENTINEL) {
        let loc_idx = find_signature_tail(tail, &ZIP64_LOCATOR_SIGNATURE).context("ZIP64 locator not found")?;
        let loc = &tail[loc_idx..];
        ensure!(loc.len() >= 16, "ZIP64 locator too short");

        // Locator +8: ZIP64 EOCD 在文件中的偏移
        let zip64_eocd_offset = u64::from_le_bytes(loc[8..16].try_into()?);
        let zip64_eocd = inner.fetch_range(zip64_eocd_offset, zip64_eocd_offset + 55)?;
        ensure!(zip64_eocd.starts_with(&ZIP64_EOCD_SIGNATURE), "invalid ZIP64 EOCD signature");
        ensure!(zip64_eocd.len() >= 56, "ZIP64 EOCD too short");
        // ZIP64 EOCD +40 / +48: 真实的 cd_size、cd_offset
        cd_size = u64::from_le_bytes(zip64_eocd[40..48].try_into()?);
        cd_offset = u64::from_le_bytes(zip64_eocd[48..56].try_into()?);
    }

    ensure!(cd_offset + cd_size <= size, "central directory out of bounds");
    Ok((cd_size, cd_offset))
}

/// 解析 Central Directory 二进制块，构建条目列表
fn parse_central_directory(inner: &Arc<ArchiveInner>, data: &[u8]) -> Result<Vec<ArchiveEntry>> {
    let mut entries = Vec::new();
    let mut i = 0usize;
    while i < data.len() {
        // 每条记录以签名 `0x02014b50` 开头
        if i + 4 > data.len() {
            break;
        }
        if u32::from_le_bytes(data[i..i + 4].try_into()?) != CD_HEADER_SIGNATURE {
            break;
        }
        ensure!(i + 46 <= data.len(), "central directory header truncated");

        let compression_method = u16::from_le_bytes(data[i + 10..i + 12].try_into()?);
        // +20 压缩大小；+24 未压缩大小（≥4 GiB 时为 `0xFFFFFFFF`，见 ZIP64 extra）
        let mut compressed_size = u64::from(u32::from_le_bytes(data[i + 20..i + 24].try_into()?));
        let mut uncompressed_size = u64::from(u32::from_le_bytes(data[i + 24..i + 28].try_into()?));
        let name_len = usize::from(u16::from_le_bytes(data[i + 28..i + 30].try_into()?));
        let extra_len = usize::from(u16::from_le_bytes(data[i + 30..i + 32].try_into()?));
        let comment_len = usize::from(u16::from_le_bytes(data[i + 32..i + 34].try_into()?));
        // +42 Local File Header 偏移
        let mut local_header_offset = u64::from(u32::from_le_bytes(data[i + 42..i + 46].try_into()?));

        let name_end = i + 46 + name_len;
        let extra_end = name_end + extra_len;
        ensure!(extra_end <= data.len(), "central directory entry out of range");

        let name = std::str::from_utf8(&data[i + 46..name_end])
            .context("invalid UTF-8 file name in central directory")?
            .to_owned();
        let extra = &data[name_end..extra_end];

        // 占位值 `0xFFFFFFFF` 时从 extra 中解析真实 64 位字段
        if compressed_size == u64::from(ZIP64_SENTINEL)
            || uncompressed_size == u64::from(ZIP64_SENTINEL)
            || local_header_offset == u64::from(ZIP64_SENTINEL)
        {
            parse_zip64_extra(extra, &mut uncompressed_size, &mut compressed_size, &mut local_header_offset)?;
        }

        // 固定头 46 字节 + 文件名 + extra + 注释
        i = extra_end + comment_len;

        entries.push(ArchiveEntry {
            name,
            compressed_size,
            uncompressed_size,
            compression_method,
            local_header_offset,
            inner: Arc::clone(inner),
        });
    }
    Ok(entries)
}

/// 解析 ZIP64 Extended Information（extra 头 ID `0x0001`）
///
/// 数据区按顺序存放（仅当对应 CD 字段为 `0xFFFFFFFF` 时才出现）：
/// 未压缩大小 → 压缩大小 → Local Header 偏移
fn parse_zip64_extra(extra: &[u8], uncompressed_size: &mut u64, compressed_size: &mut u64, local_header_offset: &mut u64) -> Result<()> {
    let mut j = 0usize;
    while j + 4 <= extra.len() {
        // [HeaderID: u16][DataSize: u16][Data...]
        let header_id = u16::from_le_bytes(extra[j..j + 2].try_into()?);
        let data_size = usize::from(u16::from_le_bytes(extra[j + 2..j + 4].try_into()?));
        j += 4;
        ensure!(j + data_size <= extra.len(), "ZIP64 extra field truncated");

        if header_id == 0x0001 {
            let mut k = j;
            if *uncompressed_size == u64::from(ZIP64_SENTINEL) {
                ensure!(k + 8 <= extra.len(), "ZIP64 extra missing uncompressed size");
                *uncompressed_size = u64::from_le_bytes(extra[k..k + 8].try_into()?);
                k += 8;
            }
            if *compressed_size == u64::from(ZIP64_SENTINEL) {
                ensure!(k + 8 <= extra.len(), "ZIP64 extra missing compressed size");
                *compressed_size = u64::from_le_bytes(extra[k..k + 8].try_into()?);
                k += 8;
            }
            if *local_header_offset == u64::from(ZIP64_SENTINEL) {
                ensure!(k + 8 <= extra.len(), "ZIP64 extra missing local header offset");
                *local_header_offset = u64::from_le_bytes(extra[k..k + 8].try_into()?);
            }
        }
        j += data_size;
    }
    Ok(())
}

/// 在 `haystack` 中自后向前查找 `signature` 的首次出现位置
fn find_signature_tail(haystack: &[u8], signature: &[u8]) -> Option<usize> {
    haystack.windows(signature.len()).rposition(|window| window == signature)
}

impl ArchiveInner {
    /// `Range: bytes=start-end`（含端点），将响应体读入内存
    fn fetch_range(&self, start: u64, end: u64) -> Result<Vec<u8>> {
        let response = self
            .client
            .get(&self.url)
            .header(RANGE, format!("bytes={start}-{end}"))
            .send()
            .with_context(|| format!("range request bytes={start}-{end}"))?;
        ensure!(response.status().is_success(), "range request failed: {}", response.status());
        Ok(response.bytes().context("read range body")?.to_vec())
    }

    /// `Range: bytes=start-end`，返回响应体流（用于 Store / Deflate，避免整段进内存）
    fn fetch_range_stream(&self, start: u64, end: u64) -> Result<Box<dyn Read + Send>> {
        let response = self
            .client
            .get(&self.url)
            .header(RANGE, format!("bytes={start}-{end}"))
            .send()
            .with_context(|| format!("range stream bytes={start}-{end}"))?;
        ensure!(response.status().is_success(), "range stream failed: {}", response.status());
        Ok(Box::new(response))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_eocd_in_tail() {
        let mut tail = vec![0u8; 20];
        tail.extend_from_slice(&[0x50, 0x4b, 0x05, 0x06, 0, 0, 0, 0]);
        assert_eq!(find_signature_tail(&tail, &EOCD_SIGNATURE), Some(20));
    }

    #[test]
    fn parse_zip64_extra_field() {
        let mut uncompressed = u64::from(ZIP64_SENTINEL);
        let mut compressed = u64::from(ZIP64_SENTINEL);
        let mut offset = u64::from(ZIP64_SENTINEL);
        let extra = {
            let mut buf = Vec::new();
            buf.extend_from_slice(&1u16.to_le_bytes());
            buf.extend_from_slice(&24u16.to_le_bytes());
            buf.extend_from_slice(&100u64.to_le_bytes());
            buf.extend_from_slice(&50u64.to_le_bytes());
            buf.extend_from_slice(&999u64.to_le_bytes());
            buf
        };
        parse_zip64_extra(&extra, &mut uncompressed, &mut compressed, &mut offset).unwrap();
        assert_eq!(uncompressed, 100);
        assert_eq!(compressed, 50);
        assert_eq!(offset, 999);
    }
}
