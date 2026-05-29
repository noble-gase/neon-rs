//! 对称/非对称加密与哈希工具（按 feature 启用子模块）

#[cfg(feature = "aes")]
pub mod aes;
#[cfg(feature = "des")]
pub mod des;
#[cfg(feature = "hash")]
pub mod hash;
#[cfg(feature = "rsa")]
pub mod rsa;

#[cfg(any(feature = "aes", feature = "des"))]
use base64::{Engine, prelude::BASE64_STANDARD as B64};

/// 加密结果（封装 ciphertext + 可选 GCM tag 长度）
#[cfg(any(feature = "aes", feature = "des"))]
#[derive(Debug, Clone)]
pub struct CipherText {
    bytes: Vec<u8>,
    tag_size: usize,
}

#[cfg(any(feature = "aes", feature = "des"))]
impl CipherText {
    /// 原始密文字节
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// GCM 真实数据部分（不含 tag）；非 GCM 模式时与 [`bytes`](Self::bytes) 相同
    pub fn data(&self) -> &[u8] {
        &self.bytes[..self.bytes.len().saturating_sub(self.tag_size)]
    }

    /// GCM tag 部分；非 GCM 模式（`tag_size == 0`）时返回空切片
    pub fn tag(&self) -> &[u8] {
        &self.bytes[self.bytes.len().saturating_sub(self.tag_size)..]
    }

    /// 消费并返回原始密文字节
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    /// 密文是否为空
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

#[cfg(any(feature = "aes", feature = "des"))]
impl AsRef<[u8]> for CipherText {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

#[cfg(any(feature = "aes", feature = "des"))]
/// 以标准 Base64 显示完整密文字节（含 GCM tag）
impl std::fmt::Display for CipherText {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&B64.encode(&self.bytes))
    }
}

#[cfg(any(feature = "aes", feature = "des"))]
fn pkcs7_padding(data: &mut Vec<u8>, block_size: usize) {
    let mut pad = block_size - data.len() % block_size;
    if pad == 0 {
        pad = block_size
    }
    data.extend(std::iter::repeat_n(pad as u8, pad));
}

#[cfg(any(feature = "aes", feature = "des"))]
fn pkcs7_unpadding(data: &mut Vec<u8>) -> anyhow::Result<()> {
    let len = data.len();
    if len == 0 {
        return Err(anyhow::anyhow!("empty data for unpadding"));
    }

    let pad = data[len - 1] as usize;
    if pad == 0 || pad > len {
        return Err(anyhow::anyhow!("invalid padding"));
    }
    if !data[len - pad..].iter().all(|&b| b == pad as u8) {
        return Err(anyhow::anyhow!("invalid padding"));
    }
    data.truncate(len - pad);
    Ok(())
}

#[cfg(all(test, any(feature = "aes", feature = "des")))]
mod tests {
    use super::*;

    #[test]
    fn pkcs7_invalid_padding_bytes() {
        let mut data = b"hello\x03\x02\x03".to_vec();
        assert!(pkcs7_unpadding(&mut data).is_err());
    }
}
