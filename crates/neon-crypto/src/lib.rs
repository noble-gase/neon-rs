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

/// PKCS#7 填充
#[cfg(any(feature = "aes", feature = "des"))]
fn pkcs7_padding(data: &mut Vec<u8>, block_size: usize) -> anyhow::Result<()> {
    if !(1..=255).contains(&block_size) {
        return Err(anyhow::anyhow!("invalid block size"));
    }

    let pad = block_size - data.len() % block_size;
    data.extend(std::iter::repeat_n(pad as u8, pad));
    Ok(())
}

/// PKCS#7 去填充
///
/// pad 合法性与 pad 字节一致性采用累积式判定（best-effort 常数时间）：
/// 恒定扫描最后一整块、不短路，避免比较耗时泄漏首个不匹配字节的位置
/// （CBC 无认证场景下的 padding oracle 放大器）；所有 pad 相关错误统一
/// 返回同一消息。数据长度非机密信息，长度校验仍可提前返回
#[cfg(any(feature = "aes", feature = "des"))]
fn pkcs7_unpadding(data: &mut Vec<u8>, block_size: usize) -> anyhow::Result<()> {
    if !(1..=255).contains(&block_size) {
        return Err(anyhow::anyhow!("invalid block size"));
    }

    let len = data.len();
    if len == 0 || !len.is_multiple_of(block_size) {
        return Err(anyhow::anyhow!("invalid data length"));
    }

    let last = data[len - 1];
    let pad = last as usize;
    // pad 合法性先记入累积标志，不提前返回
    let mut bad = u8::from(pad == 0) | u8::from(pad > block_size);
    let pad_len = pad.clamp(1, block_size);
    // 恒定扫描最后一整块：耗时仅与 block_size 相关，与 pad 值/匹配位置无关
    let start = len - block_size;
    for (i, &b) in data[start..].iter().enumerate() {
        // 位置 i 属于 pad 区域（i >= block_size - pad_len）时 mask 为 0xFF，否则 0x00
        let in_pad = (u8::from(i + pad_len >= block_size)).wrapping_neg();
        bad |= (b ^ last) & in_pad;
    }
    if bad != 0 {
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
        assert!(pkcs7_unpadding(&mut data, 8).is_err());
    }

    #[test]
    fn pkcs7_unpadding_rejects_zero_pad_byte() {
        // 末字节为 0x00：pad=0 非法，必须拒绝
        let mut data = vec![1u8; 15];
        data.push(0);
        assert!(pkcs7_unpadding(&mut data, 16).is_err());
    }

    #[test]
    fn pkcs7_padding_roundtrip() {
        for block_size in [8usize, 16, 32] {
            for data_len in 0..=64 {
                let mut buf: Vec<u8> = (0..data_len as u8).collect();
                let orig = buf.clone();
                pkcs7_padding(&mut buf, block_size).unwrap();
                assert!(buf.len().is_multiple_of(block_size));
                pkcs7_unpadding(&mut buf, block_size).unwrap();
                assert_eq!(buf, orig);
            }
        }
    }

    #[test]
    fn pkcs7_invalid_block_size() {
        let mut data = vec![1u8; 16];
        assert!(pkcs7_padding(&mut data, 0).is_err());
        assert!(pkcs7_padding(&mut data, 256).is_err());
        assert!(pkcs7_unpadding(&mut data, 0).is_err());
        assert!(pkcs7_unpadding(&mut data, 256).is_err());
    }

    #[test]
    fn pkcs7_unpadding_rejects_pad_exceeding_block_size() {
        // 伪造数据“末字节 = 0x12 = 18”：strict 模式下 block_size=16 不接受 pad>16
        let mut data = vec![0x12u8; 32];
        assert!(pkcs7_unpadding(&mut data, 16).is_err());
        // 但在 block_size=32 下 pad=18 是合法的，去填充后应剩 32-18=14 个 0x12
        let mut data = vec![0x12u8; 32];
        pkcs7_unpadding(&mut data, 32).unwrap();
        assert_eq!(data, vec![0x12u8; 14]);
    }
}
