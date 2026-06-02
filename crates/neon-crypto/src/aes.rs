//! AES 对称加密（CBC / ECB / GCM）

use aes::{Aes128, Aes192, Aes256};
use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{AesGcm, Nonce, TagSize};
use anyhow::anyhow;
use cipher::array::ArraySize;
use cipher::block_padding::Pkcs7;
use cipher::typenum::{U12, U13, U14, U15, U16 as TagU16};
use cipher::{Array, BlockCipherDecrypt, BlockCipherEncrypt, BlockModeDecrypt, BlockModeEncrypt, BlockSizeUser, KeyIvInit, consts::U16};

use crate::{CipherText, pkcs7_padding, pkcs7_unpadding};

const BLOCK_SIZE: usize = 16;
const GCM_NONCE_SIZE: usize = 12;

enum AesKey {
    K128(Aes128),
    K192(Aes192),
    K256(Aes256),
}

impl AesKey {
    fn new(key: &[u8]) -> anyhow::Result<Self> {
        match key.len() {
            16 => Aes128::new_from_slice(key).map(AesKey::K128).map_err(anyhow::Error::from),
            24 => Aes192::new_from_slice(key).map(AesKey::K192).map_err(anyhow::Error::from),
            32 => Aes256::new_from_slice(key).map(AesKey::K256).map_err(anyhow::Error::from),
            n => Err(anyhow!("invalid AES key size: {}", n)),
        }
    }
}

type Aes128CbcEnc = cbc::Encryptor<Aes128>;
type Aes128CbcDec = cbc::Decryptor<Aes128>;
type Aes192CbcEnc = cbc::Encryptor<Aes192>;
type Aes192CbcDec = cbc::Decryptor<Aes192>;
type Aes256CbcEnc = cbc::Encryptor<Aes256>;
type Aes256CbcDec = cbc::Decryptor<Aes256>;

// --------- CBC ---------

/// AES-CBC 加密（PKCS#7 padding，默认 padding 边界为 block_size=16，可自定义为更大的倍数）
///
/// `padding_size`:
/// - `None` 或 `Some(16)`：走 cipher 内置的 PKCS#7 批处理路径，效率最高
/// - `Some(n)`：`n` 必须为 16 的整数倍，按 `n` 字节对齐做 PKCS#7 padding
pub fn aes_encrypt_cbc(
    key: impl AsRef<[u8]>, iv: impl AsRef<[u8]>, data: impl AsRef<[u8]>, padding_size: Option<usize>,
) -> anyhow::Result<CipherText> {
    let key = key.as_ref();
    let iv = iv.as_ref();
    let data = data.as_ref();
    if iv.len() != BLOCK_SIZE {
        return Err(anyhow!("IV length must equal block size"));
    }

    let bytes = match padding_size {
        None | Some(BLOCK_SIZE) => cbc_encrypt_pkcs7(key, iv, data)?,
        Some(pad) => cbc_encrypt_padded(key, iv, data, pad)?,
    };
    Ok(CipherText { bytes, tag_size: 0 })
}

fn cbc_encrypt_pkcs7(key: &[u8], iv: &[u8], data: &[u8]) -> anyhow::Result<Vec<u8>> {
    match key.len() {
        16 => Ok(Aes128CbcEnc::new_from_slices(key, iv)?.encrypt_padded_vec::<Pkcs7>(data)),
        24 => Ok(Aes192CbcEnc::new_from_slices(key, iv)?.encrypt_padded_vec::<Pkcs7>(data)),
        32 => Ok(Aes256CbcEnc::new_from_slices(key, iv)?.encrypt_padded_vec::<Pkcs7>(data)),
        n => Err(anyhow!("invalid AES key size: {}", n)),
    }
}

fn cbc_encrypt_padded(key: &[u8], iv: &[u8], data: &[u8], pad_size: usize) -> anyhow::Result<Vec<u8>> {
    if pad_size == 0 || !pad_size.is_multiple_of(BLOCK_SIZE) {
        return Err(anyhow!("padding_size must be a positive multiple of {BLOCK_SIZE}"));
    }
    let mut buf = data.to_vec();
    pkcs7_padding(&mut buf, pad_size)?;
    match key.len() {
        16 => cbc_encrypt_blocks_in_place(Aes128CbcEnc::new_from_slices(key, iv)?, &mut buf),
        24 => cbc_encrypt_blocks_in_place(Aes192CbcEnc::new_from_slices(key, iv)?, &mut buf),
        32 => cbc_encrypt_blocks_in_place(Aes256CbcEnc::new_from_slices(key, iv)?, &mut buf),
        n => return Err(anyhow!("invalid AES key size: {}", n)),
    }
    Ok(buf)
}

fn cbc_encrypt_blocks_in_place<E>(mut enc: E, buf: &mut [u8])
where
    E: BlockModeEncrypt + BlockSizeUser<BlockSize = U16>,
{
    let (blocks, _) = Array::<u8, U16>::slice_as_chunks_mut(buf);
    enc.encrypt_blocks(blocks);
}

/// AES-CBC 解密（PKCS#7 unpadding）
///
/// `padding_size` 必须与加密时使用的值一致：
/// - `None` 或 `Some(16)`：走 cipher 内置的 PKCS#7 批处理路径
/// - `Some(n)`：`n` 必须为 16 的正整倍数，严格验证 pad 在 `1..=n` 范围内
pub fn aes_decrypt_cbc(
    key: impl AsRef<[u8]>, iv: impl AsRef<[u8]>, data: impl AsRef<[u8]>, padding_size: Option<usize>,
) -> anyhow::Result<Vec<u8>> {
    let key = key.as_ref();
    let iv = iv.as_ref();
    let data = data.as_ref();
    if iv.len() != BLOCK_SIZE {
        return Err(anyhow!("IV length must equal block size"));
    }
    if !data.len().is_multiple_of(BLOCK_SIZE) {
        return Err(anyhow!("input not full blocks"));
    }

    match padding_size {
        None | Some(BLOCK_SIZE) => cbc_decrypt_pkcs7(key, iv, data),
        Some(pad) => cbc_decrypt_padded(key, iv, data, pad),
    }
}

fn cbc_decrypt_pkcs7(key: &[u8], iv: &[u8], data: &[u8]) -> anyhow::Result<Vec<u8>> {
    match key.len() {
        16 => Aes128CbcDec::new_from_slices(key, iv)?
            .decrypt_padded_vec::<Pkcs7>(data)
            .map_err(|e| anyhow!("cbc decrypt: {e}")),
        24 => Aes192CbcDec::new_from_slices(key, iv)?
            .decrypt_padded_vec::<Pkcs7>(data)
            .map_err(|e| anyhow!("cbc decrypt: {e}")),
        32 => Aes256CbcDec::new_from_slices(key, iv)?
            .decrypt_padded_vec::<Pkcs7>(data)
            .map_err(|e| anyhow!("cbc decrypt: {e}")),
        n => Err(anyhow!("invalid AES key size: {}", n)),
    }
}

fn cbc_decrypt_padded(key: &[u8], iv: &[u8], data: &[u8], pad_size: usize) -> anyhow::Result<Vec<u8>> {
    if pad_size == 0 || !pad_size.is_multiple_of(BLOCK_SIZE) {
        return Err(anyhow!("padding_size must be a positive multiple of {BLOCK_SIZE}"));
    }
    if !data.len().is_multiple_of(pad_size) {
        return Err(anyhow!("input length is not a multiple of padding_size"));
    }
    let mut out = data.to_vec();
    match key.len() {
        16 => cbc_decrypt_blocks_in_place(Aes128CbcDec::new_from_slices(key, iv)?, &mut out),
        24 => cbc_decrypt_blocks_in_place(Aes192CbcDec::new_from_slices(key, iv)?, &mut out),
        32 => cbc_decrypt_blocks_in_place(Aes256CbcDec::new_from_slices(key, iv)?, &mut out),
        n => return Err(anyhow!("invalid AES key size: {}", n)),
    }
    pkcs7_unpadding(&mut out, pad_size)?;
    Ok(out)
}

fn cbc_decrypt_blocks_in_place<D>(mut dec: D, buf: &mut [u8])
where
    D: BlockModeDecrypt + BlockSizeUser<BlockSize = U16>,
{
    let (blocks, _) = Array::<u8, U16>::slice_as_chunks_mut(buf);
    dec.decrypt_blocks(blocks);
}

// --------- ECB ---------

/// AES-ECB 加密（PKCS#7）
pub fn aes_encrypt_ecb(key: impl AsRef<[u8]>, data: impl AsRef<[u8]>, padding_size: Option<usize>) -> anyhow::Result<CipherText> {
    let cipher = AesKey::new(key.as_ref())?;
    let pad_size = padding_size.unwrap_or(BLOCK_SIZE);
    if pad_size == 0 || !pad_size.is_multiple_of(BLOCK_SIZE) {
        return Err(anyhow!("padding_size must be a positive multiple of {BLOCK_SIZE}"));
    }

    let mut buf = data.as_ref().to_vec();
    pkcs7_padding(&mut buf, pad_size)?;

    let (blocks, _) = Array::<u8, U16>::slice_as_chunks_mut(&mut buf);
    match &cipher {
        AesKey::K128(c) => c.encrypt_blocks(blocks),
        AesKey::K192(c) => c.encrypt_blocks(blocks),
        AesKey::K256(c) => c.encrypt_blocks(blocks),
    }
    Ok(CipherText { bytes: buf, tag_size: 0 })
}

/// AES-ECB 解密
///
/// `padding_size` 必须与加密时使用的值一致（`None` 等价于 `Some(16)`）
pub fn aes_decrypt_ecb(key: impl AsRef<[u8]>, data: impl AsRef<[u8]>, padding_size: Option<usize>) -> anyhow::Result<Vec<u8>> {
    let data = data.as_ref();
    let pad_size = padding_size.unwrap_or(BLOCK_SIZE);
    if pad_size == 0 || !pad_size.is_multiple_of(BLOCK_SIZE) {
        return Err(anyhow!("padding_size must be a positive multiple of {BLOCK_SIZE}"));
    }
    if !data.len().is_multiple_of(pad_size) {
        return Err(anyhow!("input length is not a multiple of padding_size"));
    }
    let cipher = AesKey::new(key.as_ref())?;

    let mut out = data.to_vec();
    let (blocks, _) = Array::<u8, U16>::slice_as_chunks_mut(&mut out);
    match &cipher {
        AesKey::K128(c) => c.decrypt_blocks(blocks),
        AesKey::K192(c) => c.decrypt_blocks(blocks),
        AesKey::K256(c) => c.decrypt_blocks(blocks),
    }
    pkcs7_unpadding(&mut out, pad_size)?;
    Ok(out)
}

// --------- GCM ---------

/// AES-GCM 选项
///
/// 字段为 `0` 表示使用默认值（nonce=12，tag=16）
#[derive(Debug, Clone, Copy)]
pub struct GcmOption {
    /// 非默认 tag 大小（12..=16），`0` 表示 16
    pub tag_size: usize,
    /// 非默认 nonce 大小（12..=16），`0` 表示 12
    pub nonce_size: usize,
}

impl Default for GcmOption {
    fn default() -> Self {
        Self {
            nonce_size: GCM_NONCE_SIZE,
            tag_size: 16,
        }
    }
}

/// AES-GCM 加密；默认 NonceSize=12，TagSize=16
pub fn aes_encrypt_gcm(
    key: impl AsRef<[u8]>, nonce: impl AsRef<[u8]>, data: impl AsRef<[u8]>, aad: impl AsRef<[u8]>, opt: Option<&GcmOption>,
) -> anyhow::Result<CipherText> {
    let (nonce_size, tag_size) = resolve_gcm_sizes(opt)?;
    let call = GcmCall {
        key: key.as_ref(),
        nonce: nonce.as_ref(),
        data: data.as_ref(),
        aad: aad.as_ref(),
        seal: true,
    };
    let ct = gcm_op(call, nonce_size, tag_size)?;
    Ok(CipherText { bytes: ct, tag_size })
}

/// AES-GCM 解密
pub fn aes_decrypt_gcm(
    key: impl AsRef<[u8]>, nonce: impl AsRef<[u8]>, data: impl AsRef<[u8]>, aad: impl AsRef<[u8]>, opt: Option<&GcmOption>,
) -> anyhow::Result<Vec<u8>> {
    let (nonce_size, tag_size) = resolve_gcm_sizes(opt)?;
    let call = GcmCall {
        key: key.as_ref(),
        nonce: nonce.as_ref(),
        data: data.as_ref(),
        aad: aad.as_ref(),
        seal: false,
    };
    gcm_op(call, nonce_size, tag_size)
}

fn resolve_gcm_sizes(opt: Option<&GcmOption>) -> anyhow::Result<(usize, usize)> {
    let (nonce_size, tag_size) = match opt {
        Some(o) => (
            if o.nonce_size == 0 { GCM_NONCE_SIZE } else { o.nonce_size },
            if o.tag_size == 0 { 16 } else { o.tag_size },
        ),
        None => (GCM_NONCE_SIZE, 16),
    };
    if !(12..=16).contains(&nonce_size) {
        return Err(anyhow!("invalid GCM nonce size"));
    }
    if !(12..=16).contains(&tag_size) {
        return Err(anyhow!("invalid GCM tag size"));
    }
    Ok((nonce_size, tag_size))
}

struct GcmCall<'a> {
    key: &'a [u8],
    nonce: &'a [u8],
    data: &'a [u8],
    aad: &'a [u8],
    seal: bool,
}

fn gcm_op(call: GcmCall<'_>, nonce_size: usize, tag_size: usize) -> anyhow::Result<Vec<u8>> {
    match call.key.len() {
        16 => gcm_dispatch_nonce::<Aes128>(call, nonce_size, tag_size),
        24 => gcm_dispatch_nonce::<Aes192>(call, nonce_size, tag_size),
        32 => gcm_dispatch_nonce::<Aes256>(call, nonce_size, tag_size),
        n => Err(anyhow!("invalid AES key size: {}", n)),
    }
}

fn gcm_dispatch_nonce<A>(call: GcmCall<'_>, nonce_size: usize, tag_size: usize) -> anyhow::Result<Vec<u8>>
where
    A: BlockCipherEncrypt + BlockSizeUser<BlockSize = U16> + KeyInit,
{
    match nonce_size {
        12 => gcm_dispatch_tag::<A, U12>(call, tag_size),
        13 => gcm_dispatch_tag::<A, U13>(call, tag_size),
        14 => gcm_dispatch_tag::<A, U14>(call, tag_size),
        15 => gcm_dispatch_tag::<A, U15>(call, tag_size),
        16 => gcm_dispatch_tag::<A, TagU16>(call, tag_size),
        _ => Err(anyhow!("unsupported nonce size")),
    }
}

fn gcm_dispatch_tag<A, N>(call: GcmCall<'_>, tag_size: usize) -> anyhow::Result<Vec<u8>>
where
    A: BlockCipherEncrypt + BlockSizeUser<BlockSize = U16> + KeyInit,
    N: ArraySize,
{
    match tag_size {
        12 => gcm_run::<A, N, U12>(call),
        13 => gcm_run::<A, N, U13>(call),
        14 => gcm_run::<A, N, U14>(call),
        15 => gcm_run::<A, N, U15>(call),
        16 => gcm_run::<A, N, TagU16>(call),
        _ => Err(anyhow!("unsupported tag size")),
    }
}

fn gcm_run<A, N, T>(call: GcmCall<'_>) -> anyhow::Result<Vec<u8>>
where
    A: BlockCipherEncrypt + BlockSizeUser<BlockSize = U16> + KeyInit,
    N: ArraySize,
    T: TagSize,
{
    let cipher = AesGcm::<A, N, T>::new_from_slice(call.key).map_err(anyhow::Error::from)?;
    let nonce = Nonce::<N>::try_from(call.nonce).map_err(|_| anyhow!("incorrect nonce length given to GCM"))?;
    let payload = Payload {
        msg: call.data,
        aad: call.aad,
    };
    if call.seal {
        cipher.encrypt(&nonce, payload).map_err(|e| anyhow!(e.to_string()))
    } else {
        cipher.decrypt(&nonce, payload).map_err(|e| anyhow!(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine, prelude::BASE64_STANDARD as B64};

    const KEY: &str = "AES256Key-32Characters1234567890";
    const DATA: &str = "ILoveNobleGase";

    #[test]
    fn cbc_roundtrip() {
        let key = KEY.as_bytes();
        let iv = &KEY.as_bytes()[..16];
        let ct = aes_encrypt_cbc(key, iv, DATA.as_bytes(), None).unwrap();
        assert_eq!(ct.to_string(), "WDq8s1qdHCML8YLhfdmGRw==");
        let pt = aes_decrypt_cbc(key, iv, ct.bytes(), None).unwrap();
        assert_eq!(pt, DATA.as_bytes());

        let ct2 = aes_encrypt_cbc(key, iv, DATA.as_bytes(), Some(32)).unwrap();
        assert_eq!(ct2.to_string(), "vjemH/hxbwNh+WXhkKseCu2GrM4O6bnaaKv59wgkRSE=");
        let pt2 = aes_decrypt_cbc(key, iv, ct2.bytes(), Some(32)).unwrap();
        assert_eq!(pt2, DATA.as_bytes());
    }

    #[test]
    fn cbc_invalid_padding_size() {
        let key = KEY.as_bytes();
        let iv = &KEY.as_bytes()[..16];
        let err = aes_encrypt_cbc(key, iv, DATA.as_bytes(), Some(17)).unwrap_err();
        assert!(err.to_string().contains("padding_size"));
    }

    #[test]
    fn ecb_roundtrip() {
        let key = KEY.as_bytes();
        let ct = aes_encrypt_ecb(key, DATA.as_bytes(), None).unwrap();
        assert_eq!(ct.to_string(), "oYDjdGHY8lK1/sJo750Waw==");
        let pt = aes_decrypt_ecb(key, ct.bytes(), None).unwrap();
        assert_eq!(pt, DATA.as_bytes());

        let ct2 = aes_encrypt_ecb(key, DATA.as_bytes(), Some(32)).unwrap();
        assert_eq!(ct2.to_string(), "u0iDWHM8JMnRyJNCiCzKJNib2cOjUrx2FqMjmg3ZTZA=");
        let pt2 = aes_decrypt_ecb(key, ct2.bytes(), Some(32)).unwrap();
        assert_eq!(pt2, DATA.as_bytes());
    }

    #[test]
    fn gcm_roundtrip() {
        let key = KEY.as_bytes();
        let nonce = &KEY.as_bytes()[..12];
        let aad = b"IIInsomnia";
        let ct = aes_encrypt_gcm(key, nonce, DATA.as_bytes(), aad, Some(&GcmOption::default())).unwrap();
        assert_eq!(ct.to_string(), "qciumnROL4U9F0klEKhzE/DngAy/clYUsZGfcafh");
        assert_eq!(B64.encode(ct.data()), "qciumnROL4U9F0klEKg=");
        assert_eq!(B64.encode(ct.tag()), "cxPw54AMv3JWFLGRn3Gn4Q==");

        let pt = aes_decrypt_gcm(key, nonce, ct.bytes(), aad, None).unwrap();
        assert_eq!(pt, DATA.as_bytes());
    }

    #[test]
    fn gcm_tag_sizes_roundtrip() {
        let key = KEY.as_bytes();
        let nonce = &KEY.as_bytes()[..12];
        let aad = b"IIInsomnia";

        for tag_size in 12..=15 {
            let opt = GcmOption { tag_size, nonce_size: 0 };
            let ct = aes_encrypt_gcm(key, nonce, DATA.as_bytes(), aad, Some(&opt)).unwrap();
            assert_eq!(ct.tag().len(), tag_size);
            let pt = aes_decrypt_gcm(key, nonce, ct.bytes(), aad, Some(&opt)).unwrap();
            assert_eq!(pt, DATA.as_bytes());
        }
    }

    #[test]
    fn gcm_nonce_sizes_roundtrip() {
        let key = KEY.as_bytes();
        let aad = b"IIInsomnia";

        for nonce_size in 12..=16 {
            let nonce = &key[..nonce_size];
            let opt = GcmOption { nonce_size, tag_size: 0 };
            let ct = aes_encrypt_gcm(key, nonce, DATA.as_bytes(), aad, Some(&opt)).unwrap();
            let pt = aes_decrypt_gcm(key, nonce, ct.bytes(), aad, Some(&opt)).unwrap();
            assert_eq!(pt, DATA.as_bytes());
        }
    }

    #[test]
    fn gcm_invalid_nonce_size() {
        let key = KEY.as_bytes();
        let nonce = &KEY.as_bytes()[..12];
        let opt = GcmOption {
            nonce_size: 11,
            tag_size: 0,
        };
        let err = aes_encrypt_gcm(key, nonce, DATA.as_bytes(), b"", Some(&opt)).unwrap_err();
        assert!(err.to_string().contains("invalid GCM nonce size"));
    }
}
