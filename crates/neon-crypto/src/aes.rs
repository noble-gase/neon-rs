//! AES 对称加密（CBC / ECB / GCM）。

use aes::{Aes128, Aes192, Aes256};
use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{AesGcm, Nonce};
use anyhow::anyhow;
use cipher::{
    Array, BlockCipherDecrypt, BlockCipherEncrypt, BlockModeDecrypt, BlockModeEncrypt, BlockSizeUser, KeyIvInit, consts::U16,
};
use cipher::typenum::{U12, U13, U14, U15, U16 as TagU16};

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
            _ => Err(anyhow!("invalid AES key size: {}", key.len())),
        }
    }
}

type Aes128CbcEnc = cbc::Encryptor<Aes128>;
type Aes128CbcDec = cbc::Decryptor<Aes128>;
type Aes192CbcEnc = cbc::Encryptor<Aes192>;
type Aes192CbcDec = cbc::Decryptor<Aes192>;
type Aes256CbcEnc = cbc::Encryptor<Aes256>;
type Aes256CbcDec = cbc::Decryptor<Aes256>;

/// AES-CBC 加密（PKCS#7 padding，默认 block_size 16，可自定义）
pub fn aes_encrypt_cbc(
    key: impl AsRef<[u8]>, iv: impl AsRef<[u8]>, data: impl AsRef<[u8]>, padding_size: Option<u8>,
) -> anyhow::Result<CipherText> {
    let key = key.as_ref();
    let iv = iv.as_ref();
    let data = data.as_ref();
    if iv.len() != BLOCK_SIZE {
        return Err(anyhow!("IV length must equal block size"));
    }

    let pad_size = padding_size.map(|v| v as usize).unwrap_or(BLOCK_SIZE);

    let mut buf = data.to_vec();
    pkcs7_padding(&mut buf, pad_size);
    if !buf.len().is_multiple_of(BLOCK_SIZE) {
        return Err(anyhow!("input not full blocks"));
    }

    let out = match key.len() {
        16 => encrypt_cbc_blocks(Aes128CbcEnc::new_from_slices(key, iv), &mut buf),
        24 => encrypt_cbc_blocks(Aes192CbcEnc::new_from_slices(key, iv), &mut buf),
        32 => encrypt_cbc_blocks(Aes256CbcEnc::new_from_slices(key, iv), &mut buf),
        _ => return Err(anyhow!("invalid AES key size: {}", key.len())),
    }?;
    Ok(CipherText { bytes: out, tag_size: 0 })
}

fn encrypt_cbc_blocks<E>(enc: Result<E, cipher::InvalidLength>, buf: &mut [u8]) -> anyhow::Result<Vec<u8>>
where
    E: BlockModeEncrypt + BlockSizeUser<BlockSize = U16>,
{
    let mut enc = enc.map_err(anyhow::Error::from)?;
    for block in buf.chunks_mut(BLOCK_SIZE) {
        let block = Array::<u8, U16>::slice_as_mut_array(block).ok_or_else(|| anyhow!("invalid block size"))?;
        enc.encrypt_block(block);
    }
    Ok(buf.to_vec())
}

/// AES-CBC 解密（PKCS#7 unpadding）
pub fn aes_decrypt_cbc(key: impl AsRef<[u8]>, iv: impl AsRef<[u8]>, data: impl AsRef<[u8]>) -> anyhow::Result<Vec<u8>> {
    let key = key.as_ref();
    let iv = iv.as_ref();
    let data = data.as_ref();
    if iv.len() != BLOCK_SIZE {
        return Err(anyhow!("IV length must equal block size"));
    }

    if !data.len().is_multiple_of(BLOCK_SIZE) {
        return Err(anyhow!("input not full blocks"));
    }

    let mut out = data.to_vec();
    match key.len() {
        16 => decrypt_cbc_blocks(Aes128CbcDec::new_from_slices(key, iv), &mut out)?,
        24 => decrypt_cbc_blocks(Aes192CbcDec::new_from_slices(key, iv), &mut out)?,
        32 => decrypt_cbc_blocks(Aes256CbcDec::new_from_slices(key, iv), &mut out)?,
        _ => return Err(anyhow!("invalid AES key size: {}", key.len())),
    };
    pkcs7_unpadding(&mut out)?;
    Ok(out)
}

fn decrypt_cbc_blocks<D>(dec: Result<D, cipher::InvalidLength>, buf: &mut [u8]) -> anyhow::Result<()>
where
    D: BlockModeDecrypt + BlockSizeUser<BlockSize = U16>,
{
    let mut dec = dec.map_err(anyhow::Error::from)?;
    for block in buf.chunks_mut(BLOCK_SIZE) {
        let block = Array::<u8, U16>::slice_as_mut_array(block).ok_or_else(|| anyhow!("invalid block size"))?;
        dec.decrypt_block(block);
    }
    Ok(())
}

/// AES-ECB 加密（PKCS#7）
pub fn aes_encrypt_ecb(key: impl AsRef<[u8]>, data: impl AsRef<[u8]>, padding_size: Option<u8>) -> anyhow::Result<CipherText> {
    let key = key.as_ref();
    let data = data.as_ref();
    let cipher = AesKey::new(key)?;
    let pad_size = padding_size.map(|v| v as usize).unwrap_or(BLOCK_SIZE);

    let mut buf = data.to_vec();
    pkcs7_padding(&mut buf, pad_size);
    if !buf.len().is_multiple_of(BLOCK_SIZE) {
        return Err(anyhow!("input not full blocks"));
    }

    for block in buf.chunks_mut(BLOCK_SIZE) {
        let block = Array::<u8, U16>::slice_as_mut_array(block).ok_or_else(|| anyhow!("invalid block size"))?;
        match &cipher {
            AesKey::K128(c) => c.encrypt_block(block),
            AesKey::K192(c) => c.encrypt_block(block),
            AesKey::K256(c) => c.encrypt_block(block),
        }
    }
    Ok(CipherText { bytes: buf, tag_size: 0 })
}

/// AES-ECB 解密
pub fn aes_decrypt_ecb(key: impl AsRef<[u8]>, data: impl AsRef<[u8]>) -> anyhow::Result<Vec<u8>> {
    let key = key.as_ref();
    let data = data.as_ref();
    let cipher = AesKey::new(key)?;
    if !data.len().is_multiple_of(BLOCK_SIZE) {
        return Err(anyhow!("input not full blocks"));
    }

    let mut out = data.to_vec();
    for block in out.chunks_mut(BLOCK_SIZE) {
        let block = Array::<u8, U16>::slice_as_mut_array(block).ok_or_else(|| anyhow!("invalid block size"))?;
        match &cipher {
            AesKey::K128(c) => c.decrypt_block(block),
            AesKey::K192(c) => c.decrypt_block(block),
            AesKey::K256(c) => c.decrypt_block(block),
        }
    }
    pkcs7_unpadding(&mut out)?;
    Ok(out)
}

/// AES-GCM 选项
///
/// 字段为 `0` 表示使用默认值（nonce=12，tag=16）
/// 当 `tag_size` 与 `nonce_size` 同时非零时，与 Go 版一致，优先采用 `tag_size` 解析规则，
/// 但 `nonce_size` 仍会生效（不再被忽略）
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

/// AES-GCM 加密默认 NonceSize=12，TagSize=16
pub fn aes_encrypt_gcm(
    key: impl AsRef<[u8]>, nonce: impl AsRef<[u8]>, data: impl AsRef<[u8]>, aad: impl AsRef<[u8]>, opt: Option<&GcmOption>,
) -> anyhow::Result<CipherText> {
    let key = key.as_ref();
    let nonce = nonce.as_ref();
    let data = data.as_ref();
    let aad = aad.as_ref();
    let (nonce_size, tag_size) = resolve_gcm_sizes(opt)?;
    if nonce.len() != nonce_size {
        return Err(anyhow!("incorrect nonce length given to GCM"));
    }

    let ct = gcm_seal(key, nonce, data, aad, nonce_size, tag_size)?;
    Ok(CipherText { bytes: ct, tag_size })
}

/// AES-GCM 解密
pub fn aes_decrypt_gcm(
    key: impl AsRef<[u8]>, nonce: impl AsRef<[u8]>, data: impl AsRef<[u8]>, aad: impl AsRef<[u8]>, opt: Option<&GcmOption>,
) -> anyhow::Result<Vec<u8>> {
    let key = key.as_ref();
    let nonce = nonce.as_ref();
    let data = data.as_ref();
    let aad = aad.as_ref();
    let (nonce_size, tag_size) = resolve_gcm_sizes(opt)?;
    if nonce.len() != nonce_size {
        return Err(anyhow!("incorrect nonce length given to GCM"));
    }

    gcm_open(key, nonce, data, aad, nonce_size, tag_size)
}

fn resolve_gcm_sizes(opt: Option<&GcmOption>) -> anyhow::Result<(usize, usize)> {
    let (nonce_size, tag_size) = match opt {
        Some(o) if o.tag_size != 0 && o.nonce_size == 0 => (GCM_NONCE_SIZE, o.tag_size),
        Some(o) if o.nonce_size != 0 && o.tag_size == 0 => (o.nonce_size, 16),
        Some(o) if o.tag_size != 0 && o.nonce_size != 0 => (o.nonce_size, o.tag_size),
        _ => (GCM_NONCE_SIZE, 16),
    };
    if !(12..=16).contains(&nonce_size) {
        return Err(anyhow!("invalid GCM nonce size"));
    }
    if !(12..=16).contains(&tag_size) {
        return Err(anyhow!("invalid GCM tag size"));
    }
    Ok((nonce_size, tag_size))
}

fn gcm_seal(key: &[u8], nonce: &[u8], data: &[u8], aad: &[u8], nonce_size: usize, tag_size: usize) -> anyhow::Result<Vec<u8>> {
    gcm_op(key, nonce, data, aad, nonce_size, tag_size, true)
}

fn gcm_open(key: &[u8], nonce: &[u8], data: &[u8], aad: &[u8], nonce_size: usize, tag_size: usize) -> anyhow::Result<Vec<u8>> {
    gcm_op(key, nonce, data, aad, nonce_size, tag_size, false)
}

fn gcm_op(key: &[u8], nonce: &[u8], data: &[u8], aad: &[u8], nonce_size: usize, tag_size: usize, seal: bool) -> anyhow::Result<Vec<u8>> {
    macro_rules! gcm_run {
        ($aes:ty, $ns:ty, $ts:ty) => {{
            let cipher = AesGcm::<$aes, $ns, $ts>::new_from_slice(key).map_err(anyhow::Error::from)?;
            let n = Nonce::<$ns>::try_from(nonce).map_err(|_| anyhow!("incorrect nonce length given to GCM"))?;
            let payload = Payload { msg: data, aad };
            if seal {
                cipher.encrypt(&n, payload).map_err(|e| anyhow!(e.to_string()))
            } else {
                cipher.decrypt(&n, payload).map_err(|e| anyhow!(e.to_string()))
            }
        }};
    }

    macro_rules! gcm_for_tag {
        ($aes:ty, $ns:ty) => {
            match tag_size {
                12 => gcm_run!($aes, $ns, U12),
                13 => gcm_run!($aes, $ns, U13),
                14 => gcm_run!($aes, $ns, U14),
                15 => gcm_run!($aes, $ns, U15),
                16 => gcm_run!($aes, $ns, TagU16),
                _ => Err(anyhow!("unsupported tag size")),
            }
        };
    }

    macro_rules! gcm_for_key {
        ($aes:ty) => {
            match nonce_size {
                12 => gcm_for_tag!($aes, U12),
                13 => gcm_for_tag!($aes, U13),
                14 => gcm_for_tag!($aes, U14),
                15 => gcm_for_tag!($aes, U15),
                16 => gcm_for_tag!($aes, TagU16),
                _ => Err(anyhow!("unsupported nonce size")),
            }
        };
    }

    match key.len() {
        16 => gcm_for_key!(Aes128),
        24 => gcm_for_key!(Aes192),
        32 => gcm_for_key!(Aes256),
        _ => Err(anyhow!("invalid AES key size: {}", key.len())),
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
        let pt = aes_decrypt_cbc(key, iv, ct.bytes()).unwrap();
        assert_eq!(pt, DATA.as_bytes());

        let ct2 = aes_encrypt_cbc(key, iv, DATA.as_bytes(), Some(32)).unwrap();
        assert_eq!(ct2.to_string(), "vjemH/hxbwNh+WXhkKseCu2GrM4O6bnaaKv59wgkRSE=");
        let pt2 = aes_decrypt_cbc(key, iv, ct2.bytes()).unwrap();
        assert_eq!(pt2, DATA.as_bytes());
    }

    #[test]
    fn ecb_roundtrip() {
        let key = KEY.as_bytes();
        let ct = aes_encrypt_ecb(key, DATA.as_bytes(), None).unwrap();
        assert_eq!(ct.to_string(), "oYDjdGHY8lK1/sJo750Waw==");
        let pt = aes_decrypt_ecb(key, ct.bytes()).unwrap();
        assert_eq!(pt, DATA.as_bytes());

        let ct2 = aes_encrypt_ecb(key, DATA.as_bytes(), Some(32)).unwrap();
        assert_eq!(ct2.to_string(), "u0iDWHM8JMnRyJNCiCzKJNib2cOjUrx2FqMjmg3ZTZA=");
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
