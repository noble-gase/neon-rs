use aes::cipher::{generic_array::GenericArray, BlockDecrypt, BlockDecryptMut, BlockEncrypt, BlockEncryptMut, KeyInit, KeyIvInit};
use aes::{Aes128, Aes192, Aes256};
use aes_gcm::aead::{Aead, Payload};
use aes_gcm::{AesGcm, Nonce};

use crate::crypto::{pkcs7_padding, pkcs7_unpadding, CipherText};
use crate::error::{HeError, HeResult};

const BLOCK_SIZE: usize = 16;

// ----------------- 内部 enum 派发：根据 key 长度选 Aes128/192/256 -----------------

enum AesKey {
    K128(Aes128),
    K192(Aes192),
    K256(Aes256),
}

impl AesKey {
    fn new(key: &[u8]) -> HeResult<Self> {
        match key.len() {
            16 => Aes128::new_from_slice(key)
                .map(AesKey::K128)
                .map_err(|e| HeError::crypto(e.to_string())),
            24 => Aes192::new_from_slice(key)
                .map(AesKey::K192)
                .map_err(|e| HeError::crypto(e.to_string())),
            32 => Aes256::new_from_slice(key)
                .map(AesKey::K256)
                .map_err(|e| HeError::crypto(e.to_string())),
            _ => Err(HeError::crypto(format!("invalid AES key size: {}", key.len()))),
        }
    }
}

// ----------------- CBC -----------------

type Aes128CbcEnc = cbc::Encryptor<Aes128>;
type Aes128CbcDec = cbc::Decryptor<Aes128>;
type Aes192CbcEnc = cbc::Encryptor<Aes192>;
type Aes192CbcDec = cbc::Decryptor<Aes192>;
type Aes256CbcEnc = cbc::Encryptor<Aes256>;
type Aes256CbcDec = cbc::Decryptor<Aes256>;

/// AES-CBC 加密（PKCS#7 padding，默认 block_size 16，可自定义）。
pub fn aes_encrypt_cbc(key: &[u8], iv: &[u8], data: &[u8], padding_size: Option<u8>) -> HeResult<CipherText> {
    if iv.len() != BLOCK_SIZE {
        return Err(HeError::crypto("IV length must equal block size"));
    }

    let pad_size = padding_size.map(|v| v as usize).unwrap_or(BLOCK_SIZE);

    let mut buf = data.to_vec();
    pkcs7_padding(&mut buf, pad_size);
    if buf.len() % BLOCK_SIZE != 0 {
        return Err(HeError::crypto("input not full blocks"));
    }

    let out = match key.len() {
        16 => encrypt_cbc_blocks(Aes128CbcEnc::new_from_slices(key, iv), &buf),
        24 => encrypt_cbc_blocks(Aes192CbcEnc::new_from_slices(key, iv), &buf),
        32 => encrypt_cbc_blocks(Aes256CbcEnc::new_from_slices(key, iv), &buf),
        _ => return Err(HeError::crypto(format!("invalid AES key size: {}", key.len()))),
    }?;
    Ok(CipherText { bytes: out, tag_size: 0 })
}

fn encrypt_cbc_blocks<E>(enc: Result<E, cipher::InvalidLength>, padded: &[u8]) -> HeResult<Vec<u8>>
where
    E: BlockEncryptMut + cipher::BlockSizeUser<BlockSize = cipher::consts::U16>,
{
    let mut enc = enc.map_err(|e| HeError::crypto(e.to_string()))?;

    let mut out = vec![0u8; padded.len()];
    for (in_block, out_block) in padded.chunks(BLOCK_SIZE).zip(out.chunks_mut(BLOCK_SIZE)) {
        let block: &GenericArray<u8, cipher::consts::U16> = GenericArray::from_slice(in_block);
        let out_arr: &mut GenericArray<u8, cipher::consts::U16> = GenericArray::from_mut_slice(out_block);

        let mut tmp = *block;
        enc.encrypt_block_mut(&mut tmp);
        out_arr.copy_from_slice(&tmp);
    }
    Ok(out)
}

/// AES-CBC 解密（PKCS#7 unpadding）。
pub fn aes_decrypt_cbc(key: &[u8], iv: &[u8], data: &[u8]) -> HeResult<Vec<u8>> {
    if iv.len() != BLOCK_SIZE {
        return Err(HeError::crypto("IV length must equal block size"));
    }

    if data.len() % BLOCK_SIZE != 0 {
        return Err(HeError::crypto("input not full blocks"));
    }

    let mut out = match key.len() {
        16 => decrypt_cbc_blocks(Aes128CbcDec::new_from_slices(key, iv), data),
        24 => decrypt_cbc_blocks(Aes192CbcDec::new_from_slices(key, iv), data),
        32 => decrypt_cbc_blocks(Aes256CbcDec::new_from_slices(key, iv), data),
        _ => Err(HeError::crypto(format!("invalid AES key size: {}", key.len()))),
    }?;
    pkcs7_unpadding(&mut out)?;
    Ok(out)
}

fn decrypt_cbc_blocks<D>(dec: Result<D, cipher::InvalidLength>, cipher: &[u8]) -> HeResult<Vec<u8>>
where
    D: BlockDecryptMut + cipher::BlockSizeUser<BlockSize = cipher::consts::U16>,
{
    let mut dec = dec.map_err(|e| HeError::crypto(e.to_string()))?;

    let mut out = vec![0u8; cipher.len()];
    for (in_block, out_block) in cipher.chunks(BLOCK_SIZE).zip(out.chunks_mut(BLOCK_SIZE)) {
        let block: &GenericArray<u8, cipher::consts::U16> = GenericArray::from_slice(in_block);
        let out_arr: &mut GenericArray<u8, cipher::consts::U16> = GenericArray::from_mut_slice(out_block);

        let mut tmp = *block;
        dec.decrypt_block_mut(&mut tmp);
        out_arr.copy_from_slice(&tmp);
    }
    Ok(out)
}

// ----------------- ECB -----------------

/// AES-ECB 加密（PKCS#7）。
pub fn aes_encrypt_ecb(key: &[u8], data: &[u8], padding_size: Option<u8>) -> HeResult<CipherText> {
    let cipher = AesKey::new(key)?;
    let pad_size = padding_size.map(|v| v as usize).unwrap_or(BLOCK_SIZE);

    let mut buf = data.to_vec();
    pkcs7_padding(&mut buf, pad_size);
    if buf.len() % BLOCK_SIZE != 0 {
        return Err(HeError::crypto("input not full blocks"));
    }

    let mut out = vec![0u8; buf.len()];
    for (in_block, out_block) in buf.chunks(BLOCK_SIZE).zip(out.chunks_mut(BLOCK_SIZE)) {
        let block: &GenericArray<u8, cipher::consts::U16> = GenericArray::from_slice(in_block);
        let out_arr: &mut GenericArray<u8, cipher::consts::U16> = GenericArray::from_mut_slice(out_block);

        let mut tmp = *block;
        match &cipher {
            AesKey::K128(c) => c.encrypt_block(&mut tmp),
            AesKey::K192(c) => c.encrypt_block(&mut tmp),
            AesKey::K256(c) => c.encrypt_block(&mut tmp),
        }
        out_arr.copy_from_slice(&tmp);
    }
    Ok(CipherText { bytes: out, tag_size: 0 })
}

/// AES-ECB 解密。
pub fn aes_decrypt_ecb(key: &[u8], data: &[u8]) -> HeResult<Vec<u8>> {
    let cipher = AesKey::new(key)?;
    if data.len() % BLOCK_SIZE != 0 {
        return Err(HeError::crypto("input not full blocks"));
    }

    let mut out = vec![0u8; data.len()];
    for (in_block, out_block) in data.chunks(BLOCK_SIZE).zip(out.chunks_mut(BLOCK_SIZE)) {
        let block: &GenericArray<u8, cipher::consts::U16> = GenericArray::from_slice(in_block);
        let out_arr: &mut GenericArray<u8, cipher::consts::U16> = GenericArray::from_mut_slice(out_block);

        let mut tmp = *block;
        match &cipher {
            AesKey::K128(c) => c.decrypt_block(&mut tmp),
            AesKey::K192(c) => c.decrypt_block(&mut tmp),
            AesKey::K256(c) => c.decrypt_block(&mut tmp),
        }
        out_arr.copy_from_slice(&tmp);
    }
    pkcs7_unpadding(&mut out)?;
    Ok(out)
}

// ----------------- GCM -----------------

/// AES-GCM 选项（二选一）。
#[derive(Debug, Clone, Copy, Default)]
pub struct GcmOption {
    /// 非默认 tag 大小（12..=16）。
    pub tag_size: usize,
    /// 非默认 nonce 大小。
    pub nonce_size: usize,
}

/// AES-GCM 加密。默认 NonceSize=12，TagSize=16。
pub fn aes_encrypt_gcm(key: &[u8], nonce: &[u8], data: &[u8], aad: &[u8], opt: Option<&GcmOption>) -> HeResult<CipherText> {
    let (nonce_size, tag_size) = resolve_gcm_sizes(opt);
    if nonce.len() != nonce_size {
        return Err(HeError::crypto("incorrect nonce length given to GCM"));
    }

    // tag_size 校验
    if !(12..=16).contains(&tag_size) {
        return Err(HeError::crypto("invalid GCM tag size"));
    }

    let ct = gcm_seal(key, nonce, data, aad, tag_size)?;
    Ok(CipherText { bytes: ct, tag_size })
}

/// AES-GCM 解密。
pub fn aes_decrypt_gcm(key: &[u8], nonce: &[u8], data: &[u8], aad: &[u8], opt: Option<&GcmOption>) -> HeResult<Vec<u8>> {
    let (nonce_size, tag_size) = resolve_gcm_sizes(opt);
    if nonce.len() != nonce_size {
        return Err(HeError::crypto("incorrect nonce length given to GCM"));
    }

    // tag_size 校验
    if !(12..=16).contains(&tag_size) {
        return Err(HeError::crypto("invalid GCM tag size"));
    }

    gcm_open(key, nonce, data, aad, tag_size)
}

fn resolve_gcm_sizes(opt: Option<&GcmOption>) -> (usize, usize) {
    match opt {
        Some(o) if o.tag_size != 0 && o.nonce_size == 0 => (12, o.tag_size),
        Some(o) if o.nonce_size != 0 && o.tag_size == 0 => (o.nonce_size, 16),
        Some(o) if o.tag_size != 0 && o.nonce_size != 0 => {
            // Go 版优先 TagSize
            (12, o.tag_size)
        }
        _ => (12, 16),
    }
}

fn gcm_seal(key: &[u8], nonce: &[u8], data: &[u8], aad: &[u8], tag_size: usize) -> HeResult<Vec<u8>> {
    // aes-gcm crate 的标准 tag 是 16；自定义 tag_size 需要使用 AesGcm<Aes, NonceSize, TagSize>，
    // 这里仅支持 12..=16 范围，通过 TagSize 的 typenum 类型擦除。
    use aes_gcm::aead::generic_array::typenum::{U12, U13, U14, U15, U16};
    macro_rules! seal_with {
        ($ts:ty) => {
            match key.len() {
                16 => {
                    let cipher = AesGcm::<Aes128, U12, $ts>::new_from_slice(key).map_err(|e| HeError::crypto(e.to_string()))?;
                    let n = Nonce::<U12>::from_slice(nonce);
                    cipher
                        .encrypt(n, Payload { msg: data, aad })
                        .map_err(|e| HeError::crypto(e.to_string()))?
                }
                24 => {
                    let cipher = AesGcm::<Aes192, U12, $ts>::new_from_slice(key).map_err(|e| HeError::crypto(e.to_string()))?;
                    let n = Nonce::<U12>::from_slice(nonce);
                    cipher
                        .encrypt(n, Payload { msg: data, aad })
                        .map_err(|e| HeError::crypto(e.to_string()))?
                }
                32 => {
                    let cipher = AesGcm::<Aes256, U12, $ts>::new_from_slice(key).map_err(|e| HeError::crypto(e.to_string()))?;
                    let n = Nonce::<U12>::from_slice(nonce);
                    cipher
                        .encrypt(n, Payload { msg: data, aad })
                        .map_err(|e| HeError::crypto(e.to_string()))?
                }
                _ => return Err(HeError::crypto(format!("invalid AES key size: {}", key.len()))),
            }
        };
    }
    let ct = match tag_size {
        12 => seal_with!(U12),
        13 => seal_with!(U13),
        14 => seal_with!(U14),
        15 => seal_with!(U15),
        16 => seal_with!(U16),
        _ => return Err(HeError::crypto("unsupported tag size")),
    };
    Ok(ct)
}

fn gcm_open(key: &[u8], nonce: &[u8], data: &[u8], aad: &[u8], tag_size: usize) -> HeResult<Vec<u8>> {
    use aes_gcm::aead::generic_array::typenum::{U12, U13, U14, U15, U16};
    macro_rules! open_with {
        ($ts:ty) => {
            match key.len() {
                16 => {
                    let cipher = AesGcm::<Aes128, U12, $ts>::new_from_slice(key).map_err(|e| HeError::crypto(e.to_string()))?;
                    let n = Nonce::<U12>::from_slice(nonce);
                    cipher
                        .decrypt(n, Payload { msg: data, aad })
                        .map_err(|e| HeError::crypto(e.to_string()))?
                }
                24 => {
                    let cipher = AesGcm::<Aes192, U12, $ts>::new_from_slice(key).map_err(|e| HeError::crypto(e.to_string()))?;
                    let n = Nonce::<U12>::from_slice(nonce);
                    cipher
                        .decrypt(n, Payload { msg: data, aad })
                        .map_err(|e| HeError::crypto(e.to_string()))?
                }
                32 => {
                    let cipher = AesGcm::<Aes256, U12, $ts>::new_from_slice(key).map_err(|e| HeError::crypto(e.to_string()))?;
                    let n = Nonce::<U12>::from_slice(nonce);
                    cipher
                        .decrypt(n, Payload { msg: data, aad })
                        .map_err(|e| HeError::crypto(e.to_string()))?
                }
                _ => return Err(HeError::crypto(format!("invalid AES key size: {}", key.len()))),
            }
        };
    }
    let pt = match tag_size {
        12 => open_with!(U12),
        13 => open_with!(U13),
        14 => open_with!(U14),
        15 => open_with!(U15),
        16 => open_with!(U16),
        _ => return Err(HeError::crypto("unsupported tag size")),
    };
    Ok(pt)
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::STANDARD as B64;
    use base64::Engine as _;

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
}
