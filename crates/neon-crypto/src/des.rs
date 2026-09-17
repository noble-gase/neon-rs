//! DES 对称加密（CBC / ECB + PKCS#7）
//!
//! # 安全提示
//! 单 DES 密钥仅 56 位，早已可被暴力破解；ECB 模式也不隐藏明文模式。
//! 本模块**仅用于对接遗留系统（legacy）**，新业务请使用 AES-GCM

use anyhow::anyhow;
use cipher::block_padding::Pkcs7;
use cipher::{
    Array, BlockCipherDecrypt, BlockCipherEncrypt, BlockModeDecrypt, BlockModeEncrypt, KeyInit,
    KeyIvInit, consts::U8,
};
use des::Des;

use crate::{CipherText, pkcs7_padding, pkcs7_unpadding};

const BLOCK_SIZE: usize = 8;

// --------------------------- CBC ---------------------------

/// DES-CBC 加密（PKCS#7）
///
/// `key` 和 `iv` 均须为 8 字节。每次加密应使用不可预测的新 IV；
/// 返回的密文不包含 IV，调用方需自行保存。CBC 不提供完整性认证。
pub fn des_encrypt_cbc(
    key: impl AsRef<[u8]>,
    iv: impl AsRef<[u8]>,
    data: impl AsRef<[u8]>,
) -> anyhow::Result<CipherText> {
    let iv = iv.as_ref();
    if iv.len() != BLOCK_SIZE {
        return Err(anyhow!("IV length must equal block size"));
    }
    let cipher = cbc::Encryptor::<Des>::new_from_slices(key.as_ref(), iv)?;
    Ok(CipherText {
        bytes: cipher.encrypt_padded_vec::<Pkcs7>(data.as_ref()),
        tag_size: 0,
    })
}

/// DES-CBC 解密（PKCS#7）
///
/// `key` 和 `iv` 均须为 8 字节，并与加密时使用的值一致。
pub fn des_decrypt_cbc(
    key: impl AsRef<[u8]>,
    iv: impl AsRef<[u8]>,
    data: impl AsRef<[u8]>,
) -> anyhow::Result<Vec<u8>> {
    let iv = iv.as_ref();
    let data = data.as_ref();
    if iv.len() != BLOCK_SIZE {
        return Err(anyhow!("IV length must equal block size"));
    }
    if !data.len().is_multiple_of(BLOCK_SIZE) {
        return Err(anyhow!("input not full blocks"));
    }
    cbc::Decryptor::<Des>::new_from_slices(key.as_ref(), iv)?
        .decrypt_padded_vec::<Pkcs7>(data)
        .map_err(|e| anyhow!("cbc decrypt: {e}"))
}

// --------------------------- ECB ---------------------------

/// DES-ECB 加密（PKCS#7）
pub fn des_encrypt_ecb(
    key: impl AsRef<[u8]>,
    data: impl AsRef<[u8]>,
) -> anyhow::Result<CipherText> {
    let cipher = Des::new_from_slice(key.as_ref()).map_err(anyhow::Error::from)?;
    let mut buf = data.as_ref().to_vec();
    pkcs7_padding(&mut buf, BLOCK_SIZE)?;

    let (blocks, tail) = Array::<u8, U8>::slice_as_chunks_mut(&mut buf);
    debug_assert!(tail.is_empty(), "pkcs7_padding 必产生整数倍块");
    cipher.encrypt_blocks(blocks);
    Ok(CipherText {
        bytes: buf,
        tag_size: 0,
    })
}

/// DES-ECB 解密（PKCS#7）
pub fn des_decrypt_ecb(key: impl AsRef<[u8]>, data: impl AsRef<[u8]>) -> anyhow::Result<Vec<u8>> {
    let data = data.as_ref();
    if !data.len().is_multiple_of(BLOCK_SIZE) {
        return Err(anyhow!("input not full blocks"));
    }
    let cipher = Des::new_from_slice(key.as_ref()).map_err(anyhow::Error::from)?;

    let mut out = data.to_vec();
    let (blocks, _) = Array::<u8, U8>::slice_as_chunks_mut(&mut out);
    cipher.decrypt_blocks(blocks);
    pkcs7_unpadding(&mut out, BLOCK_SIZE)?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cbc_known_vector() {
        // LibreSSL: enc -des-cbc -K 3132333435363738 -iv 3837363534333231 -a
        let key = b"12345678";
        let iv = b"87654321";
        let data = b"hello world";
        let ct = des_encrypt_cbc(key, iv, data).unwrap();
        assert_eq!(ct.to_string(), "f66U/RqLiA2NVFTdjfMMQA==");
        assert!(ct.tag().is_empty());
        assert_eq!(des_decrypt_cbc(key, iv, ct.bytes()).unwrap(), data);
    }

    #[test]
    fn cbc_roundtrip() {
        for len in [0, 1, 7, 8, 9, 16, 31, 32] {
            let data = vec![0x42; len];
            let ct = des_encrypt_cbc(b"12345678", b"87654321", &data).unwrap();
            assert_eq!(ct.bytes().len(), (len / BLOCK_SIZE + 1) * BLOCK_SIZE);
            let pt = des_decrypt_cbc(b"12345678", b"87654321", ct.bytes()).unwrap();
            assert_eq!(pt, data);
        }
    }

    #[test]
    fn cbc_invalid_inputs() {
        let key = b"12345678";
        let iv = b"87654321";
        let ct = des_encrypt_cbc(key, iv, b"hello").unwrap();
        for len in [0, 7, 9] {
            let invalid = vec![0; len];
            assert!(des_encrypt_cbc(&invalid, iv, b"hello").is_err());
            assert!(des_decrypt_cbc(&invalid, iv, ct.bytes()).is_err());
            assert!(des_encrypt_cbc(key, &invalid, b"hello").is_err());
            assert!(des_decrypt_cbc(key, &invalid, ct.bytes()).is_err());
        }
        assert!(des_decrypt_cbc(key, iv, b"").is_err());
        assert!(des_decrypt_cbc(key, iv, &ct.bytes()[..7]).is_err());

        // 单块明文末字节的 padding 为 3，通过 IV 将其改为 0。
        let mut invalid_iv = *iv;
        invalid_iv[7] ^= 3;
        assert!(des_decrypt_cbc(key, invalid_iv, ct.bytes()).is_err());
    }

    #[test]
    fn des_roundtrip() {
        let key = b"12345678";
        let data = b"hello world";
        let ct = des_encrypt_ecb(key, data).unwrap();
        let pt = des_decrypt_ecb(key, ct.bytes()).unwrap();
        assert_eq!(pt, data);
    }
}
