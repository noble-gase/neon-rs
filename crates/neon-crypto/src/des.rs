//! DES 对称加密（ECB + PKCS#7）

use anyhow::anyhow;
use cipher::{Array, BlockCipherDecrypt, BlockCipherEncrypt, KeyInit, consts::U8};
use des::Des;

use crate::{CipherText, pkcs7_padding, pkcs7_unpadding};

const BLOCK: usize = 8;

/// DES-ECB 加密（PKCS#7）
pub fn des_encrypt_ecb(key: impl AsRef<[u8]>, data: impl AsRef<[u8]>) -> anyhow::Result<CipherText> {
    let key = key.as_ref();
    let data = data.as_ref();
    let cipher = Des::new_from_slice(key).map_err(anyhow::Error::from)?;
    let mut buf = data.to_vec();

    pkcs7_padding(&mut buf, BLOCK);
    if !buf.len().is_multiple_of(BLOCK) {
        return Err(anyhow!("input not full blocks"));
    }

    for block in buf.chunks_mut(BLOCK) {
        let block = Array::<u8, U8>::slice_as_mut_array(block).ok_or_else(|| anyhow!("invalid block size"))?;
        cipher.encrypt_block(block);
    }
    Ok(CipherText { bytes: buf, tag_size: 0 })
}

/// DES-ECB 解密（PKCS#7）
pub fn des_decrypt_ecb(key: impl AsRef<[u8]>, data: impl AsRef<[u8]>) -> anyhow::Result<Vec<u8>> {
    let key = key.as_ref();
    let data = data.as_ref();
    let cipher = Des::new_from_slice(key).map_err(anyhow::Error::from)?;
    if !data.len().is_multiple_of(BLOCK) {
        return Err(anyhow!("input not full blocks"));
    }

    let mut out = data.to_vec();
    for block in out.chunks_mut(BLOCK) {
        let block = Array::<u8, U8>::slice_as_mut_array(block).ok_or_else(|| anyhow!("invalid block size"))?;
        cipher.decrypt_block(block);
    }
    pkcs7_unpadding(&mut out)?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn des_roundtrip() {
        let key = b"12345678";
        let data = b"hello world";
        let ct = des_encrypt_ecb(key, data).unwrap();
        let pt = des_decrypt_ecb(key, ct.bytes()).unwrap();
        assert_eq!(pt, data);
    }
}
