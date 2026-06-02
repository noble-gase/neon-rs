//! DES 对称加密（ECB + PKCS#7）

use anyhow::anyhow;
use cipher::{Array, BlockCipherDecrypt, BlockCipherEncrypt, KeyInit, consts::U8};
use des::Des;

use crate::{CipherText, pkcs7_padding, pkcs7_unpadding};

const BLOCK_SIZE: usize = 8;

/// DES-ECB 加密（PKCS#7）
pub fn des_encrypt_ecb(key: impl AsRef<[u8]>, data: impl AsRef<[u8]>) -> anyhow::Result<CipherText> {
    let cipher = Des::new_from_slice(key.as_ref()).map_err(anyhow::Error::from)?;
    let mut buf = data.as_ref().to_vec();
    pkcs7_padding(&mut buf, BLOCK_SIZE)?;

    let (blocks, tail) = Array::<u8, U8>::slice_as_chunks_mut(&mut buf);
    debug_assert!(tail.is_empty(), "pkcs7_padding 必产生整数倍块");
    cipher.encrypt_blocks(blocks);
    Ok(CipherText { bytes: buf, tag_size: 0 })
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
    fn des_roundtrip() {
        let key = b"12345678";
        let data = b"hello world";
        let ct = des_encrypt_ecb(key, data).unwrap();
        let pt = des_decrypt_ecb(key, ct.bytes()).unwrap();
        assert_eq!(pt, data);
    }
}
