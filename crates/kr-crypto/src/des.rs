//! DES-ECB 加解密。

use aes::cipher::{generic_array::GenericArray, BlockDecrypt, BlockEncrypt, KeyInit};
use des::Des;

use crate::{
    crypto::{pkcs7_padding, pkcs7_unpadding},
    error::{HeError, HeResult},
};

const BLOCK: usize = 8;

/// DES-ECB 加密（PKCS#7）。
pub fn des_encrypt_ecb(key: &[u8], data: &[u8]) -> HeResult<Vec<u8>> {
    let cipher = Des::new_from_slice(key).map_err(|e| HeError::crypto(e.to_string()))?;
    let mut buf = data.to_vec();

    pkcs7_padding(&mut buf, BLOCK);
    if buf.len() % BLOCK != 0 {
        return Err(HeError::crypto("input not full blocks"));
    }

    let mut out = vec![0u8; buf.len()];
    for (in_b, out_b) in buf.chunks(BLOCK).zip(out.chunks_mut(BLOCK)) {
        let block: &GenericArray<u8, cipher::consts::U8> = GenericArray::from_slice(in_b);
        let out_arr: &mut GenericArray<u8, cipher::consts::U8> = GenericArray::from_mut_slice(out_b);

        let mut tmp = *block;
        cipher.encrypt_block(&mut tmp);
        out_arr.copy_from_slice(&tmp);
    }
    Ok(out)
}

/// DES-ECB 解密（PKCS#7）。
pub fn des_decrypt_ecb(key: &[u8], data: &[u8]) -> HeResult<Vec<u8>> {
    let cipher = Des::new_from_slice(key).map_err(|e| HeError::crypto(e.to_string()))?;
    if data.len() % BLOCK != 0 {
        return Err(HeError::crypto("input not full blocks"));
    }

    let mut out = vec![0u8; data.len()];
    for (in_b, out_b) in data.chunks(BLOCK).zip(out.chunks_mut(BLOCK)) {
        let block: &GenericArray<u8, cipher::consts::U8> = GenericArray::from_slice(in_b);
        let out_arr: &mut GenericArray<u8, cipher::consts::U8> = GenericArray::from_mut_slice(out_b);

        let mut tmp = *block;
        cipher.decrypt_block(&mut tmp);
        out_arr.copy_from_slice(&tmp);
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
        let pt = des_decrypt_ecb(key, &ct).unwrap();
        assert_eq!(pt, data);
    }
}
