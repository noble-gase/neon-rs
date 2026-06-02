//! 哈希/HMAC 工具；输出可为 hex 字符串或原始字节

use digest::Digest;
use digest::KeyInit;
use digest::Mac;
use hmac::SimpleHmac;
use md5::Md5;
use sha1::Sha1;
use sha2::Sha256;

/// 控制哈希结果的输出形式（`Vec<u8>` 原始字节 / `String` hex）
pub trait HashOutput: Sized {
    fn from_digest(bytes: &[u8]) -> Self;
}

impl HashOutput for Vec<u8> {
    #[inline]
    fn from_digest(bytes: &[u8]) -> Self {
        bytes.to_vec()
    }
}

impl HashOutput for String {
    #[inline]
    fn from_digest(bytes: &[u8]) -> Self {
        const_hex::encode(bytes)
    }
}

/// 计算 MD5；`T = String` 时返回 hex，`T = Vec<u8>` 时返回原始字节
pub fn md5<T: HashOutput>(data: impl AsRef<[u8]>) -> T {
    hash::<Md5, T>(data)
}

/// 计算 SHA-1
pub fn sha1<T: HashOutput>(data: impl AsRef<[u8]>) -> T {
    hash::<Sha1, T>(data)
}

/// 计算 SHA-256
pub fn sha256<T: HashOutput>(data: impl AsRef<[u8]>) -> T {
    hash::<Sha256, T>(data)
}

/// 使用指定 `Digest` 算法计算哈希
pub fn hash<D: Digest, T: HashOutput>(data: impl AsRef<[u8]>) -> T {
    let mut h = D::new();
    Digest::update(&mut h, data);
    T::from_digest(h.finalize().as_ref())
}

/// HMAC-SHA1
pub fn hmac_sha1<T: HashOutput>(key: impl AsRef<[u8]>, data: impl AsRef<[u8]>) -> anyhow::Result<T> {
    hmac::<Sha1, T>(key, data)
}

/// HMAC-SHA256
pub fn hmac_sha256<T: HashOutput>(key: impl AsRef<[u8]>, data: impl AsRef<[u8]>) -> anyhow::Result<T> {
    hmac::<Sha256, T>(key, data)
}

/// 使用指定 `Digest` 算法计算 HMAC
pub fn hmac<D, T>(key: impl AsRef<[u8]>, data: impl AsRef<[u8]>) -> anyhow::Result<T>
where
    D: Digest + digest::block_api::BlockSizeUser,
    T: HashOutput,
{
    let key = key.as_ref();
    if key.is_empty() {
        return Err(anyhow::anyhow!("HMAC key must not be empty"));
    }
    let mut h = SimpleHmac::<D>::new_from_slice(key).map_err(anyhow::Error::from)?;
    h.update(data.as_ref());
    Ok(T::from_digest(h.finalize().into_bytes().as_ref()))
}

#[cfg(test)]
mod tests {
    use md5::Md5;
    use sha1::Sha1;
    use sha2::{Sha224, Sha256, Sha384, Sha512, Sha512_224, Sha512_256};

    use crate::hash::{hash, hmac, hmac_sha1, hmac_sha256, md5, sha1, sha256};

    #[test]
    fn digest_hash() {
        assert_eq!(md5::<String>("shenghui"), "ff7f89cbe5c489ff2825d97c4e7b6f7c");
        assert_eq!(sha1::<String>("shenghui"), "5d06bcf2a58b4e2ae3280e031f84baa8a28db3aa");
        assert_eq!(sha256::<String>("shenghui"), "c6f540373c19d5cc0564fdce042b74d7e57c4fc352878f8128a7d513bac76568");
        assert_eq!(hash::<Sha1, String>("shenghui"), "5d06bcf2a58b4e2ae3280e031f84baa8a28db3aa");
        assert_eq!(hash::<Sha224, String>("shenghui"), "a79fee2960ea91b511556f393e3bbdc1da5aa17253b029c36adf0ef3");
        assert_eq!(
            hash::<Sha256, String>("shenghui"),
            "c6f540373c19d5cc0564fdce042b74d7e57c4fc352878f8128a7d513bac76568"
        );
        assert_eq!(
            hash::<Sha384, String>("shenghui"),
            "1ad756ef7fbc0912b56d2609a646a2887ce34f70cbb0144a86a2f394a121dee88d09d0b47e0b99f039f36e7dba06e90d"
        );
        assert_eq!(
            hash::<Sha512, String>("shenghui"),
            "42071eb6241a2a19c01c1cb7cad9aa5730c1d15de8b54ff4f333e7c9e5854640084f20a1406bf362c22131725c432b387832a9431859eb031b914890ddd01671"
        );
        assert_eq!(hash::<Sha512_224, String>("shenghui"), "25ecca889865b41d2386b08d71e84bd4bb6dc9bfb4bda5127462ad90");
        assert_eq!(
            hash::<Sha512_256, String>("shenghui"),
            "f12bb32e3b8cf30102b9b2a316e84bc69ee009623197a17a97ed33dc8a71a872"
        );
    }

    #[test]
    fn digest_hmac() {
        assert_eq!(hmac::<Md5, String>("IIInsomnia", "shenghui").unwrap(), "cac9160ed60eb1bcca32c7460b5ca238");
        assert_eq!(hmac::<Sha1, String>("IIInsomnia", "shenghui").unwrap(), "750583660d10fbadf8004f462aa7ef1d9f18cd91");
        assert_eq!(
            hmac::<Sha224, String>("IIInsomnia", "shenghui").unwrap(),
            "c2b5456bf70ab7be63de54c055a66554d0ee558f1c6985a5325f2b0a"
        );
        assert_eq!(
            hmac::<Sha256, String>("IIInsomnia", "shenghui").unwrap(),
            "6ea90a066be004ca5ac384d79605d8a2403cc8a9b14ffc988822bf85be12b038"
        );
        assert_eq!(
            hmac::<Sha384, String>("IIInsomnia", "shenghui").unwrap(),
            "04faa29cd8da1e4d18d9890006242a90dfcb127e5914ceb18226857bdb04e106af54473afd6a061c9f6f16c70990d73c"
        );
        assert_eq!(
            hmac::<Sha512, String>("IIInsomnia", "shenghui").unwrap(),
            "094f0911af5717643188cce2537528f36212473a4756a110606b7c98bdcc5d0dcd64ee03acb7a2f8e91b6c46bd78ac82279ed9889834e52433da90a57c8ef506"
        );
        assert_eq!(
            hmac::<Sha512_224, String>("IIInsomnia", "shenghui").unwrap(),
            "94732693878898c638f449a4c3c2bc6d0ed73d43d2c1c2233aeedfa2"
        );
        assert_eq!(
            hmac::<Sha512_256, String>("IIInsomnia", "shenghui").unwrap(),
            "9863f2c13c3218265d374f82605ef368d6577e4d292d122117fa07c72839b71e"
        );
        assert_eq!(hmac_sha1::<String>("IIInsomnia", "shenghui").unwrap(), "750583660d10fbadf8004f462aa7ef1d9f18cd91");
        assert_eq!(
            hmac_sha256::<String>("IIInsomnia", "shenghui").unwrap(),
            "6ea90a066be004ca5ac384d79605d8a2403cc8a9b14ffc988822bf85be12b038"
        );
    }

    #[test]
    fn hmac_empty_key_returns_err() {
        assert!(hmac_sha1::<String>("", "data").is_err());
        assert!(hmac_sha256::<String>("", "data").is_err());
        assert!(hmac::<Sha256, String>("", "data").is_err());
    }
}
