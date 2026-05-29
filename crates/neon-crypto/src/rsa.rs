use std::fmt;

use anyhow::anyhow;
use digest::Digest;
use md5::Md5;
use rsa::pkcs1::{DecodeRsaPrivateKey, DecodeRsaPublicKey, EncodeRsaPrivateKey, EncodeRsaPublicKey, LineEnding as Pkcs1LineEnding};
use rsa::pkcs8::{DecodePrivateKey, DecodePublicKey, EncodePrivateKey, EncodePublicKey, LineEnding as Pkcs8LineEnding};
use rsa::signature::{RandomizedSigner, SignatureEncoding, Verifier};
use rsa::traits::PublicKeyParts;
use rsa::{Oaep, Pkcs1v15Encrypt, RsaPrivateKey, RsaPublicKey};
use spki::SubjectPublicKeyInfoOwned;
use x509_cert::Certificate;
use x509_cert::der::Decode;

/// PKCS#1 RSA 私钥 PEM 标签
pub const RSA_PRIVATE_KEY: &str = "RSA PRIVATE KEY";
/// PKCS#8 私钥 PEM 标签
pub const PRIVATE_KEY: &str = "PRIVATE KEY";
/// PKCS#1 RSA 公钥 PEM 标签
pub const RSA_PUBLIC_KEY: &str = "RSA PUBLIC KEY";
/// PKIX 公钥 PEM 标签
pub const PUBLIC_KEY: &str = "PUBLIC KEY";
/// X.509 证书 PEM 标签
pub const CERTIFICATE: &str = "CERTIFICATE";

/// 生成的 RSA 密钥对（PEM 编码）
#[derive(Debug, Clone)]
pub struct KeyPair {
    pub private_pem: String,
    pub public_pem: String,
}

/// 生成 PKCS#1 格式的 RSA 密钥对
pub fn generate_pkcs1_keypair(bits: usize) -> anyhow::Result<KeyPair> {
    let key = generate_rsa_key(bits)?;
    let private_pem = key
        .to_pkcs1_pem(Pkcs1LineEnding::LF)
        .map(|s| s.to_string())
        .map_err(|e| anyhow!("encode PKCS#1 private key: {e}"))?;
    let public_pem = RsaPublicKey::from(&key)
        .to_pkcs1_pem(Pkcs1LineEnding::LF)
        .map(|s| s.to_string())
        .map_err(|e| anyhow!("encode PKCS#1 public key: {e}"))?;
    Ok(KeyPair { private_pem, public_pem })
}

/// 生成 PKCS#8 格式的 RSA 密钥对
pub fn generate_pkcs8_keypair(bits: usize) -> anyhow::Result<KeyPair> {
    let key = generate_rsa_key(bits)?;
    let private_pem = key
        .to_pkcs8_pem(Pkcs8LineEnding::LF)
        .map(|s| s.to_string())
        .map_err(|e| anyhow!("encode PKCS#8 private key: {e}"))?;
    let public_pem = RsaPublicKey::from(&key)
        .to_public_key_pem(Pkcs8LineEnding::LF)
        .map(|s| s.to_string())
        .map_err(|e| anyhow!("encode PKIX public key: {e}"))?;
    Ok(KeyPair { private_pem, public_pem })
}

fn generate_rsa_key(bits: usize) -> anyhow::Result<RsaPrivateKey> {
    let mut rng = rand::rng();
    RsaPrivateKey::new(&mut rng, bits).map_err(|e| anyhow!("generate rsa key: {e}"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrivatePemType {
    /// PKCS#1
    RsaPrivateKey,
    /// PKCS#8
    PrivateKey,
}

impl PrivatePemType {
    pub fn as_str(&self) -> &'static str {
        match self {
            PrivatePemType::RsaPrivateKey => RSA_PRIVATE_KEY,
            PrivatePemType::PrivateKey => PRIVATE_KEY,
        }
    }
}

impl fmt::Display for PrivatePemType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

// --------- PEM Format ---------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicPemType {
    /// PKCS#1
    RsaPublicKey,
    /// PKCS#8 / PKIX
    PublicKey,
    /// X.509 证书
    Certificate,
}

impl PublicPemType {
    pub fn as_str(&self) -> &'static str {
        match self {
            PublicPemType::RsaPublicKey => RSA_PUBLIC_KEY,
            PublicPemType::PublicKey => PUBLIC_KEY,
            PublicPemType::Certificate => CERTIFICATE,
        }
    }
}

impl fmt::Display for PublicPemType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// 把 base64-encoded raw 转成 PEM 字符串（每 64 字符换行）
pub fn format_private_pem_raw(raw: &str, pem_type: PrivatePemType) -> String {
    format_pem(raw, pem_type.as_str())
}

/// 把 base64 原始内容格式化为公钥/证书 PEM 字符串（每 64 字符换行）
pub fn format_public_pem_raw(raw: &str, pem_type: PublicPemType) -> String {
    format_pem(raw, pem_type.as_str())
}

fn format_pem(raw: &str, ty: &str) -> String {
    const LINE_LEN: usize = 64;
    let mut out = String::with_capacity(raw.len() + 64);
    out.push_str("-----BEGIN ");
    out.push_str(ty);
    out.push_str("-----\n");
    let bytes = raw.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let end = (i + LINE_LEN).min(bytes.len());
        // raw 是 ascii base64，所以可以直接按字节切
        out.push_str(std::str::from_utf8(&bytes[i..end]).unwrap_or(""));
        out.push('\n');
        i = end;
    }
    out.push_str("-----END ");
    out.push_str(ty);
    out.push_str("-----\n");
    out
}

// --------- PrivateKey ---------

/// RSA 私钥
pub struct PrivateKey {
    key: RsaPrivateKey,
}

impl PrivateKey {
    /// 从 PEM 解析私钥（支持 PKCS#1 / PKCS#8）
    pub fn from_pem(data: impl AsRef<[u8]>) -> anyhow::Result<Self> {
        let pem = pem::parse(data.as_ref()).map_err(|e| anyhow!("no PEM data: {e}"))?;
        match pem.tag() {
            RSA_PRIVATE_KEY => RsaPrivateKey::from_pkcs1_der(pem.contents())
                .map(|key| PrivateKey { key })
                .map_err(|e| anyhow!("PKCS#1: {e}")),
            PRIVATE_KEY => RsaPrivateKey::from_pkcs8_der(pem.contents())
                .map(|key| PrivateKey { key })
                .map_err(|e| anyhow!("PKCS#8: {e}")),
            other => Err(anyhow!("unsupported PEM type: {other}")),
        }
    }

    /// 导出为 PEM
    pub fn to_pem(&self, pem_type: PrivatePemType) -> anyhow::Result<String> {
        match pem_type {
            PrivatePemType::RsaPrivateKey => self
                .key
                .to_pkcs1_pem(Pkcs1LineEnding::LF)
                .map(|s| s.to_string())
                .map_err(|e| anyhow!("encode PKCS#1 private key: {e}")),
            PrivatePemType::PrivateKey => self
                .key
                .to_pkcs8_pem(Pkcs8LineEnding::LF)
                .map(|s| s.to_string())
                .map_err(|e| anyhow!("encode PKCS#8 private key: {e}")),
        }
    }

    /// 导出对应公钥
    pub fn public_key(&self) -> PublicKey {
        PublicKey {
            key: RsaPublicKey::from(&self.key),
        }
    }

    /// PKCS#1 v1.5 解密（legacy，仅用于兼容）
    pub fn decrypt(&self, data: impl AsRef<[u8]>) -> anyhow::Result<Vec<u8>> {
        self.key
            .decrypt(Pkcs1v15Encrypt, data.as_ref())
            .map_err(|e| anyhow!("rsa decrypt: {e}"))
    }

    /// OAEP 解密；`D` 为 MGF/哈希算法（如 `Sha256`）
    pub fn decrypt_oaep<D>(&self, data: impl AsRef<[u8]>) -> anyhow::Result<Vec<u8>>
    where
        D: Digest + digest::FixedOutputReset,
    {
        self.key
            .decrypt(Oaep::<D>::default(), data.as_ref())
            .map_err(|e| anyhow!("rsa decrypt_oaep: {e}"))
    }

    /// PKCS#1 v1.5 签名；`D` 为摘要算法
    pub fn sign<D>(&self, data: impl AsRef<[u8]>) -> anyhow::Result<Vec<u8>>
    where
        D: Digest + digest::FixedOutputReset + digest::HashMarker + digest::const_oid::AssociatedOid,
    {
        use rsa::pkcs1v15::SigningKey;
        let mut rng = rand::rng();
        Ok(SigningKey::<D>::new(self.key.clone())
            .sign_with_rng(&mut rng, data.as_ref())
            .to_vec())
    }

    /// MD5 签名（legacy，仅用于兼容）
    pub fn sign_md5(&self, data: impl AsRef<[u8]>) -> anyhow::Result<Vec<u8>> {
        use rsa::pkcs1v15::SigningKey;
        let mut rng = rand::rng();
        Ok(SigningKey::<Md5>::new_unprefixed(self.key.clone())
            .sign_with_rng(&mut rng, data.as_ref())
            .to_vec())
    }

    /// PSS 签名，salt 长度默认取哈希输出长度
    pub fn sign_pss<D>(&self, data: impl AsRef<[u8]>) -> anyhow::Result<Vec<u8>>
    where
        D: Digest + digest::FixedOutputReset,
    {
        self.sign_pss_with_salt_len::<D>(data, <D as Digest>::output_size())
    }

    /// PSS 签名，指定 salt 长度
    pub fn sign_pss_with_salt_len<D>(&self, data: impl AsRef<[u8]>, salt_len: usize) -> anyhow::Result<Vec<u8>>
    where
        D: Digest + digest::FixedOutputReset,
    {
        use rsa::pss::SigningKey;
        let mut rng = rand::rng();
        Ok(SigningKey::<D>::new_with_salt_len(self.key.clone(), salt_len)
            .sign_with_rng(&mut rng, data.as_ref())
            .to_vec())
    }

    /// 密钥字节长度
    pub fn size(&self) -> usize {
        self.key.size()
    }
}

// --------- PublicKey ---------

/// RSA 公钥（支持 PKCS#1 / PKIX / X.509 证书 PEM）
pub struct PublicKey {
    key: RsaPublicKey,
}

impl PublicKey {
    /// 从 PEM 解析公钥或证书
    pub fn from_pem(data: impl AsRef<[u8]>) -> anyhow::Result<Self> {
        let pem = pem::parse(data.as_ref()).map_err(|e| anyhow!("no PEM data: {e}"))?;
        match pem.tag() {
            RSA_PUBLIC_KEY => RsaPublicKey::from_pkcs1_der(pem.contents())
                .map(|key| PublicKey { key })
                .map_err(|e| anyhow!("PKCS#1: {e}")),
            PUBLIC_KEY => RsaPublicKey::from_public_key_der(pem.contents())
                .map(|key| PublicKey { key })
                .map_err(|e| anyhow!("PKIX: {e}")),
            CERTIFICATE => {
                let cert = Certificate::from_der(pem.contents()).map_err(|e| anyhow!("X.509: {e}"))?;
                public_key_from_spki(cert.tbs_certificate().subject_public_key_info().clone())
            }
            other => Err(anyhow!("unsupported PEM type: {other}")),
        }
    }

    /// 导出为 PEM（不支持导出为 X.509 证书）
    pub fn to_pem(&self, pem_type: PublicPemType) -> anyhow::Result<String> {
        match pem_type {
            PublicPemType::RsaPublicKey => self
                .key
                .to_pkcs1_pem(Pkcs1LineEnding::LF)
                .map(|s| s.to_string())
                .map_err(|e| anyhow!("encode PKCS#1 public key: {e}")),
            PublicPemType::PublicKey => self
                .key
                .to_public_key_pem(Pkcs8LineEnding::LF)
                .map(|s| s.to_string())
                .map_err(|e| anyhow!("encode PKIX public key: {e}")),
            PublicPemType::Certificate => Err(anyhow!("cannot encode public key as X.509 certificate")),
        }
    }

    /// PKCS#1 v1.5 加密（legacy，仅用于兼容）
    pub fn encrypt(&self, data: impl AsRef<[u8]>) -> anyhow::Result<Vec<u8>> {
        let mut rng = rand::rng();
        self.key
            .encrypt(&mut rng, Pkcs1v15Encrypt, data.as_ref())
            .map_err(|e| anyhow!("rsa encrypt: {e}"))
    }

    /// OAEP 加密；`D` 为 MGF/哈希算法（如 `Sha256`）
    pub fn encrypt_oaep<D>(&self, data: impl AsRef<[u8]>) -> anyhow::Result<Vec<u8>>
    where
        D: Digest + digest::FixedOutputReset,
    {
        let mut rng = rand::rng();
        self.key
            .encrypt(&mut rng, Oaep::<D>::default(), data.as_ref())
            .map_err(|e| anyhow!("rsa encrypt_oaep: {e}"))
    }

    /// PKCS#1 v1.5 验签；`D` 为摘要算法
    pub fn verify<D>(&self, data: impl AsRef<[u8]>, signature: impl AsRef<[u8]>) -> anyhow::Result<()>
    where
        D: Digest + digest::FixedOutputReset + digest::HashMarker + digest::const_oid::AssociatedOid,
    {
        use rsa::pkcs1v15::{Signature, VerifyingKey};
        let sig = Signature::try_from(signature.as_ref()).map_err(anyhow::Error::from)?;
        VerifyingKey::<D>::new(self.key.clone())
            .verify(data.as_ref(), &sig)
            .map_err(anyhow::Error::from)
    }

    /// MD5 验签（legacy，仅用于兼容）
    pub fn verify_md5(&self, data: impl AsRef<[u8]>, signature: impl AsRef<[u8]>) -> anyhow::Result<()> {
        use rsa::pkcs1v15::{Signature, VerifyingKey};
        let sig = Signature::try_from(signature.as_ref()).map_err(anyhow::Error::from)?;
        VerifyingKey::<Md5>::new_unprefixed(self.key.clone())
            .verify(data.as_ref(), &sig)
            .map_err(anyhow::Error::from)
    }

    /// PSS 验签，salt 长度默认取哈希输出长度
    pub fn verify_pss<D>(&self, data: impl AsRef<[u8]>, signature: impl AsRef<[u8]>) -> anyhow::Result<()>
    where
        D: Digest + digest::FixedOutputReset,
    {
        self.verify_pss_with_salt_len::<D>(data, signature, <D as Digest>::output_size())
    }

    /// PSS 验签，指定 salt 长度
    pub fn verify_pss_with_salt_len<D>(&self, data: impl AsRef<[u8]>, signature: impl AsRef<[u8]>, salt_len: usize) -> anyhow::Result<()>
    where
        D: Digest + digest::FixedOutputReset,
    {
        use rsa::pss::{Signature, VerifyingKey};
        let sig = Signature::try_from(signature.as_ref()).map_err(anyhow::Error::from)?;
        VerifyingKey::<D>::new_with_salt_len(self.key.clone(), salt_len)
            .verify(data.as_ref(), &sig)
            .map_err(anyhow::Error::from)
    }

    pub fn size(&self) -> usize {
        self.key.size()
    }
}

fn public_key_from_spki(spki: SubjectPublicKeyInfoOwned) -> anyhow::Result<PublicKey> {
    use spki::der::Encode;
    let der = spki.to_der().map_err(anyhow::Error::from)?;
    RsaPublicKey::from_public_key_der(&der)
        .map(|key| PublicKey { key })
        .map_err(|e| anyhow!("X.509: {e}"))
}

// --------- PFX/P12 ---------

/// 从 PFX/P12 提取私钥
pub fn pfx_to_private_key(pfx: impl AsRef<[u8]>, password: &str) -> anyhow::Result<PrivateKey> {
    let (key_pem, _) = pfx_to_pem(pfx, password)?;
    PrivateKey::from_pem(key_pem.as_bytes())
}

/// 从 PFX/P12 提取 `(key_pem, cert_pem)`
pub fn pfx_to_pem(pfx: impl AsRef<[u8]>, password: &str) -> anyhow::Result<(String, String)> {
    let p12 = p12::PFX::parse(pfx.as_ref()).map_err(|e| anyhow!("pfx parse: {e}"))?;
    let key_bags = p12.key_bags(password).map_err(|e| anyhow!("pfx key: {e}"))?;
    let cert_bags = p12.cert_bags(password).map_err(|e| anyhow!("pfx cert: {e}"))?;
    if key_bags.is_empty() || cert_bags.is_empty() {
        return Err(anyhow!("pfx missing cert or key"));
    }

    let key_der = &key_bags[0];
    let key_tag = if RsaPrivateKey::from_pkcs8_der(key_der).is_ok() {
        PRIVATE_KEY
    } else if RsaPrivateKey::from_pkcs1_der(key_der).is_ok() {
        RSA_PRIVATE_KEY
    } else {
        return Err(anyhow!("pfx: not an RSA private key"));
    };
    let key_pem = pem::encode(&pem::Pem::new(key_tag.to_string(), key_der.clone()));

    let mut cert_pem = String::new();
    for cert in &cert_bags {
        cert_pem.push_str(&pem::encode(&pem::Pem::new(CERTIFICATE.to_string(), cert.clone())));
    }

    Ok((key_pem, cert_pem))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha1::Sha1;
    use sha2::Sha256;

    #[test]
    fn format_private() {
        let raw = "abcd".repeat(20); // 80 字符
        let pem = format_private_pem_raw(&raw, PrivatePemType::PrivateKey);
        assert!(pem.starts_with(&format!("-----BEGIN {PRIVATE_KEY}-----\n")));
        assert!(pem.ends_with(&format!("-----END {PRIVATE_KEY}-----\n")));
        // 80 字符应产生 2 行（64 + 16）
        let body: Vec<&str> = pem.lines().collect();
        assert_eq!(body[1].len(), 64);
        assert_eq!(body[2].len(), 16);
    }

    #[test]
    fn format_public_cert() {
        let raw = "MIIB";
        let pem = format_public_pem_raw(raw, PublicPemType::Certificate);
        assert!(pem.contains(&format!("-----BEGIN {CERTIFICATE}-----\nMIIB\n-----END {CERTIFICATE}-----\n")));
    }

    #[test]
    fn generate_pkcs1_roundtrip() {
        let KeyPair { private_pem, public_pem } = generate_pkcs1_keypair(1024).unwrap();
        assert!(private_pem.contains(RSA_PRIVATE_KEY));
        assert!(public_pem.contains(RSA_PUBLIC_KEY));
        let prvk = PrivateKey::from_pem(private_pem.as_bytes()).unwrap();
        let pubk = PublicKey::from_pem(public_pem.as_bytes()).unwrap();
        let data = b"hello rsa pkcs1";
        let sig = prvk.sign::<Sha256>(data).unwrap();
        pubk.verify::<Sha256>(data, &sig).unwrap();
    }

    #[test]
    fn generate_pkcs8_roundtrip() {
        let KeyPair { private_pem, public_pem } = generate_pkcs8_keypair(1024).unwrap();
        assert!(private_pem.contains(PRIVATE_KEY));
        assert!(public_pem.contains(PUBLIC_KEY));
        let prvk = PrivateKey::from_pem(private_pem.as_bytes()).unwrap();
        let pubk = PublicKey::from_pem(public_pem.as_bytes()).unwrap();
        assert_eq!(prvk.public_key().size(), pubk.size());
    }

    #[test]
    fn sign_verify() {
        let KeyPair { private_pem, public_pem } = generate_pkcs8_keypair(1024).unwrap();
        let prvk = PrivateKey::from_pem(&private_pem).unwrap();
        let pubk = PublicKey::from_pem(&public_pem).unwrap();
        let data = b"hello rsa";
        let sig = prvk.sign::<Sha256>(data).unwrap();
        pubk.verify::<Sha256>(data, &sig).unwrap();
        let sig2 = prvk.sign_pss::<Sha256>(data).unwrap();
        pubk.verify_pss::<Sha256>(data, &sig2).unwrap();
    }

    #[test]
    fn encrypt_decrypt() {
        let KeyPair { private_pem, public_pem } = generate_pkcs8_keypair(1024).unwrap();
        let prvk = PrivateKey::from_pem(&private_pem).unwrap();
        let pubk = PublicKey::from_pem(&public_pem).unwrap();
        let ct = pubk.encrypt(b"sensitive").unwrap();
        assert_eq!(prvk.decrypt(&ct).unwrap(), b"sensitive");
    }

    #[test]
    fn encrypt_decrypt_oaep() {
        let KeyPair { private_pem, public_pem } = generate_pkcs8_keypair(1024).unwrap();
        let prvk = PrivateKey::from_pem(&private_pem).unwrap();
        let pubk = PublicKey::from_pem(&public_pem).unwrap();
        let ct = pubk.encrypt_oaep::<Sha1>(b"sensitive-oaep").unwrap();
        assert_eq!(prvk.decrypt_oaep::<Sha1>(&ct).unwrap(), b"sensitive-oaep");
    }

    #[test]
    fn export_pem() {
        let KeyPair { private_pem, .. } = generate_pkcs8_keypair(1024).unwrap();
        let prvk = PrivateKey::from_pem(&private_pem).unwrap();
        let exported = prvk.to_pem(PrivatePemType::PrivateKey).unwrap();
        assert!(PrivateKey::from_pem(exported.as_bytes()).is_ok());
        let pubk = prvk.public_key();
        let pub_exported = pubk.to_pem(PublicPemType::PublicKey).unwrap();
        assert!(PublicKey::from_pem(pub_exported.as_bytes()).is_ok());
    }
}
