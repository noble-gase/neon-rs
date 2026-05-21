use md5::Md5;
use rsa::pkcs1::{DecodeRsaPrivateKey, DecodeRsaPublicKey};
use rsa::pkcs8::{DecodePrivateKey, DecodePublicKey};
use rsa::signature::{RandomizedSigner, SignatureEncoding, Verifier};
use rsa::traits::PublicKeyParts;
use rsa::{Oaep, Pkcs1v15Encrypt, RsaPrivateKey, RsaPublicKey};
use sha1::Sha1;
use sha2::{Sha224, Sha256, Sha384, Sha512};
use spki::SubjectPublicKeyInfoOwned;
use x509_cert::der::DecodePem;
use x509_cert::Certificate;

use crate::error::{HeError, HeResult};
use crate::hashkit::HashAlgo;

/// RSA 私钥。
pub struct PrivateKey {
    key: RsaPrivateKey,
}

impl PrivateKey {
    /// 从 PEM 字节构造（自动识别 PKCS#1 / PKCS#8）。
    pub fn from_pem(data: &[u8]) -> HeResult<Self> {
        let pem = pem::parse(data).map_err(|e| HeError::pem(format!("no PEM data: {e}")))?;
        match pem.tag() {
            "RSA PRIVATE KEY" => RsaPrivateKey::from_pkcs1_der(pem.contents())
                .map(|key| PrivateKey { key })
                .map_err(|e| HeError::pem(format!("PKCS#1: {e}"))),
            "PRIVATE KEY" => RsaPrivateKey::from_pkcs8_der(pem.contents())
                .map(|key| PrivateKey { key })
                .map_err(|e| HeError::pem(format!("PKCS#8: {e}"))),
            other => Err(HeError::pem(format!("unsupported PEM type: {other}"))),
        }
    }

    /// 取底层公钥。
    pub fn public_key(&self) -> PublicKey {
        PublicKey {
            key: RsaPublicKey::from(&self.key),
        }
    }

    /// RSA-PKCS#1 v1.5 解密。
    pub fn decrypt(&self, data: &[u8]) -> HeResult<Vec<u8>> {
        self.key
            .decrypt(Pkcs1v15Encrypt, data)
            .map_err(|e| HeError::crypto(format!("rsa decrypt: {e}")))
    }

    /// RSA-OAEP 解密。
    pub fn decrypt_oaep(&self, hash: HashAlgo, data: &[u8]) -> HeResult<Vec<u8>> {
        let oaep = oaep_for(hash)?;
        self.key
            .decrypt(oaep, data)
            .map_err(|e| HeError::crypto(format!("rsa decrypt_oaep: {e}")))
    }

    /// RSA-PKCS#1 v1.5 签名。
    pub fn sign(&self, hash: HashAlgo, data: &[u8]) -> HeResult<Vec<u8>> {
        use rsa::pkcs1v15::SigningKey;
        let mut rng = rand::rngs::OsRng;
        let sig: Vec<u8> = match hash {
            HashAlgo::Md5 => {
                // MD5 不支持 DigestInfo OID，使用 unprefixed（与一般行业用法保持兼容）
                let key = SigningKey::<Md5>::new_unprefixed(self.key.clone());
                key.sign_with_rng(&mut rng, data).to_vec()
            }
            HashAlgo::Sha1 => SigningKey::<Sha1>::new(self.key.clone()).sign_with_rng(&mut rng, data).to_vec(),
            HashAlgo::Sha224 => SigningKey::<Sha224>::new(self.key.clone())
                .sign_with_rng(&mut rng, data)
                .to_vec(),
            HashAlgo::Sha256 => SigningKey::<Sha256>::new(self.key.clone())
                .sign_with_rng(&mut rng, data)
                .to_vec(),
            HashAlgo::Sha384 => SigningKey::<Sha384>::new(self.key.clone())
                .sign_with_rng(&mut rng, data)
                .to_vec(),
            HashAlgo::Sha512 => SigningKey::<Sha512>::new(self.key.clone())
                .sign_with_rng(&mut rng, data)
                .to_vec(),
        };
        Ok(sig)
    }

    /// RSA-PSS 签名。`salt_length=None` 表示等于哈希长度（Go 版 PSSSaltLengthEqualsHash）。
    pub fn sign_pss(&self, hash: HashAlgo, data: &[u8], salt_length: Option<usize>) -> HeResult<Vec<u8>> {
        use rsa::pss::SigningKey;
        let mut rng = rand::rngs::OsRng;
        let salt = salt_length.unwrap_or(match hash {
            HashAlgo::Md5 => 16,
            HashAlgo::Sha1 => 20,
            HashAlgo::Sha224 => 28,
            HashAlgo::Sha256 => 32,
            HashAlgo::Sha384 => 48,
            HashAlgo::Sha512 => 64,
        });
        let sig: Vec<u8> = match hash {
            HashAlgo::Md5 => SigningKey::<Md5>::new_with_salt_len(self.key.clone(), salt)
                .sign_with_rng(&mut rng, data)
                .to_vec(),
            HashAlgo::Sha1 => SigningKey::<Sha1>::new_with_salt_len(self.key.clone(), salt)
                .sign_with_rng(&mut rng, data)
                .to_vec(),
            HashAlgo::Sha224 => SigningKey::<Sha224>::new_with_salt_len(self.key.clone(), salt)
                .sign_with_rng(&mut rng, data)
                .to_vec(),
            HashAlgo::Sha256 => SigningKey::<Sha256>::new_with_salt_len(self.key.clone(), salt)
                .sign_with_rng(&mut rng, data)
                .to_vec(),
            HashAlgo::Sha384 => SigningKey::<Sha384>::new_with_salt_len(self.key.clone(), salt)
                .sign_with_rng(&mut rng, data)
                .to_vec(),
            HashAlgo::Sha512 => SigningKey::<Sha512>::new_with_salt_len(self.key.clone(), salt)
                .sign_with_rng(&mut rng, data)
                .to_vec(),
        };
        Ok(sig)
    }

    /// 模数长度（bit 数 / 8）。
    pub fn size(&self) -> usize {
        self.key.size()
    }
}

/// RSA 公钥。
pub struct PublicKey {
    key: RsaPublicKey,
}

impl PublicKey {
    /// 从 PEM 字节构造（自动识别 PKCS#1 / PKIX / X.509 Certificate）。
    pub fn from_pem(data: &[u8]) -> HeResult<Self> {
        let pem = pem::parse(data).map_err(|e| HeError::pem(format!("no PEM data: {e}")))?;
        match pem.tag() {
            "RSA PUBLIC KEY" => RsaPublicKey::from_pkcs1_der(pem.contents())
                .map(|key| PublicKey { key })
                .map_err(|e| HeError::pem(format!("PKCS#1: {e}"))),
            "PUBLIC KEY" => RsaPublicKey::from_public_key_der(pem.contents())
                .map(|key| PublicKey { key })
                .map_err(|e| HeError::pem(format!("PKIX: {e}"))),
            "CERTIFICATE" => {
                let cert = Certificate::from_pem(data).map_err(|e| HeError::pem(format!("X.509: {e}")))?;
                let spki: SubjectPublicKeyInfoOwned = cert.tbs_certificate.subject_public_key_info;
                use spki::der::Encode;
                let der = spki.to_der().map_err(|e| HeError::pem(e.to_string()))?;
                RsaPublicKey::from_public_key_der(&der)
                    .map(|key| PublicKey { key })
                    .map_err(|e| HeError::pem(format!("X.509: {e}")))
            }
            other => Err(HeError::pem(format!("unsupported PEM type: {other}"))),
        }
    }

    /// RSA-PKCS#1 v1.5 加密。
    pub fn encrypt(&self, data: &[u8]) -> HeResult<Vec<u8>> {
        let mut rng = rand::rngs::OsRng;
        self.key
            .encrypt(&mut rng, Pkcs1v15Encrypt, data)
            .map_err(|e| HeError::crypto(format!("rsa encrypt: {e}")))
    }

    /// RSA-OAEP 加密。
    pub fn encrypt_oaep(&self, hash: HashAlgo, data: &[u8]) -> HeResult<Vec<u8>> {
        let mut rng = rand::rngs::OsRng;
        let oaep = oaep_for(hash)?;
        self.key
            .encrypt(&mut rng, oaep, data)
            .map_err(|e| HeError::crypto(format!("rsa encrypt_oaep: {e}")))
    }

    /// RSA-PKCS#1 v1.5 验签。
    pub fn verify(&self, hash: HashAlgo, data: &[u8], signature: &[u8]) -> HeResult<()> {
        use rsa::pkcs1v15::{Signature, VerifyingKey};
        let sig = Signature::try_from(signature).map_err(|e| HeError::sign(e.to_string()))?;
        match hash {
            HashAlgo::Md5 => VerifyingKey::<Md5>::new_unprefixed(self.key.clone())
                .verify(data, &sig)
                .map_err(|e: ::rsa::signature::Error| HeError::sign(e.to_string())),
            HashAlgo::Sha1 => VerifyingKey::<Sha1>::new(self.key.clone())
                .verify(data, &sig)
                .map_err(|e: ::rsa::signature::Error| HeError::sign(e.to_string())),
            HashAlgo::Sha224 => VerifyingKey::<Sha224>::new(self.key.clone())
                .verify(data, &sig)
                .map_err(|e: ::rsa::signature::Error| HeError::sign(e.to_string())),
            HashAlgo::Sha256 => VerifyingKey::<Sha256>::new(self.key.clone())
                .verify(data, &sig)
                .map_err(|e: ::rsa::signature::Error| HeError::sign(e.to_string())),
            HashAlgo::Sha384 => VerifyingKey::<Sha384>::new(self.key.clone())
                .verify(data, &sig)
                .map_err(|e: ::rsa::signature::Error| HeError::sign(e.to_string())),
            HashAlgo::Sha512 => VerifyingKey::<Sha512>::new(self.key.clone())
                .verify(data, &sig)
                .map_err(|e: ::rsa::signature::Error| HeError::sign(e.to_string())),
        }
    }

    /// RSA-PSS 验签。
    pub fn verify_pss(&self, hash: HashAlgo, data: &[u8], signature: &[u8], salt_length: Option<usize>) -> HeResult<()> {
        use rsa::pss::{Signature, VerifyingKey};
        let sig = Signature::try_from(signature).map_err(|e| HeError::sign(e.to_string()))?;
        let salt = salt_length.unwrap_or(match hash {
            HashAlgo::Md5 => 16,
            HashAlgo::Sha1 => 20,
            HashAlgo::Sha224 => 28,
            HashAlgo::Sha256 => 32,
            HashAlgo::Sha384 => 48,
            HashAlgo::Sha512 => 64,
        });
        match hash {
            HashAlgo::Md5 => VerifyingKey::<Md5>::new_with_salt_len(self.key.clone(), salt)
                .verify(data, &sig)
                .map_err(|e: ::rsa::signature::Error| HeError::sign(e.to_string())),
            HashAlgo::Sha1 => VerifyingKey::<Sha1>::new_with_salt_len(self.key.clone(), salt)
                .verify(data, &sig)
                .map_err(|e: ::rsa::signature::Error| HeError::sign(e.to_string())),
            HashAlgo::Sha224 => VerifyingKey::<Sha224>::new_with_salt_len(self.key.clone(), salt)
                .verify(data, &sig)
                .map_err(|e: ::rsa::signature::Error| HeError::sign(e.to_string())),
            HashAlgo::Sha256 => VerifyingKey::<Sha256>::new_with_salt_len(self.key.clone(), salt)
                .verify(data, &sig)
                .map_err(|e: ::rsa::signature::Error| HeError::sign(e.to_string())),
            HashAlgo::Sha384 => VerifyingKey::<Sha384>::new_with_salt_len(self.key.clone(), salt)
                .verify(data, &sig)
                .map_err(|e: ::rsa::signature::Error| HeError::sign(e.to_string())),
            HashAlgo::Sha512 => VerifyingKey::<Sha512>::new_with_salt_len(self.key.clone(), salt)
                .verify(data, &sig)
                .map_err(|e: ::rsa::signature::Error| HeError::sign(e.to_string())),
        }
    }
}

fn oaep_for(hash: HashAlgo) -> HeResult<Oaep> {
    Ok(match hash {
        HashAlgo::Md5 => Oaep::new::<Md5>(),
        HashAlgo::Sha1 => Oaep::new::<Sha1>(),
        HashAlgo::Sha224 => Oaep::new::<Sha224>(),
        HashAlgo::Sha256 => Oaep::new::<Sha256>(),
        HashAlgo::Sha384 => Oaep::new::<Sha384>(),
        HashAlgo::Sha512 => Oaep::new::<Sha512>(),
    })
}

/// PFX(p12) → RSA 私钥。
///
/// 注意：证书需采用 TripleDES-SHA1 加密方式（同 Go 版要求）。
pub fn pfx_to_private_key(pfx: &[u8], password: &str) -> HeResult<PrivateKey> {
    let (key_pem, _cert_pem) = pfx_to_pem(pfx, password)?;
    PrivateKey::from_pem(key_pem.as_bytes())
}

/// PFX(p12) → (private key PEM, certificate PEM)。
pub fn pfx_to_pem(pfx: &[u8], password: &str) -> HeResult<(String, String)> {
    let p12 = p12::PFX::parse(pfx).map_err(|e| HeError::pem(format!("pfx parse: {e}")))?;
    let key_bags = p12.key_bags(password).map_err(|e| HeError::pem(format!("pfx key: {e}")))?;
    let cert_bags = p12.cert_bags(password).map_err(|e| HeError::pem(format!("pfx cert: {e}")))?;
    if key_bags.is_empty() || cert_bags.is_empty() {
        return Err(HeError::pem("pfx missing cert or key"));
    }

    // Go 版 ToPEM 的私钥 tag 可能是 "PRIVATE KEY"/"RSA PRIVATE KEY"/"EC PRIVATE KEY"；
    // p12 crate 解出来的是 PKCS#8 DER，对应 "PRIVATE KEY"。
    let key_der = &key_bags[0];
    let key_pem = pem::encode(&pem::Pem::new("PRIVATE KEY".to_string(), key_der.clone()));

    // 多个证书拼接（中间证书链）。
    let mut cert_pem = String::new();
    for c in &cert_bags {
        cert_pem.push_str(&pem::encode(&pem::Pem::new("CERTIFICATE".to_string(), c.clone())));
    }
    Ok((key_pem, cert_pem))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsa::pkcs8::EncodePrivateKey;

    fn gen_pem() -> (String, String) {
        // 1024 bit 测试用，速度更快
        let mut rng = rand::rngs::OsRng;
        let key = RsaPrivateKey::new(&mut rng, 1024).unwrap();
        let der = key.to_pkcs8_der().unwrap();
        let priv_pem = pem::encode(&pem::Pem::new("PRIVATE KEY".to_string(), der.as_bytes().to_vec()));
        use rsa::pkcs8::EncodePublicKey;
        let pub_pem = RsaPublicKey::from(&key)
            .to_public_key_pem(spki::der::pem::LineEnding::LF)
            .unwrap();
        (priv_pem, pub_pem)
    }

    #[test]
    fn parse_pkcs8_pem_and_sign_verify() {
        let (priv_pem, pub_pem) = gen_pem();
        let prv = PrivateKey::from_pem(priv_pem.as_bytes()).unwrap();
        let pubk = PublicKey::from_pem(pub_pem.as_bytes()).unwrap();
        let data = b"hello rsa";
        let sig = prv.sign(HashAlgo::Sha256, data).unwrap();
        pubk.verify(HashAlgo::Sha256, data, &sig).unwrap();

        // PSS
        let sig2 = prv.sign_pss(HashAlgo::Sha256, data, None).unwrap();
        pubk.verify_pss(HashAlgo::Sha256, data, &sig2, None).unwrap();
    }

    #[test]
    fn encrypt_decrypt_pkcs1v15() {
        let (priv_pem, pub_pem) = gen_pem();
        let prv = PrivateKey::from_pem(priv_pem.as_bytes()).unwrap();
        let pubk = PublicKey::from_pem(pub_pem.as_bytes()).unwrap();
        let data = b"sensitive";
        let ct = pubk.encrypt(data).unwrap();
        let pt = prv.decrypt(&ct).unwrap();
        assert_eq!(pt, data);
    }

    #[test]
    fn encrypt_decrypt_oaep() {
        let (priv_pem, pub_pem) = gen_pem();
        let prv = PrivateKey::from_pem(priv_pem.as_bytes()).unwrap();
        let pubk = PublicKey::from_pem(pub_pem.as_bytes()).unwrap();
        let data = b"sensitive-oaep";
        let ct = pubk.encrypt_oaep(HashAlgo::Sha1, data).unwrap();
        let pt = prv.decrypt_oaep(HashAlgo::Sha1, &ct).unwrap();
        assert_eq!(pt, data);
    }
}
