use std::fmt;

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
            PrivatePemType::RsaPrivateKey => "RSA PRIVATE KEY",
            PrivatePemType::PrivateKey => "PRIVATE KEY",
        }
    }
}

impl fmt::Display for PrivatePemType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

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
            PublicPemType::RsaPublicKey => "RSA PUBLIC KEY",
            PublicPemType::PublicKey => "PUBLIC KEY",
            PublicPemType::Certificate => "CERTIFICATE",
        }
    }
}

impl fmt::Display for PublicPemType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// 把 base64-encoded raw 转成 PEM 字符串（每 64 字符换行）。
pub fn format_private_pem_raw(raw: &str, pem_type: PrivatePemType) -> String {
    format_pem(raw, pem_type.as_str())
}

/// 同上，但用于公钥/证书。
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_private() {
        let raw = "abcd".repeat(20); // 80 字符
        let pem = format_private_pem_raw(&raw, PrivatePemType::PrivateKey);
        assert!(pem.starts_with("-----BEGIN PRIVATE KEY-----\n"));
        assert!(pem.ends_with("-----END PRIVATE KEY-----\n"));
        // 80 字符应产生 2 行（64 + 16）
        let body: Vec<&str> = pem.lines().collect();
        assert_eq!(body[1].len(), 64);
        assert_eq!(body[2].len(), 16);
    }

    #[test]
    fn format_public_cert() {
        let raw = "MIIB";
        let pem = format_public_pem_raw(raw, PublicPemType::Certificate);
        assert!(pem.contains("-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"));
    }
}
