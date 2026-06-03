//! 通用基础工具：随机串、时区转换、坐标计算、树形结构构建、远程 ZIP 读取 等

pub mod coord;
pub mod httpzip;
pub mod params;
pub mod tree;
pub mod zoned;

/// 通用 JSON 对象类型
pub type X = serde_json::Map<String, serde_json::Value>;

/// 生成指定长度的随机串（size 应为偶数）
pub fn nonce(size: usize) -> String {
    let mut buf = vec![0u8; size / 2];
    getrandom::fill(&mut buf).expect("failed to read random bytes");
    const_hex::encode(buf)
}

/// 生成指定长度的随机字节
pub fn nonce_bytes(size: usize) -> Vec<u8> {
    let mut buf = vec![0u8; size];
    getrandom::fill(&mut buf).expect("failed to read random bytes");
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nonce() {
        let nonce = nonce(32);
        assert_eq!(nonce.len(), 32);
        println!("nonce: {}", nonce);
    }

    #[test]
    fn nonce_hex_length() {
        assert_eq!(nonce(32).len(), 32);
        assert_eq!(nonce(16).len(), 16);
    }

    #[test]
    fn nonce_hex_chars() {
        let s = nonce(32);
        assert!(s.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn nonce_odd_size_truncates() {
        assert_eq!(nonce(31).len(), 30);
    }

    #[test]
    fn test_nonce_bytes() {
        let bytes = nonce_bytes(32);
        assert_eq!(bytes.len(), 32);
        println!("nonce_bytes: {:?}", bytes);
    }

    #[test]
    fn nonce_bytes_length() {
        assert_eq!(nonce_bytes(32).len(), 32);
        assert_eq!(nonce_bytes(17).len(), 17);
    }

    #[test]
    fn nonce_bytes_empty() {
        assert!(nonce_bytes(0).is_empty());
    }

    #[test]
    fn nonce_bytes_not_all_zero() {
        let bytes = nonce_bytes(32);
        assert!(bytes.iter().any(|&b| b != 0));
    }
}
