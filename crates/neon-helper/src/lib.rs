//! 通用基础工具：随机串、时区转换、坐标计算、树形结构构建、远程 ZIP 读取 等

pub mod coord;
pub mod httpzip;
pub mod tree;
pub mod zoned;

/// 生成随机串（size 应为偶数）
pub fn nonce(size: u8) -> String {
    let len = size / 2;
    let mut buf = vec![0u8; len as usize];
    getrandom::fill(&mut buf).expect("failed to read random bytes");
    const_hex::encode(buf)
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
}
