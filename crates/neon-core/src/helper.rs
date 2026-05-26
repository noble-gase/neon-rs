use getrandom;

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
