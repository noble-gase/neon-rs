//! `application/x-www-form-urlencoded` 编码与解析工具
//!
//! 同时适用于 URL query 字符串和 HTML 表单 body。

use std::collections::HashMap;

/// 将键值对序列编码为 URL query（保留顺序，同一 key 可多次出现）
pub fn encode<K, V>(pairs: &[(K, V)]) -> String
where
    K: AsRef<str>,
    V: AsRef<str>,
{
    let mut ser = form_urlencoded::Serializer::new(String::new());
    for (k, v) in pairs {
        ser.append_pair(k.as_ref(), v.as_ref());
    }
    ser.finish()
}

/// 将 URL query 字符串解析为 `HashMap`（同一 key 多次出现时按顺序收集为 `Vec`）
pub fn parse(query: impl AsRef<str>) -> HashMap<String, Vec<String>> {
    let mut map: HashMap<String, Vec<String>> = HashMap::new();
    for (k, v) in form_urlencoded::parse(query.as_ref().as_bytes()) {
        map.entry(k.into_owned()).or_default().push(v.into_owned());
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_empty() {
        let pairs: Vec<(String, String)> = Vec::new();
        let query = encode(&pairs);
        assert_eq!(query, "");
    }

    #[test]
    fn encode_roundtrip() {
        let pairs = vec![
            ("b".to_string(), "c&d".to_string()),
            ("a".to_string(), "1+2".to_string()),
        ];
        let query = encode(&pairs);
        println!("query: {}", query);

        let parsed = parse(&query);
        assert_eq!(parsed.get("a").cloned(), Some(vec!["1+2".into()]));
        assert_eq!(parsed.get("b").cloned(), Some(vec!["c&d".into()]));
    }

    #[test]
    fn parse_duplicate_key() {
        let map = parse("a=1&a=2&b=3");
        assert_eq!(map.get("a").cloned(), Some(vec!["1".into(), "2".into()]));
        assert_eq!(map.get("b").cloned(), Some(vec!["3".into()]));
    }
}
