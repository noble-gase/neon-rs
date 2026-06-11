//! 参数字典，用于生成签名串
//!
//! 底层为 `BTreeMap<String, String>`，key 按 ASCII 字典序排列

use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::ops::{Deref, DerefMut};

/// 参数字典（key 按字典序排序）
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Params(BTreeMap<String, String>);

/// 签名 encode 时 value 为空字符串的处理策略
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum EmptyMode {
    /// 默认：`bar=baz&foo=`
    #[default]
    Default,
    /// 忽略：`bar=baz`
    Ignore,
    /// 仅保留 key：`bar=baz&foo`
    OnlyKey,
}

impl Params {
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    /// 按指定符号与分隔符编码签名串（按 key ASCII 升序）
    ///
    /// `empty_mode` 为 `None` 时使用 [`EmptyMode::Default`]
    pub fn encode(
        &self,
        sym: impl AsRef<str>,
        sep: impl AsRef<str>,
        empty_mode: Option<EmptyMode>,
        ignore_keys: Option<Vec<String>>,
    ) -> String {
        if self.0.is_empty() {
            return String::new();
        }

        let sym = sym.as_ref();
        let sep = sep.as_ref();
        let empty_mode = empty_mode.unwrap_or_default();
        let ignore_keys = ignore_keys.unwrap_or_default();

        let mut buf = String::new();
        buf.reserve(
            self.0
                .iter()
                .map(|(k, v)| k.len() + v.len() + sym.len() + sep.len())
                .sum(),
        );

        for (k, v) in self.0.iter() {
            if ignore_keys.contains(k) {
                continue;
            }
            if v.is_empty() && empty_mode == EmptyMode::Ignore {
                continue;
            }

            if !buf.is_empty() {
                buf.push_str(sep);
            }
            buf.push_str(k);
            if !v.is_empty() {
                buf.push_str(sym);
                buf.push_str(v);
                continue;
            }
            // 保留符号
            if empty_mode != EmptyMode::OnlyKey {
                buf.push_str(sym);
            }
        }

        buf
    }

    /// 从 `(key, value)` 迭代器构造 Params（key 会按 ASCII 字典序排序）
    ///
    /// 重复 key 时保留**首次**出现的 value。
    pub fn from_pairs<I, K, V>(iter: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: AsRef<str>,
    {
        let mut inner = BTreeMap::new();
        for (k, v) in iter {
            inner
                .entry(k.as_ref().to_owned())
                .or_insert_with(|| v.as_ref().to_owned());
        }
        Self(inner)
    }

    /// 从 `HashMap` 构造 Params（key 会按 ASCII 字典序排序）
    pub fn from_hash_map<K, V>(map: &HashMap<K, V>) -> Self
    where
        K: Eq + Hash + AsRef<str>,
        V: AsRef<str>,
    {
        Self(
            map.iter()
                .map(|(k, v)| (k.as_ref().to_owned(), v.as_ref().to_owned()))
                .collect(),
        )
    }

    /// 转为 `HashMap`
    pub fn to_hash_map(&self) -> HashMap<String, String> {
        self.0.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    /// 从 URL query 字符串解析为 Params
    ///
    /// 注意：重复 key 时保留**首次**出现的 value
    pub fn from_url_query(query: impl AsRef<str>) -> Self {
        let parsed = form_urlencoded::parse(query.as_ref().as_bytes());
        let mut inner = BTreeMap::new();
        for (k, v) in parsed {
            inner
                .entry(k.into_owned())
                .or_insert_with(|| v.into_owned());
        }
        Self(inner)
    }

    /// URL 编码
    pub fn url_encode(&self) -> String {
        let mut ser = form_urlencoded::Serializer::new(String::new());
        for (k, v) in self.0.iter() {
            ser.append_pair(k, v);
        }
        ser.finish()
    }
}

impl<K, V> From<HashMap<K, V>> for Params
where
    K: Eq + Hash + Into<String>,
    V: Into<String>,
{
    fn from(map: HashMap<K, V>) -> Self {
        Self(map.into_iter().map(|(k, v)| (k.into(), v.into())).collect())
    }
}

impl From<Params> for HashMap<String, String> {
    fn from(params: Params) -> Self {
        params.0.into_iter().collect()
    }
}

impl Deref for Params {
    type Target = BTreeMap<String, String>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Params {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_default() {
        let mut params = Params::new();
        params.insert("foo".into(), "quux".into());
        params.insert("bar".into(), "baz".into());
        assert_eq!(params.encode("=", "&", None, None), "bar=baz&foo=quux");
    }

    #[test]
    fn encode_empty() {
        assert_eq!(Params::new().encode("=", "&", None, None), "");
        assert_eq!(Params::new().url_encode(), "");
    }

    #[test]
    fn encode_empty_modes() {
        let mut params = Params::new();
        params.insert("foo".into(), "".into());
        params.insert("bar".into(), "baz".into());
        assert_eq!(params.encode("=", "&", None, None), "bar=baz&foo=");
        assert_eq!(
            params.encode("=", "&", Some(EmptyMode::Ignore), None),
            "bar=baz"
        );
        assert_eq!(
            params.encode("=", "&", Some(EmptyMode::OnlyKey), None),
            "bar=baz&foo"
        );
    }

    #[test]
    fn encode_ignore_keys() {
        let mut params = Params::new();
        params.insert("foo".into(), "quux".into());
        params.insert("bar".into(), "baz".into());
        params.insert("sign".into(), "xx".into());
        assert_eq!(
            params.encode("=", "&", None, Some(vec!["sign".to_string()])),
            "bar=baz&foo=quux"
        );
    }

    #[test]
    fn encode_ignore_keys_str_and_string() {
        let mut params = Params::new();
        params.insert("foo".into(), "quux".into());
        params.insert("sign".into(), "xx".into());
        let nonce = String::from("nonce");
        assert_eq!(
            params.encode("=", "&", None, Some(vec!["sign".into(), nonce])),
            "foo=quux"
        );
    }

    #[test]
    fn from_hash_map_sorted_encode() {
        let mut map: HashMap<String, String> = HashMap::new();
        map.insert("foo".into(), "quux".into());
        map.insert("bar".into(), "baz".into());
        let params = Params::from_hash_map(&map);
        assert_eq!(params.encode("=", "&", None, None), "bar=baz&foo=quux");
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn from_hash_map_owned() {
        let mut map: HashMap<String, String> = HashMap::new();
        map.insert("foo".into(), "quux".into());
        map.insert("bar".into(), "baz".into());
        let params = Params::from(map);
        assert_eq!(params.encode("=", "&", None, None), "bar=baz&foo=quux");
    }

    #[test]
    fn from_hash_map_str_refs() {
        let mut map = HashMap::new();
        map.insert("foo", "quux");
        map.insert("bar", "baz");
        let params = Params::from_hash_map(&map);
        assert_eq!(params.encode("=", "&", None, None), "bar=baz&foo=quux");
        let params = Params::from(map);
        assert_eq!(params.get("foo"), Some(&"quux".to_string()));
    }

    #[test]
    fn from_hash_map_mixed_string_and_str() {
        let mut map: HashMap<String, &str> = HashMap::new();
        map.insert("foo".into(), "quux");
        map.insert("bar".into(), "baz");
        let params = Params::from_hash_map(&map);
        assert_eq!(params.encode("=", "&", None, None), "bar=baz&foo=quux");
    }

    #[test]
    fn from_pairs_str_refs() {
        let params = Params::from_pairs([("foo", "quux"), ("bar", "baz")]);
        assert_eq!(params.encode("=", "&", None, None), "bar=baz&foo=quux");
    }

    #[test]
    fn from_pairs_duplicate_keys_first_wins() {
        let params = Params::from_pairs([("foo", "first"), ("foo", "second"), ("bar", "baz")]);
        assert_eq!(params.get("foo"), Some(&"first".to_string()));
        assert_eq!(params.get("bar"), Some(&"baz".to_string()));
    }

    #[test]
    fn from_pairs_owned_strings() {
        let params = Params::from_pairs(vec![
            (String::from("foo"), String::from("quux")),
            (String::from("bar"), String::from("baz")),
        ]);
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn to_hash_map_borrowed() {
        let mut params = Params::new();
        params.insert("foo".into(), "quux".into());
        params.insert("bar".into(), "baz".into());
        let map = params.to_hash_map();
        assert_eq!(map.get("foo"), Some(&"quux".to_string()));
        assert_eq!(map.get("bar"), Some(&"baz".to_string()));
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn to_hash_map_owned_roundtrip() {
        let mut expected: HashMap<String, String> = HashMap::new();
        expected.insert("foo".into(), "quux".into());
        expected.insert("bar".into(), "baz".into());
        let params = Params::from(expected.clone());
        let map: HashMap<_, _> = params.into();
        assert_eq!(map, expected);
    }

    #[test]
    fn from_url_query_duplicate_keys() {
        let params = Params::from_url_query("foo=first&foo=second&bar=baz");
        assert_eq!(params.get("foo"), Some(&"first".to_string()));
        assert_eq!(params.get("bar"), Some(&"baz".to_string()));
    }

    #[test]
    fn url_encode_format() {
        let mut params = Params::new();
        params.insert("a".into(), "1 2".into());
        params.insert("b".into(), "c&d".into());
        assert_eq!(params.url_encode(), "a=1+2&b=c%26d");
    }

    #[test]
    fn url_encode_roundtrip() {
        let mut params = Params::new();
        params.insert("a".into(), "1 2".into());
        params.insert("b".into(), "c&d".into());
        let back = Params::from_url_query(params.url_encode());
        assert_eq!(back.get("a"), Some(&"1 2".to_string()));
        assert_eq!(back.get("b"), Some(&"c&d".to_string()));
    }
}
