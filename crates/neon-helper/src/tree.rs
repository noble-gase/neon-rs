//! 由扁平列表按父 ID 构建树

use std::collections::HashMap;
use std::hash::Hash;

use serde::{Deserialize, Serialize};

/// 扁平记录需提供 ID 与 PID
pub trait Node<E> {
    fn id(&self) -> E;
    fn pid(&self) -> E;
}

/// 树节点；`id` / `pid` 通过 [`Node`] 从 `data` 读取
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TreeNode<T> {
    pub data: T,
    pub children: Vec<TreeNode<T>>,
}

fn build_tree<T, E>(by_parent: &mut HashMap<E, Vec<T>>, parent_id: E) -> Vec<TreeNode<T>>
where
    T: Node<E>,
    E: Eq + Hash + Clone,
{
    let nodes = by_parent.remove(&parent_id).unwrap_or_default();
    let mut out = Vec::with_capacity(nodes.len());
    for node in nodes {
        let id = node.id();
        out.push(TreeNode {
            data: node,
            children: build_tree(by_parent, id),
        });
    }
    out
}

/// 构建树：`root_id` 为顶层节点所挂的父 ID（即这些节点的 `pid()` 等于 `root_id`）
pub fn new_tree<T, E>(data: impl IntoIterator<Item = T>, root_id: E) -> Vec<TreeNode<T>>
where
    T: Node<E>,
    E: Eq + Hash + Clone,
{
    let data: Vec<T> = data.into_iter().collect();
    let mut by_parent: HashMap<E, Vec<T>> = HashMap::with_capacity(data.len());
    for node in data {
        by_parent.entry(node.pid()).or_default().push(node);
    }
    build_tree(&mut by_parent, root_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dump_tree<T: std::fmt::Debug>(label: &str, tree: &[TreeNode<T>]) {
        println!("\n=== {label} ===");
        println!("{tree:#?}");
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Item {
        id: i32,
        pid: i32,
        name: &'static str,
    }

    impl Node<i32> for Item {
        fn id(&self) -> i32 {
            self.id
        }

        fn pid(&self) -> i32 {
            self.pid
        }
    }

    #[test]
    fn builds_hierarchy() {
        let data = vec![
            Item { id: 1, pid: 0, name: "a" },
            Item { id: 2, pid: 1, name: "b" },
            Item { id: 3, pid: 1, name: "c" },
            Item { id: 4, pid: 2, name: "d" },
        ];
        let tree = new_tree(data, 0);
        dump_tree("builds_hierarchy", &tree);
        assert_eq!(tree.len(), 1);
        assert_eq!(tree[0].data.id, 1);
        assert_eq!(tree[0].data.name, "a");
        assert_eq!(tree[0].children.len(), 2);
        assert_eq!(tree[0].children[0].data.id, 2);
        assert_eq!(tree[0].children[0].children.len(), 1);
        assert_eq!(tree[0].children[0].children[0].data.id, 4);
        assert_eq!(tree[0].children[0].children[0].children.len(), 0);
        assert_eq!(tree[0].children[1].data.id, 3);
        assert!(tree[0].children[1].children.is_empty());
    }

    #[test]
    fn empty_when_no_nodes_under_root() {
        let tree = new_tree(vec![Item { id: 1, pid: 99, name: "x" }], 0);
        dump_tree("empty_when_no_nodes_under_root", &tree);
        assert!(tree.is_empty());
    }

    #[test]
    fn multiple_top_level_nodes() {
        let tree = new_tree(vec![Item { id: 1, pid: 0, name: "a" }, Item { id: 2, pid: 0, name: "b" }], 0);
        dump_tree("multiple_top_level_nodes", &tree);
        assert_eq!(tree.len(), 2);
        assert_eq!(tree[0].data.id, 1);
        assert_eq!(tree[1].data.id, 2);
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct JsonItem {
        id: i32,
        pid: i32,
        name: String,
    }

    impl Node<i32> for JsonItem {
        fn id(&self) -> i32 {
            self.id
        }

        fn pid(&self) -> i32 {
            self.pid
        }
    }

    #[test]
    fn json_roundtrip() {
        let data = vec![
            JsonItem {
                id: 1,
                pid: 0,
                name: "root".into(),
            },
            JsonItem {
                id: 2,
                pid: 1,
                name: "leaf".into(),
            },
        ];
        let tree = new_tree(data, 0);
        dump_tree("json_roundtrip (tree)", &tree);
        let json = serde_json::to_string_pretty(&tree).unwrap();
        println!("\n=== json_roundtrip (json) ===\n{json}");
        let restored: Vec<TreeNode<JsonItem>> = serde_json::from_str(&json).unwrap();
        assert_eq!(restored, tree);
    }
}
