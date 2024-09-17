use std::fs::File;
use super::node::{Node, leaf::LeafNode, internal::InternalNode};
use super::pager::{Pager, PageOperator, Offset};

pub(crate) type Key = String;
pub(crate) type Value = Vec<u8>;

pub struct BPTree {
    degree: usize,
    pager: Box<dyn PageOperator>,
    root_node: Option<Offset>,
}

impl BPTree {
    pub fn new(degree: usize, startup_offset: usize, file: File) -> Self {
        Self {
            degree,
            pager: Box::new(Pager::new(file, startup_offset)),
            root_node: None,
        }
    }

    pub fn is_empty(&mut self) -> anyhow::Result<bool> {
        match self.root_node.take() {
            None => Ok(true),
            Some(root_offset) => {
                let node = self.pager.read(root_offset)?;
                self.root_node = Some(root_offset);
                Ok(node.is_empty())
            },
        }
    }

    pub fn insert(&mut self, key: Key, value: Value) -> anyhow::Result<()> {
        match self.root_node.take() {
            None => {
                let root_node = Node::Leaf(LeafNode {
                    keys: vec![key],
                    values: vec![value],
                    offset: Some(self.pager.next_offset()),
                });
                let root_offset = self.pager.write(&root_node)?;
                self.root_node = Some(root_offset);
            },
            Some(root_offset) => {
                let mut root_node = self.pager.read(root_offset)?;
                let root_copy_offset = self.pager.write(&root_node)?;

                match root_node.insert(&mut self.pager, key, value, self.degree)? {
                    None => {
                        self.pager.write_at(&root_node, root_copy_offset)?;
                        self.root_node = Some(root_copy_offset);
                    },
                    Some((mid_key, sibling)) => {
                        let sibling_offset = self.pager.write(&sibling)?;
                        self.pager.write_at(&root_node, root_copy_offset)?;

                        let new_root = Node::Internal(InternalNode {
                            keys: vec![mid_key],
                            children: vec![root_copy_offset, sibling_offset],
                            offset: Some(self.pager.next_offset()),
                        });

                        let new_root_offset = self.pager.write(&new_root)?;
                        self.root_node = Some(new_root_offset);
                    },
                }
            },
        }

        Ok(())
    }

    pub fn delete(&mut self, key: Key) -> anyhow::Result<()> {
        match self.root_node.take() {
            None => {},
            Some(root_offset) => {
                let mut root_node = self.pager.read(root_offset)?;
                let root_copy_offset = self.pager.write(&root_node)?;

                let need_rebalance = root_node.remove(&mut self.pager, key, self.degree)?;
                self.pager.write_at(&root_node, root_copy_offset)?;

                self.root_node = match need_rebalance {
                    None => Some(root_copy_offset),
                    Some(value) => {
                        if value {
                            match root_node {
                                Node::Leaf(_) => Some(root_copy_offset),
                                Node::Internal(payload) => {
                                    if payload.keys.is_empty() {
                                        Some(payload.children[0])
                                    } else {
                                        Some(root_copy_offset)
                                    }
                                },
                            }
                        } else {
                            Some(root_copy_offset)
                        }
                    },
                }
            },
        }

        Ok(())
    }

    pub fn search(&mut self, key: Key) -> anyhow::Result<Option<Value>> {
        match self.root_node.take() {
            None => Ok(None),
            Some(root_offset) => {
                let root_node = self.pager.read(root_offset)?;
                self.root_node = Some(root_offset);
                root_node.search(&mut self.pager, key)
            },
        }
    }

    pub fn debug_print(&mut self) -> anyhow::Result<()> {
        if let Some(node_offset) = self.root_node {
            let node = self.pager.read(node_offset)?;
            let _ = node.debug_print(&mut self.pager, 0)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, HashSet},
        fs::OpenOptions,
    };

    use crate::pager::STARTUP_OFFSET;

    use super::*;

    #[test]
    fn test_tree_structure() -> anyhow::Result<()> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open("/tmp/test_tree_structure.ldb")
            .unwrap();

        let mut tree = BPTree::new(4, STARTUP_OFFSET, file);

        tree.insert("0010".to_string(), "ten".as_bytes().to_vec())?;
        tree.insert("0020".to_string(), "twenty".as_bytes().to_vec())?;
        tree.insert("0005".to_string(), "five".as_bytes().to_vec())?;
        tree.insert("0006".to_string(), "six".as_bytes().to_vec())?;
        tree.insert("0012".to_string(), "twelve".as_bytes().to_vec())?;
        tree.insert("0030".to_string(), "thirty".as_bytes().to_vec())?;
        tree.insert("0007".to_string(), "seven".as_bytes().to_vec())?;
        tree.insert("0017".to_string(), "seventeen".as_bytes().to_vec())?;

        assert_eq!(tree.search("0010".to_string())?, Some("ten".as_bytes().to_vec()));
        assert_eq!(tree.search("0020".to_string())?, Some("twenty".as_bytes().to_vec()));
        assert_eq!(tree.search("0005".to_string())?, Some("five".as_bytes().to_vec()));
        assert_eq!(tree.search("0006".to_string())?, Some("six".as_bytes().to_vec()));
        assert_eq!(tree.search("0012".to_string())?, Some("twelve".as_bytes().to_vec()));
        assert_eq!(tree.search("0030".to_string())?, Some("thirty".as_bytes().to_vec()));
        assert_eq!(tree.search("0007".to_string())?, Some("seven".as_bytes().to_vec()));
        assert_eq!(
            tree.search("0017".to_string())?,
            Some("seventeen".as_bytes().to_vec())
        );

        assert_eq!(tree.search("2000".to_string())?, None);
        assert_eq!(tree.search("3000".to_string())?, None);

        Ok(())
    }

    #[test]
    fn test_large_insertions() -> anyhow::Result<()> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open("/tmp/test_large_insertions.ldb")
            .unwrap();

        let mut tree = BPTree::new(300, STARTUP_OFFSET, file);

        for i in 1..=100000 {
            tree.insert(i.to_string(), i.to_string().as_bytes().to_vec())?;
        }

        for i in 1..=100000 {
            assert_eq!(tree.search(i.to_string())?, Some(i.to_string().as_bytes().to_vec()));
        }

        Ok(())
    }

    #[test]
    fn assemble_disassemble() -> anyhow::Result<()> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open("/tmp/assemble_disassemble.ldb")
            .unwrap();

        let mut tree = BPTree::new(4, 0, file);

        let key_value_pairs = BTreeMap::from([
            ("001".to_string(), "derby".as_bytes().to_vec()),
            ("002".to_string(), "elephant".as_bytes().to_vec()),
            ("003".to_string(), "four".as_bytes().to_vec()),
            ("004".to_string(), "avengers".as_bytes().to_vec()),
            ("005".to_string(), "bing".as_bytes().to_vec()),
            ("006".to_string(), "center".as_bytes().to_vec()),
            ("007".to_string(), "center".as_bytes().to_vec()),
            ("008".to_string(), "bing".as_bytes().to_vec()),
            ("009".to_string(), "center".as_bytes().to_vec()),
            ("010".to_string(), "center".as_bytes().to_vec()),
            ("011".to_string(), "derby".as_bytes().to_vec()),
            ("012".to_string(), "elephant".as_bytes().to_vec()),
            ("013".to_string(), "four".as_bytes().to_vec()),
            ("014".to_string(), "avengers".as_bytes().to_vec()),
            ("015".to_string(), "bing".as_bytes().to_vec()),
            ("016".to_string(), "center".as_bytes().to_vec()),
            ("017".to_string(), "center".as_bytes().to_vec()),
            ("018".to_string(), "bing".as_bytes().to_vec()),
            ("019".to_string(), "center".as_bytes().to_vec()),
            ("020".to_string(), "center".as_bytes().to_vec()),
        ]);

        for (key, value) in &key_value_pairs {
            tree.insert(key.clone(), value.clone())?;
        }

        for (key, value) in &key_value_pairs {
            assert_eq!(tree.search(key.clone())?, Some(value.clone()));
        }

        assert!(!tree.is_empty()?);

        tree.delete("006".to_string())?;
        tree.delete("012".to_string())?;
        tree.delete("002".to_string())?;
        tree.delete("005".to_string())?;
        tree.delete("001".to_string())?;
        tree.delete("003".to_string())?;
        tree.delete("004".to_string())?;
        tree.delete("007".to_string())?;
        tree.delete("008".to_string())?;
        tree.delete("009".to_string())?;
        tree.delete("010".to_string())?;
        tree.delete("011".to_string())?;
        tree.delete("018".to_string())?;
        tree.delete("019".to_string())?;
        tree.delete("017".to_string())?;
        tree.delete("020".to_string())?;
        tree.delete("014".to_string())?;
        tree.delete("015".to_string())?;
        tree.delete("016".to_string())?;
        tree.delete("013".to_string())?;

        assert!(tree.is_empty()?);

        Ok(())
    }

    #[test]
    fn delete_works() -> anyhow::Result<()> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open("/tmp/delete_works.ldb")
            .unwrap();

        let mut tree = BPTree::new(4, STARTUP_OFFSET, file);

        let key_value_pairs = BTreeMap::from([
            ("d".to_string(), "derby".as_bytes().to_vec()),
            ("e".to_string(), "elephant".as_bytes().to_vec()),
            ("f".to_string(), "four".as_bytes().to_vec()),
            ("a".to_string(), "avengers".as_bytes().to_vec()),
            ("b".to_string(), "bing".as_bytes().to_vec()),
            ("c".to_string(), "center".as_bytes().to_vec()),
            ("g".to_string(), "gover".as_bytes().to_vec()),
        ]);

        for (key, value) in &key_value_pairs {
            tree.insert(key.clone(), value.clone())?;
        }

        for (key, value) in &key_value_pairs {
            assert_eq!(tree.search(key.clone())?, Some(value.clone()));
        }

        let keys_for_delete = vec![
            "f".to_string(),
            "e".to_string(),
            "c".to_string(),
            "a".to_string(),
            "b".to_string(),
            "d".to_string(),
            "g".to_string(),
        ];

        let mut deleted_keys = HashSet::new();

        for key in &keys_for_delete {
            tree.delete(key.clone())?;
            assert_eq!(tree.search(key.clone())?, None);
            deleted_keys.insert(key.clone());

            for (initial_key, value) in &key_value_pairs {
                if !deleted_keys.contains(initial_key) {
                    assert_eq!(tree.search(initial_key.clone())?, Some(value.clone()));
                }
            }
        }

        assert!(tree.is_empty()?);
        Ok(())
    }
}
