use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use bincode::{Decode, Encode};

const PAGE_SIZE: usize = 4096;
const HEADER_SIZE: usize = PAGE_SIZE;
const STARTUP_OFFSET: usize = HEADER_SIZE + 20;

type Key = String;
type Value = String;
type Offset = usize;

#[derive(Clone, Debug, Encode, Decode)]
enum Node {
    Leaf(LeafNode),
    Internal(InternalNode),
}

impl Node {
    fn can_borrow(&self, degree: usize) -> bool {
        match self {
            Node::Leaf(leaf_node) => leaf_node.keys.len() > (degree / 2) - 1,
            Node::Internal(internal_node) => internal_node.keys.len() > (degree / 2) - 1,
        }
    }

    fn insert(
        &mut self,
        pager: &mut Pager,
        key: Key,
        value: Value,
        degree: usize
    ) -> Option<(Key, Node)> {
        match self {
            Node::Leaf(leaf_node) => match leaf_node.insert(pager, key, value, degree) {
                None => None,
                Some(new_item) => Some((new_item.0, Node::Leaf(new_item.1)))
            },
            Node::Internal(internal_node) => match internal_node.insert(pager, key, value, degree) {
                None => None,
                Some(new_item) => Some((new_item.0, new_item.1))
            },
        }
    }

    fn delete(
        &mut self,
        pager: &mut Pager,
        key: Key,
        degree: usize,
    ) -> Option<bool> {
        match self {
            Node::Leaf(leaf_node) => leaf_node.delete(key, degree),
            Node::Internal(internal_node) => internal_node.delete(pager, key, degree),
        }
    }

    fn search(&self, pager: &mut Pager, key: Key) -> Option<Value> {
        match self {
            Node::Leaf(leaf_node) => leaf_node.search(key),
            Node::Internal(internal_node) => internal_node.search(pager, key),
        }
    }

    fn debug_print(&self, pager: &mut Pager, level: usize) {
        match self {
            Node::Leaf(leaf_node) => leaf_node.debug_print(level),
            Node::Internal(internal_node) => internal_node.debug_print(pager, level),
        }
    }
}

#[derive(Clone, Debug, Encode, Decode)]
struct LeafNode {
    keys: Vec<Key>,
    values: Vec<Value>,
    offset: Option<usize>,
}

impl LeafNode {
    fn insert(
        &mut self,
        pager: &mut Pager,
        key: Key,
        value: Value,
        degree: usize,
    ) -> Option<(Key, LeafNode)> {
        let position = self.keys.binary_search(&key).unwrap_or_else(|pos| pos);
        self.keys.insert(position, key);
        self.values.insert(position, value);

        if self.keys.len() > degree - 1 {
            Some(self.split(pager, degree))
        } else {
            None
        }
    }

    fn split(
        &mut self,
        pager: &mut Pager,
        degree: usize
    ) -> (Key, LeafNode) {
        // Есть более хорошие методы разделения, но этот стандартный
        let split_index = self.keys.len() / 2;
        // Ключ который должен стать внутренним узлом
        let mid_key = self.keys[split_index - 1].clone();

        let new_leaf_node = LeafNode {
            keys: self.keys.split_off(split_index),
            values: self.values.split_off(split_index),
            offset: Some(pager.next_offset()),
        };

        (mid_key, new_leaf_node)
    }

    fn delete(&mut self, key: Key, degree: usize) -> Option<bool> {
        match self.keys.binary_search(&key) {
            Err(_) => None,
            Ok(position) => {
                self.keys.remove(position);
                self.values.remove(position);
                Some(self.keys.len() < (degree / 2) - 1)
            }
        }
    }

    fn search(&self, key: Key) -> Option<Value> {
        let position = self.keys.binary_search(&key).unwrap_or_else(|pos| pos);
        // self.values.get(position)
        // match self.keys.binary_search(&key) {
        //     Ok(position) => Some(self.values[position].clone()),
        //     Err(_) => None
        // }

        // println!("{:?} {}", self, position);
        match self.values.get(position) {
            None => None,
            Some(value) => Some(value.clone()),
        }
    }

    fn debug_print(&self, level: usize) {
        let indent = "  ".repeat(level);
        println!("{}LeafNode: {:?} keys = {:?}, values = {:?}", indent, self.offset, self.keys, self.values);
    }
}

#[derive(Clone, Debug, Encode, Decode)]
struct InternalNode {
    keys: Vec<Key>,
    children: Vec<usize>,
    offset: Option<usize>,
}

impl InternalNode {
    fn insert(
        &mut self,
        pager: &mut Pager,
        key: Key,
        value: Value,
        degree: usize,
    ) -> Option<(Key, Node)> {
        let position = self.keys.binary_search(&key).unwrap_or_else(|pos| pos);
        let child_offset = self.children[position];
        let mut child_node = pager.read(child_offset).unwrap();
        
        match child_node.insert(pager, key, value, degree) {
            None => {
                pager.write_at(&child_node, child_offset).unwrap();
                None
            },
            Some((mid_key, sibling)) => {
                let sibling_offset = pager.write(&sibling).unwrap();
                pager.write_at(&child_node, child_offset).unwrap();

                self.keys.insert(position, mid_key);
                self.children.insert(position.saturating_add(1), sibling_offset);

                if self.keys.len() > degree - 1 {
                    Some(self.split(pager, degree))
                } else {
                    None
                }
            }
        }
    }

    fn split(
        &mut self,
        pager: &mut Pager,
        degree: usize,
    ) -> (Key, Node) {
        let split_index = self.keys.len() / 2;
        let mut sibling_keys = self.keys.split_off(split_index);
        let median_key = sibling_keys.remove(0);

        let new_internal_node = InternalNode {
            keys: sibling_keys,
            children: self.children.split_off(split_index + 1),
            offset: Some(pager.next_offset()),
        };

        (median_key, Node::Internal(new_internal_node))
    }

    fn delete(
        &mut self,
        pager: &mut Pager,
        key: Key,
        degree: usize
    ) -> Option<bool> {
        let position = self.keys.binary_search(&key).unwrap_or_else(|pos| pos);
        let child_offset = self.children[position];
        let mut child_node = pager.read(child_offset).unwrap();

        match child_node.delete(pager, key, degree) {
            None => {
                pager.write_at(&child_node, child_offset).unwrap();
                None
            },
            Some(need_rebalance) => {
                pager.write_at(&child_node, child_offset).unwrap();
                if need_rebalance && self.rebalance(pager, position, degree) {
                    return Some(true)
                }

                Some(false)
            },
        }

    }

    fn rebalance(&mut self, pager: &mut Pager, index: usize, degree: usize) -> bool {        
        let child_offset = self.children[index];
        let mut child_node = pager.read(child_offset).unwrap();
    
        if index > 0 {
            let left_sibling_offset = self.children[index - 1];
            let mut left_sibling = pager.read(left_sibling_offset).unwrap();
    
            if left_sibling.can_borrow(degree) {
                match (&mut left_sibling, &mut child_node) {
                    (Node::Internal(ref mut sibling), Node::Internal(ref mut current)) => {
                        println!("1");
                        let borrowed_key = sibling.keys.pop().unwrap();
                        current.keys.insert(0, self.keys[index - 1].clone());
                        self.keys[index - 1] = borrowed_key;
    
                        let borrowed_child = sibling.children.pop().unwrap();
                        current.children.insert(0, borrowed_child);
    
                        pager.write_at(&left_sibling, left_sibling_offset).unwrap();
                        pager.write_at(&child_node, child_offset).unwrap();
                    },
                    (Node::Leaf(ref mut sibling), Node::Leaf(ref mut current)) => {
                        println!("2");
                        let borrowed_key = sibling.keys.pop().unwrap();
                        let borrowed_value = sibling.values.pop().unwrap();
                        current.keys.insert(0, borrowed_key);
                        current.values.insert(0, borrowed_value);
    
                        self.keys[index - 1] = current.keys[0].clone();
    
                        pager.write_at(&left_sibling, left_sibling_offset).unwrap();
                        pager.write_at(&child_node, child_offset).unwrap();
                    },
                    _ => unreachable!(),
                }
                return false;
            }
        }
        
        if index < self.children.len() - 1 {
            let right_sibling_offset = self.children[index + 1];
            let mut right_sibling = pager.read(right_sibling_offset).unwrap();
    
            if right_sibling.can_borrow(degree) {
                match (&mut right_sibling, &mut child_node) {
                    (Node::Internal(ref mut sibling), Node::Internal(ref mut current)) => {
                        println!("3");
                        let borrowed_key = sibling.keys.remove(0);
                        current.keys.push(self.keys[index].clone());
                        self.keys[index] = borrowed_key;
    
                        let borrowed_child = sibling.children.remove(0);
                        current.children.push(borrowed_child);
    
                        pager.write_at(&right_sibling, right_sibling_offset).unwrap();
                        pager.write_at(&child_node, child_offset).unwrap();
                    },
                    (Node::Leaf(ref mut sibling), Node::Leaf(ref mut current)) => {
                        println!("4");
                        let borrowed_key = sibling.keys.remove(0);
                        let borrowed_value = sibling.values.remove(0);
                        current.keys.push(borrowed_key);
                        current.values.push(borrowed_value);
    
                        self.keys[index] = sibling.keys[0].clone();
    
                        pager.write_at(&right_sibling, right_sibling_offset).unwrap();
                        pager.write_at(&child_node, child_offset).unwrap();
                    },
                    _ => unreachable!(),
                }
                return false;
            }
        }
    
        if index > 0 {
            let left_sibling_offset = self.children[index - 1];
            let mut left_sibling = pager.read(left_sibling_offset).unwrap();
            match (&mut left_sibling, &mut child_node) {
                (Node::Internal(ref mut sibling), Node::Internal(ref mut current)) => {                    
                    println!("5");
                    sibling.keys.push(self.keys.remove(index - 1));
                    sibling.keys.append(&mut current.keys);
                    sibling.children.append(&mut current.children);
    
                    self.children.remove(index);
    
                    pager.write_at(&left_sibling, left_sibling_offset).unwrap();
                    pager.write_at(&child_node, child_offset).unwrap();
                },
                (Node::Leaf(ref mut sibling), Node::Leaf(ref mut current)) => {
                    println!("6");
                    sibling.keys.append(&mut current.keys);
                    sibling.values.append(&mut current.values);
    
                    if index <= self.keys.len() - 1 {
                        self.keys.remove(index);
                    }
                    self.children.remove(index);

                    pager.write_at(&left_sibling, left_sibling_offset).unwrap();
                    pager.write_at(&child_node, child_offset).unwrap();
                },
                _ => unreachable!(),
            }
        } else {
            let right_sibling_offset = self.children[index + 1];
            let mut right_sibling = pager.read(right_sibling_offset).unwrap();
            match (&mut child_node, &mut right_sibling) {
                (Node::Internal(ref mut current), Node::Internal(ref mut sibling)) => {
                    println!("7 {:?} {:?} {:?}", self, current, sibling);
                    current.keys.push(self.keys.remove(index));
                    current.keys.append(&mut sibling.keys);
                    current.children.append(&mut sibling.children);
                    // self.keys.remove(index);
                    self.children.remove(index + 1);
    
                    pager.write_at(&child_node, child_offset).unwrap();
                    pager.write_at(&right_sibling, right_sibling_offset).unwrap();
                },
                (Node::Leaf(ref mut current), Node::Leaf(ref mut sibling)) => {
                    println!("8 {:?} {:?} {:?}", self, current, sibling);
                    current.keys.append(&mut sibling.keys);
                    current.values.append(&mut sibling.values);
    
                    self.keys.remove(index);
                    self.children.remove(index + 1);

                    pager.write_at(&child_node, child_offset).unwrap();
                    pager.write_at(&right_sibling, right_sibling_offset).unwrap();
                },
                _ => unreachable!(),
            }
        }
    
        true
    }

    fn search(&self, pager: &mut Pager, key: Key) -> Option<Value> {
        let position = self.keys.binary_search(&key).unwrap_or_else(|pos| pos);
        let child_offset = self.children[position];
        let child_node = pager.read(child_offset).unwrap();
        // println!("{:?} {}", self, position);
        child_node.search(pager, key)
    }

    fn debug_print(&self, pager: &mut Pager, level: usize) {
        let indent = "  ".repeat(level);
        println!("{}InternalNode: {:?} keys = {:?}, children = {:?}", indent, self.offset, self.keys, self.children);
        for (i, child_offset) in self.children.iter().enumerate() {
            println!("{}  Child {}:", indent, i);
            let child = pager.read(*child_offset).unwrap();
            child.debug_print(pager, level + 1);
        }
    }
}

#[derive(Debug)]
pub struct BPTree {
    degree: usize,
    pager: Pager,
    root_node: Option<usize>,
}

impl BPTree {
    pub fn new(degree: usize, startup_offset: usize, file: File) -> Self {
        Self {
            degree,
            pager: Pager::new(file, startup_offset),
            root_node: None,
        }
    }

    pub fn insert(&mut self, key: Key, value: Value) {
        match self.root_node.take() {
            None => {
                let root_node = Node::Leaf(LeafNode {
                    keys: vec![key],
                    values: vec![value],
                    offset: Some(self.pager.next_offset()),
                });
                let root_offset = self.pager.write(&root_node).unwrap();
                self.root_node = Some(root_offset);
            },
            Some(root_offset) => {
                let mut root_node = self.pager.read(root_offset).unwrap();

                match root_node.insert(&mut self.pager, key, value, self.degree) {
                    None => {
                        self.pager.write_at(&mut root_node, root_offset).unwrap();
                        self.root_node = Some(root_offset);
                    },
                    Some((mid_key, sibling)) => {
                        let sibling_offset = self.pager.write(&sibling).unwrap();

                        let new_root = Node::Internal(InternalNode {
                            keys: vec![mid_key],
                            children: vec![root_offset, sibling_offset],
                            offset: Some(self.pager.next_offset()),
                        });

                        let new_root_offset = self.pager.write(&new_root).unwrap();
                        self.pager.write_at(&mut root_node, root_offset).unwrap();
                        self.pager.write_at(&sibling, sibling_offset).unwrap();
                        self.root_node = Some(new_root_offset);
                    },
                }
            }
        }
    }

    pub fn delete(&mut self, key: Key) {
        match self.root_node.take() {
            None => {},
            Some(root_offset) => {
                let mut root_node = self.pager.read(root_offset).unwrap();
                match root_node.delete(&mut self.pager, key, self.degree) {
                    None => {
                        self.pager.write_at(&root_node, root_offset).unwrap();
                        self.root_node = Some(root_offset);
                    },
                    Some(need_rebalance) => {       
                        self.pager.write_at(&root_node, root_offset).unwrap();
                 
                        if need_rebalance {
                            match root_node {
                                Node::Leaf(_) => {
                                    self.root_node = Some(root_offset);     
                                },
                                Node::Internal(payload) => {
                                    if payload.keys.is_empty() {
                                        self.root_node = Some(payload.children[0])
                                    } else {
                                        self.root_node = Some(root_offset)
                                    }
                                }
                            }
                        } else {
                            self.root_node = Some(root_offset);
                        }
                    }
                }
            }
        }
    }

    pub fn search(&mut self, key: Key) -> Option<Value> {
        match self.root_node.take() {
            None => None,
            Some(root_offset) => {
                let root_node = self.pager.read(root_offset).unwrap();
                self.root_node = Some(root_offset);
                root_node.search(&mut self.pager, key)
            }
        }
    }

    pub fn debug_print(&mut self) {
        if let Some(node_offset) = self.root_node.clone() {
            let node = self.pager.read(node_offset).unwrap();
            node.debug_print(&mut self.pager, 0);
        }
    }
}

#[derive(Debug)]
struct Pager {
    file: File,
    cursor: usize,
}

impl Pager {
    fn new(file: File, startup_offset: usize) -> Self {
        Self {
            file,
            cursor: startup_offset,
        }
    }

    fn next_offset(&self) -> usize {
        self.cursor
    }

    fn read(&mut self, offset: usize) -> anyhow::Result<Node> {
        self.file.seek(SeekFrom::Start(offset as u64))?;
        let mut buffer: [u8; PAGE_SIZE] = [0x00; PAGE_SIZE];
        self.file.read(&mut buffer)?;
        let encoder_config = bincode::config::standard();
        let (node, _) = bincode::decode_from_slice(&buffer, encoder_config)?;
        Ok(node)
    }

    fn write(&mut self, node: &Node) -> anyhow::Result<usize> {
        let encoder_config = bincode::config::standard();
        let offset = self.file.seek(SeekFrom::Start((self.cursor) as u64))?;
        let data: Vec<u8> = bincode::encode_to_vec(&node, encoder_config)?;
        self.file.write_all(data.as_slice())?;
        self.cursor += PAGE_SIZE;
        Ok(offset as usize)
    }

    fn write_at(&mut self, node: &Node, offset: usize) -> anyhow::Result<()> {
        let encoder_config = bincode::config::standard();
        let _ = self.file.seek(SeekFrom::Start(offset as u64))?;
        let data: Vec<u8> = bincode::encode_to_vec(&node, encoder_config)?;
        self.file.write_all(data.as_slice())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, fs::OpenOptions};

    use super::*;

    #[test]
    // fn test_tree_structure() {
    //     let file = OpenOptions::new()
    //         .create(true)
    //         .read(true)
    //         .write(true)
    //         .truncate(true)
    //         .open("/tmp/test_tree_structure.ldb")
    //         .unwrap();

    //     let mut tree = BPTree::new(2, STARTUP_OFFSET, file);

    //     tree.insert("10".to_string(), "ten".to_string());
    //     tree.insert("20".to_string(), "twenty".to_string());
    //     tree.insert("5".to_string(), "five".to_string());
    //     tree.insert("6".to_string(), "six".to_string());
    //     tree.insert("12".to_string(), "twelve".to_string());
    //     tree.insert("30".to_string(), "thirty".to_string());
    //     tree.insert("7".to_string(), "seven".to_string());
    //     tree.insert("17".to_string(), "seventeen".to_string());

    //     assert_eq!(tree.search("10".to_string()), Some("ten".to_string()));
    //     assert_eq!(tree.search("20".to_string()), Some("twenty".to_string()));
    //     assert_eq!(tree.search("5".to_string()), Some("five".to_string()));
    //     assert_eq!(tree.search("6".to_string()), Some("six".to_string()));
    //     assert_eq!(tree.search("12".to_string()), Some("twelve".to_string()));
    //     assert_eq!(tree.search("30".to_string()), Some("thirty".to_string()));
    //     assert_eq!(tree.search("7".to_string()), Some("seven".to_string()));
    //     assert_eq!(tree.search("17".to_string()), Some("seventeen".to_string()));

    //     assert_eq!(tree.search("2000".to_string()), None);
    //     assert_eq!(tree.search("3000".to_string()), None);
    // }

    // #[test]
    // fn test_large_insertions() {
    //     let file = OpenOptions::new()
    //         .create(true)
    //         .read(true)
    //         .write(true)
    //         .truncate(true)
    //         .open("/tmp/test_large_insertions.ldb")
    //         .unwrap();

    //     let mut tree = BPTree::new(2, STARTUP_OFFSET, file);

    //     for i in 1..=10000 {
    //         tree.insert(i.to_string(), i.to_string());
    //     }

    //     for i in 1..=10000 {
    //         assert_eq!(tree.search(i.to_string()), Some(i.to_string()));
    //     }
    // }

    #[test]
    fn delete_works() {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open("/tmp/delete_works.ldb")
            .unwrap();

        let mut tree = BPTree::new(2, STARTUP_OFFSET, file);

        let key_value_pairs = BTreeMap::from([
            ("d".to_string(), "derby".to_string()),
            ("e".to_string(), "elephant".to_string()),
            ("f".to_string(), "four".to_string()),
            ("a".to_string(), "avengers".to_string()),
            ("b".to_string(), "bing".to_string()),
            ("c".to_string(), "center".to_string()),
            ("g".to_string(), "center".to_string()),
        ]);

        for (key, value) in &key_value_pairs {
            tree.insert(key.clone(), value.clone());
        }

        // for (key, value) in &key_value_pairs {
        //     assert_eq!(tree.search(key.clone()), Some(value.clone()));
        // }

        tree.delete("d".to_string());
        assert_eq!(tree.search("d".to_string()), None);

        // tree.delete("c".to_string());
        // assert_eq!(tree.search("c".to_string()), None);

        // tree.delete("f".to_string());
        // assert_eq!(tree.search("f".to_string()), None);

        tree.debug_print();
    }
}
