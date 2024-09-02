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
    is_dummy: bool,
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
                    Some(self.split(pager))
                } else {
                    None
                }
            }
        }
    }

    fn split(
        &mut self,
        pager: &mut Pager,
    ) -> (Key, Node) {
        let split_index = self.keys.len() / 2;
        let mut sibling_keys = self.keys.split_off(split_index);
        let median_key = sibling_keys.remove(0);

        let new_internal_node = InternalNode {
            is_dummy: false,
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
        let need_rebalance = child_node.delete(pager, key, degree);
        pager.write_at(&child_node, child_offset).unwrap();

        match need_rebalance {
            None => None,
            Some(value) => {
                if value {
                    Some(self.rebalance(pager, position, degree))
                } else {
                    Some(false)
                }
            },
        }

    }

    fn rebalance(&mut self, pager: &mut Pager, index: usize, degree: usize) -> bool {        
        // Реализуй эту функцию
    }

    fn search(&self, pager: &mut Pager, key: Key) -> Option<Value> {
        let position = self.keys.binary_search(&key).unwrap_or_else(|pos| pos);
        let child_offset = self.children[position];
        let child_node = pager.read(child_offset).unwrap();
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
                            is_dummy: false,
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
