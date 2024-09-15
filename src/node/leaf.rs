use bincode::{Decode, Encode};
use crate::tree::{Key, Value};
use crate::pager::{PageOperator, Offset};

#[derive(Clone, Debug, Encode, Decode)]
pub(crate) struct LeafNode {
    pub keys: Vec<Key>,
    pub values: Vec<Value>,
    pub offset: Option<Offset>,
}

impl LeafNode {
    pub(crate) fn insert(
        &mut self,
        pager: &mut Box<dyn PageOperator>,
        key: Key,
        value: Value,
        degree: usize,
    ) -> Option<(Key, LeafNode)> {
        let position = self.keys.binary_search(&key).unwrap_or_else(|pos| pos);
        self.keys.insert(position, key);
        self.values.insert(position, value);

        if self.keys.len() > degree - 1 {
            Some(self.split(pager))
        } else {
            None
        }
    }

    fn split(&mut self, pager: &mut Box<dyn PageOperator>) -> (Key, LeafNode) {
        let split_index = self.keys.len() / 2;
        let mid_key = self.keys[split_index - 1].clone();

        let new_leaf_node = LeafNode {
            keys: self.keys.split_off(split_index),
            values: self.values.split_off(split_index),
            offset: Some(pager.next_offset()),
        };

        (mid_key, new_leaf_node)
    }

    pub(crate) fn remove(&mut self, key: Key, degree: usize) -> Option<bool> {
        match self.keys.binary_search(&key) {
            Err(_) => None,
            Ok(position) => {
                self.keys.remove(position);
                self.values.remove(position);
                Some(self.keys.len() < (degree / 2))
            },
        }
    }

    pub(crate) fn search(&self, key: Key) -> Option<Value> {
        match self.keys.binary_search(&key) {
            Err(_) => None,
            Ok(position) => Some(self.values[position].clone()),
        }
    }

    pub(crate) fn debug_print(&self, level: usize) {
        let indent = "  ".repeat(level);
        println!(
            "{}LeafNode: {:?} keys = {:?}, values = {:?}",
            indent, self.offset, self.keys, self.values
        );
    }
}
