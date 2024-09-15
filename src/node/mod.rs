pub(crate) mod leaf;
pub(crate) mod internal;

use bincode::{Decode, Encode};
use leaf::LeafNode;
use internal::InternalNode;
use crate::tree::{Key, Value};
use crate::pager::PageOperator;

#[derive(Clone, Debug, Encode, Decode)]
pub(crate) enum Node {
    Leaf(LeafNode),
    Internal(InternalNode),
}

impl Node {
    pub(crate) fn is_empty(&self) -> bool {
        match self {
            Node::Internal(payload) => {
                payload.keys.is_empty() && payload.children.is_empty()
            },
            Node::Leaf(payload) => payload.keys.is_empty() && payload.values.is_empty(),
        }
    }

    pub(crate) fn can_borrow(&self, degree: usize) -> bool {
        match self {
            Node::Leaf(leaf_node) => leaf_node.keys.len() >= (degree / 2),
            Node::Internal(internal_node) => internal_node.keys.len() >= (degree / 2),
        }
    }

    pub(crate) fn insert(
        &mut self,
        pager: &mut Box<dyn PageOperator>,
        key: Key,
        value: Value,
        degree: usize,
    ) -> anyhow::Result<Option<(Key, Node)>> {
        match self {
            Node::Leaf(leaf_node) => match leaf_node.insert(pager, key, value, degree) {
                None => Ok(None),
                Some(new_item) => Ok(Some((new_item.0, Node::Leaf(new_item.1)))),
            },
            Node::Internal(internal_node) => {
                match internal_node.insert(pager, key, value, degree)? {
                    None => Ok(None),
                    Some(new_item) => Ok(Some((new_item.0, new_item.1))),
                }
            },
        }
    }

    pub(crate) fn remove(&mut self, pager: &mut Box<dyn PageOperator>, key: Key, degree: usize) -> anyhow::Result<Option<bool>> {
        match self {
            Node::Leaf(leaf_node) => Ok(leaf_node.remove(key, degree)),
            Node::Internal(internal_node) => internal_node.remove(pager, key, degree),
        }
    }

    pub(crate) fn search(&self, pager: &mut Box<dyn PageOperator>, key: Key) -> anyhow::Result<Option<Value>> {
        match self {
            Node::Leaf(leaf_node) => Ok(leaf_node.search(key)),
            Node::Internal(internal_node) => internal_node.search(pager, key),
        }
    }

    pub(crate) fn debug_print(&self, pager: &mut Box<dyn PageOperator>, level: usize) -> anyhow::Result<()> {
        match self {
            Node::Leaf(leaf_node) => Ok(leaf_node.debug_print(level)),
            Node::Internal(internal_node) => internal_node.debug_print(pager, level),
        }
    }
}
