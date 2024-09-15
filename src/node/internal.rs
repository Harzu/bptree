use bincode::{Decode, Encode};
use super::Node;
use crate::tree::{Key, Value};
use crate::pager::{PageOperator, Offset};

#[derive(Clone, Debug, Encode, Decode)]
pub(crate) struct InternalNode {
    pub keys: Vec<Key>,
    pub children: Vec<Offset>,
    pub offset: Option<Offset>,
}

impl InternalNode {
    pub(crate) fn insert(
        &mut self,
        pager: &mut Box<dyn PageOperator>,
        key: Key,
        value: Value,
        degree: usize,
    ) -> anyhow::Result<Option<(Key, Node)>> {
        let position = self.keys.binary_search(&key).unwrap_or_else(|pos| pos);
        let child_offset = self.children[position];
        let mut child_node = pager.read(child_offset)?;
        let child_node_copy_offset = pager.write(&child_node)?;
        self.children[position] = child_node_copy_offset;

        let is_splitted = child_node.insert(pager, key, value, degree)?;
        pager.write_at(&child_node, child_node_copy_offset)?;

        match is_splitted {
            None => Ok(None),
            Some((mid_key, sibling)) => {
                let sibling_offset = pager.write(&sibling)?;
                self.keys.insert(position, mid_key);
                self.children.insert(position + 1, sibling_offset);

                if self.keys.len() > degree - 1 {
                    Ok(Some(self.split(pager)))
                } else {
                    Ok(None)
                }
            },
        }
    }

    fn split(&mut self, pager: &mut Box<dyn PageOperator>) -> (Key, Node) {
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

    pub(crate) fn remove(&mut self, pager: &mut Box<dyn PageOperator>, key: Key, degree: usize) -> anyhow::Result<Option<bool>> {
        let position = self.keys.binary_search(&key).unwrap_or_else(|pos| pos);
        let child_offset = self.children[position];
        let mut child_node = pager.read(child_offset)?;
        let child_node_copy_offset = pager.write(&child_node)?;
        self.children[position] = child_node_copy_offset;

        match child_node.remove(pager, key, degree)? {
            None => Ok(None),
            Some(need_rebalance) => {
                pager.write_at(&child_node, child_node_copy_offset)?;

                if need_rebalance {
                    Ok(Some(self.rebalance(pager, position, &mut child_node, degree)?))
                } else {
                    Ok(Some(false))
                }
            },
        }
    }

    fn rebalance(
        &mut self,
        pager: &mut Box<dyn PageOperator>,
        child_offset_position: usize,
        child_node: &mut Node,
        degree: usize
    ) -> anyhow::Result<bool> {
        let child_offset = self.children[child_offset_position];

        if child_offset_position > 0 {
            let left_sibling_offset = self.children[child_offset_position - 1];
            let mut left_sibling = pager.read(left_sibling_offset)?;
            let left_sibling_copy_offset = pager.write(&left_sibling)?;
            self.children[child_offset_position - 1] = left_sibling_copy_offset;

            if left_sibling.can_borrow(degree) {
                self.borrow_left(
                    pager,
                    child_offset_position,
                    &mut left_sibling,
                    left_sibling_copy_offset,
                    child_node,
                    child_offset
                )?;
                return Ok(false);
            }
        }

        if child_offset_position < self.children.len() - 1 {
            let right_sibling_offset = self.children[child_offset_position + 1];
            let mut right_sibling = pager.read(right_sibling_offset)?;
            let right_sibling_copy_offset = pager.write(&right_sibling)?;
            self.children[child_offset_position + 1] = right_sibling_copy_offset;

            if right_sibling.can_borrow(degree) {
                self.borrow_right(
                    pager,
                    child_offset_position,
                    &mut right_sibling,
                    right_sibling_copy_offset,
                    child_node,
                    child_offset,
                )?;
                return Ok(false);
            }
        }

        if child_offset_position > 0 {
            let left_sibling_offset = self.children[child_offset_position - 1];
            let mut left_sibling = pager.read(left_sibling_offset)?;
            let left_sibling_copy_offset = pager.write(&left_sibling)?;
            self.children[child_offset_position - 1] = left_sibling_copy_offset;

            self.merge_left(
                pager,
                child_offset_position,
                &mut left_sibling,
                left_sibling_copy_offset,
                child_node,
                child_offset
            )?;
        } else {
            let right_sibling_offset = self.children[child_offset_position + 1];
            let mut right_sibling = pager.read(right_sibling_offset)?;
            let right_sibling_copy_offset = pager.write(&right_sibling)?;
            self.children[child_offset_position + 1] = right_sibling_copy_offset;

            self.merge_right(
                pager,
                child_offset_position,
                &mut right_sibling,
                right_sibling_copy_offset,
                child_node,
                child_offset,
            )?;
        }

        Ok(self.keys.len() < (degree / 2))
    }

    fn borrow_left(
        &mut self,
        pager: &mut Box<dyn PageOperator>,
        index: usize,
        left_sibling: &mut Node,
        left_sibling_offset: Offset,
        child_node: &mut Node,
        child_offset: Offset,
    ) -> anyhow::Result<()> {
        match (left_sibling, child_node) {
            (Node::Internal(ref mut sibling), Node::Internal(ref mut current)) => {
                let borrowed_key = sibling.keys.pop().unwrap();
                current.keys.insert(0, self.keys[index - 1].clone());
                self.keys[index - 1] = borrowed_key;

                let borrowed_child = sibling.children.pop().unwrap();
                current.children.insert(0, borrowed_child);

                pager
                    .write_at(&Node::Internal(sibling.clone()), left_sibling_offset)?;
                pager
                    .write_at(&Node::Internal(current.clone()), child_offset)?;
            },
            (Node::Leaf(ref mut sibling), Node::Leaf(ref mut current)) => {
                let borrowed_key = sibling.keys.pop().unwrap();
                let borrowed_value = sibling.values.pop().unwrap();
                current.keys.insert(0, borrowed_key.clone());
                current.values.insert(0, borrowed_value);
                self.keys[index - 1].clone_from(&sibling.keys[0]);

                pager
                    .write_at(&Node::Leaf(sibling.clone()), left_sibling_offset)?;
                pager
                    .write_at(&Node::Leaf(current.clone()), child_offset)?;
            },
            _ => {},
        }

        Ok(())
    }

    fn borrow_right(
        &mut self,
        pager: &mut Box<dyn PageOperator>,
        index: usize,
        right_sibling: &mut Node,
        right_sibling_offset: Offset,
        child_node: &mut Node,
        child_offset: Offset,
    ) -> anyhow::Result<()> {
        match (right_sibling, child_node) {
            (Node::Internal(ref mut sibling), Node::Internal(ref mut current)) => {
                let borrowed_key = sibling.keys.remove(0);
                current.keys.push(self.keys[index].clone());
                self.keys[index] = borrowed_key;

                let borrowed_child = sibling.children.remove(0);
                current.children.push(borrowed_child);

                pager
                    .write_at(&Node::Internal(sibling.clone()), right_sibling_offset)?;
                pager
                    .write_at(&Node::Internal(current.clone()), child_offset)?;
            },
            (Node::Leaf(ref mut sibling), Node::Leaf(ref mut current)) => {
                let borrowed_key = sibling.keys.remove(0);
                let borrowed_value = sibling.values.remove(0);
                self.keys[index].clone_from(&borrowed_key);

                current.keys.push(borrowed_key);
                current.values.push(borrowed_value);

                pager
                    .write_at(&Node::Leaf(sibling.clone()), right_sibling_offset)?;
                pager
                    .write_at(&Node::Leaf(current.clone()), child_offset)?;
            },
            _ => {},
        }

        Ok(())
    }

    fn merge_left(
        &mut self,
        pager: &mut Box<dyn PageOperator>,
        index: usize,
        left_sibling: &mut Node,
        left_sibling_offset: Offset,
        child_node: &mut Node,
        child_offset: Offset,
    ) -> anyhow::Result<()> {
        match (left_sibling, child_node) {
            (Node::Internal(ref mut sibling), Node::Internal(ref mut current)) => {
                sibling.keys.push(self.keys.remove(index - 1));
                sibling.keys.append(&mut current.keys);
                sibling.children.append(&mut current.children);
                self.children.remove(index);

                pager
                    .write_at(&Node::Internal(sibling.clone()), left_sibling_offset)?;
                pager
                    .write_at(&Node::Internal(current.clone()), child_offset)?;
            },
            (Node::Leaf(ref mut sibling), Node::Leaf(ref mut current)) => {
                sibling.keys.append(&mut current.keys);
                sibling.values.append(&mut current.values);

                self.keys.remove(index - 1);
                self.children.remove(index);

                pager
                    .write_at(&Node::Leaf(sibling.clone()), left_sibling_offset)?;
                pager
                    .write_at(&Node::Leaf(current.clone()), child_offset)?;
            },
            _ => {},
        }

        Ok(())
    }

    fn merge_right(
        &mut self,
        pager: &mut Box<dyn PageOperator>,
        index: usize,
        right_sibling: &mut Node,
        right_sibling_offset: Offset,
        child_node: &mut Node,
        child_offset: Offset,
    ) -> anyhow::Result<()> {
        match (child_node, right_sibling) {
            (Node::Internal(ref mut current), Node::Internal(ref mut sibling)) => {
                current.keys.push(self.keys.remove(index));
                current.keys.append(&mut sibling.keys);
                current.children.append(&mut sibling.children);
                self.children.remove(index + 1);

                pager
                    .write_at(&Node::Internal(sibling.clone()), right_sibling_offset)?;
                pager
                    .write_at(&Node::Internal(current.clone()), child_offset)?;
            },
            (Node::Leaf(ref mut current), Node::Leaf(ref mut sibling)) => {
                current.keys.append(&mut sibling.keys);
                current.values.append(&mut sibling.values);

                self.keys.remove(index);
                self.children.remove(index + 1);

                pager
                    .write_at(&Node::Leaf(sibling.clone()), right_sibling_offset)?;
                pager
                    .write_at(&Node::Leaf(current.clone()), child_offset)?;
            },
            _ => {},
        }

        Ok(())
    }

    pub(crate) fn search(&self, pager: &mut Box<dyn PageOperator>, key: Key) -> anyhow::Result<Option<Value>> {
        let position = self.keys.binary_search(&key).unwrap_or_else(|pos| pos);
        let child_offset = self.children[position];
        let child_node = pager.read(child_offset)?;
        child_node.search(pager, key)
    }

    pub(crate) fn debug_print(&self, pager: &mut Box<dyn PageOperator>, level: usize) -> anyhow::Result<()> {
        let indent = "  ".repeat(level);
        println!(
            "{}InternalNode: {:?} keys = {:?}, children = {:?}",
            indent, self.offset, self.keys, self.children
        );
        for (i, child_offset) in self.children.iter().enumerate() {
            println!("{indent}  Child {i}:");
            let child = pager.read(*child_offset)?;
            let _ = child.debug_print(pager, level + 1);
        }

        Ok(())
    }
}
