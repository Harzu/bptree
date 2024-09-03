#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions, clippy::cast_possible_truncation)]

mod bptree;

use bptree::BPTree;
use std::{collections::BTreeMap, fs::OpenOptions};

fn main() {
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open("/tmp/delete_works.ldb")
        .unwrap();

    let mut tree = BPTree::new(4, 0, file);

    let key_value_pairs = BTreeMap::from([
        ("001".to_string(), "derby".to_string()),
        ("002".to_string(), "elephant".to_string()),
        ("003".to_string(), "four".to_string()),
        ("004".to_string(), "avengers".to_string()),
        ("005".to_string(), "bing".to_string()),
        ("006".to_string(), "center".to_string()),
        ("007".to_string(), "center".to_string()),
        ("008".to_string(), "bing".to_string()),
        ("009".to_string(), "center".to_string()),
        ("010".to_string(), "center".to_string()),
        ("011".to_string(), "derby".to_string()),
        ("012".to_string(), "elephant".to_string()),
        ("013".to_string(), "four".to_string()),
        ("014".to_string(), "avengers".to_string()),
        ("015".to_string(), "bing".to_string()),
        ("016".to_string(), "center".to_string()),
        ("017".to_string(), "center".to_string()),
        ("018".to_string(), "bing".to_string()),
        ("019".to_string(), "center".to_string()),
        ("020".to_string(), "center".to_string()),
    ]);

    for (key, value) in &key_value_pairs {
        tree.insert(key.clone(), value.clone());
    }

    for (key, value) in &key_value_pairs {
        assert_eq!(tree.search(key.clone()), Some(value.clone()));
    }

    println!("Tree is full");
    assert!(!tree.is_empty());
    tree.debug_print();

    tree.delete("006".to_string());
    tree.delete("012".to_string());
    tree.delete("002".to_string());
    tree.delete("005".to_string());
    tree.delete("001".to_string());
    tree.delete("003".to_string());
    tree.delete("004".to_string());
    tree.delete("007".to_string());
    tree.delete("008".to_string());
    tree.delete("009".to_string());
    tree.delete("010".to_string());
    tree.delete("011".to_string());
    tree.delete("018".to_string());
    tree.delete("019".to_string());
    tree.delete("017".to_string());
    tree.delete("020".to_string());
    tree.delete("014".to_string());
    tree.delete("015".to_string());
    tree.delete("016".to_string());
    tree.delete("013".to_string());

    println!("Tree is empty");
    assert!(tree.is_empty());
    tree.debug_print();
}
