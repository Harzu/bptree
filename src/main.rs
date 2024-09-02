use std::{collections::BTreeMap, fs::OpenOptions};

mod bptree;

use bptree::BPTree;

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
        // ("d".to_string(), "derby".to_string()),
        // ("e".to_string(), "elephant".to_string()),
        // ("f".to_string(), "four".to_string()),
        // ("a".to_string(), "avengers".to_string()),
        // ("b".to_string(), "bing".to_string()),
        // ("c".to_string(), "center".to_string()),
        // ("g".to_string(), "center".to_string()),

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

    // for (key, value) in &key_value_pairs {
    //     assert_eq!(tree.search(key.clone()), Some(value.clone()));
    // }

    tree.delete("006".to_string());
    // tree.delete("012".to_string());
    // tree.delete("002".to_string());
    // tree.delete("005".to_string());
    // tree.delete("001".to_string());
    // tree.delete("003".to_string());
    // tree.delete("004".to_string());
    // tree.delete("007".to_string());
    // tree.delete("008".to_string());
    // tree.delete("009".to_string());
    // tree.delete("010".to_string());
    // tree.delete("011".to_string());
    // tree.delete("018".to_string());
    // tree.delete("019".to_string());
    // tree.delete("017".to_string());
    // tree.delete("020".to_string());
    // tree.delete("014".to_string());
    // tree.delete("015".to_string());
    // tree.delete("016".to_string());
    // tree.delete("013".to_string());

    tree.debug_print();
    // println!("{:?}", tree.search("020".to_string()));
    // assert_eq!(tree.search("020".to_string()), None);

    // tree.delete("c".to_string());
    // assert_eq!(tree.search("c".to_string()), None);

    // tree.delete("f".to_string());
    // assert_eq!(tree.search("f".to_string()), None);
}