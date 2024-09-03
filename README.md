# B+Tree in rust

This is B+tree implementation for my education process of storage engines.

> **ATTENTION** this is not lib or prod ready solution, this is just entrypoint for futher research of storage engines that based on trees.

- No WAL
- No append-only mechanism
- No copy-on-write mechanism
- No transactions, acid, etc

This tree is used disk for store blocks and showed the basic concept of tree building, searching and rebalancing. I don't plan to implement other features, maybe in the future.

You can run demo main `cargo run` or tests `cargo tests`.

Enjoy your education!
