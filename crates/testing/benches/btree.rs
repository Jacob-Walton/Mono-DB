use std::hint::black_box;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use testing::BTree;

const INSERT_SIZE: u64 = 100_000;
const LOOKUP_SIZE: u64 = 500_000;
const LOOKUP_QUERIES: usize = 10_000;

fn build_tree(size: u64) -> BTree<u64, u64> {
    let mut tree = BTree::new();
    for i in 0..size {
        tree.insert(i, i);
    }
    tree
}

fn bench_insert(c: &mut Criterion) {
    c.bench_function("btree_insert_sequential_100k", |b| {
        b.iter_batched(
            BTree::<u64, u64>::new,
            |mut tree| {
                for i in 0..INSERT_SIZE {
                    tree.insert(i, i);
                }
                black_box(tree);
            },
            BatchSize::LargeInput,
        );
    });
}

fn bench_lookup(c: &mut Criterion) {
    let tree = build_tree(LOOKUP_SIZE);
    let keys: Vec<u64> = (0..LOOKUP_QUERIES as u64)
        .map(|k| (k * 7) % LOOKUP_SIZE)
        .collect();

    c.bench_function("btree_lookup_hits_10k", |b| {
        b.iter(|| {
            for &key in &keys {
                black_box(tree.get(&key));
            }
        });
    });
}

criterion_group!(btree_benches, bench_insert, bench_lookup);
criterion_main!(btree_benches);
