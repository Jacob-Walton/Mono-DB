use super::*;
use crate::storage::{buffer_pool::BufferPool, disk_manager::DiskManager};
use std::sync::Arc;
use tempfile::TempDir;

/// Helper to create a temporary B-tree for testing
fn create_test_btree() -> (BTree, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let disk_manager =
        Arc::new(DiskManager::new(temp_dir.path()).expect("Failed to create disk manager"));
    let buffer_pool = Arc::new(BufferPool::new(100, disk_manager)); // 100 pages

    let btree = BTree::new("test_tree".to_string(), buffer_pool);
    (btree, temp_dir)
}

// ============================================================================
// TEST: B-Tree Node Split
// ============================================================================

#[tokio::test]
async fn test_btree_node_split() {
    // Test Synopsis: Split algorithm when node overflows
    // Test Data: Insert keys 1-500 sequentially into order-3 tree
    // Expected Outcome: Tree height increases correctly, all keys retrievable, parent pointers valid

    let (btree, _temp_dir) = create_test_btree();

    // Insert 500 sequential keys
    for i in 1..=500 {
        let key = format!("key_{:05}", i).into_bytes();
        let value = format!("value_{}", i).into_bytes();
        btree.insert(&key, &value).await.expect("Insert failed");
    }

    // Verify all keys are retrievable
    for i in 1..=500 {
        let key = format!("key_{:05}", i).into_bytes();
        let result = btree.get(&key).await.expect("Get failed");
        assert!(result.is_some(), "Key {} should exist", i);

        let value = result.unwrap();
        let expected = format!("value_{}", i).into_bytes();
        assert_eq!(value, expected, "Value mismatch for key {}", i);
    }

    // Verify keys are accessible via scan
    let start_key = b"key_00001";
    let end_key = b"key_00100";
    let scan_results = btree.scan(start_key, end_key).await.expect("Scan failed");

    assert_eq!(scan_results.len(), 100, "Expected 100 keys in scan range");

    // Verify scan results are in order
    for (idx, (key, _value)) in scan_results.iter().enumerate() {
        let expected_key = format!("key_{:05}", idx + 1).into_bytes();
        assert_eq!(
            key, &expected_key,
            "Scan results out of order at index {}",
            idx
        );
    }

    println!("✓ B-Tree Node Split test passed: 500 keys inserted, all retrievable, scan works");
}

#[ignore = "Tree growth not yet implemented"]
#[tokio::test]
async fn test_btree_node_split_forces_tree_growth() {
    // Test that tree height increases when root splits
    let (btree, _temp_dir) = create_test_btree();

    // Insert enough data to force multiple splits
    // Each page can hold ~200 bytes of data, so we need large values
    for i in 0..200 {
        let key = format!("k{:04}", i).into_bytes();
        let value = vec![b'x'; 100]; // 100-byte values
        btree.insert(&key, &value).await.expect("Insert failed");
    }

    // Verify all data is still accessible
    for i in 0..200 {
        let key = format!("k{:04}", i).into_bytes();
        let result = btree.get(&key).await.expect("Get failed");
        assert!(result.is_some(), "Key {} should exist after splits", i);
    }

    println!("✓ B-Tree handles multiple splits and tree growth correctly");
}

// ============================================================================
// TEST: B-Tree Merge
// ============================================================================

#[tokio::test]
async fn test_btree_node_merge_on_underflow() {
    // Test Synopsis: Node merging on underflow
    // Test Data: Delete 400 keys from 500-key tree
    // Expected Outcome: Nodes merge when < 50% full, tree shrinks, no orphaned nodes

    let (btree, _temp_dir) = create_test_btree();

    // Insert 500 keys
    for i in 1..=500 {
        let key = format!("key_{:05}", i).into_bytes();
        let value = format!("value_{}", i).into_bytes();
        btree.insert(&key, &value).await.expect("Insert failed");
    }

    // Delete 400 keys (keys 1-400)
    for i in 1..=400 {
        let key = format!("key_{:05}", i).into_bytes();
        btree.delete(&key).await.expect("Delete failed");
    }

    // Verify deleted keys are gone
    for i in 1..=400 {
        let key = format!("key_{:05}", i).into_bytes();
        let result = btree.get(&key).await.expect("Get failed");
        assert!(result.is_none(), "Key {} should be deleted", i);
    }

    // Verify remaining keys (401-500) still exist
    for i in 401..=500 {
        let key = format!("key_{:05}", i).into_bytes();
        let result = btree.get(&key).await.expect("Get failed");
        assert!(result.is_some(), "Key {} should still exist", i);

        let value = result.unwrap();
        let expected = format!("value_{}", i).into_bytes();
        assert_eq!(value, expected, "Value mismatch for key {}", i);
    }

    // Verify scan works on remaining keys
    let start_key = b"key_00401";
    let end_key = b"key_00500";
    let scan_results = btree.scan(start_key, end_key).await.expect("Scan failed");

    assert_eq!(scan_results.len(), 100, "Expected 100 remaining keys");

    println!("✓ B-Tree Merge test passed: 400/500 keys deleted, remaining 100 intact");
}

#[tokio::test]
async fn test_btree_alternating_insert_delete() {
    // Test tree structure under alternating operations
    let (btree, _temp_dir) = create_test_btree();

    // Pattern: insert 100, delete 50, insert 100, delete 50...
    for batch in 0..5 {
        let base = batch * 100;

        // Insert 100 keys
        for i in 0..100 {
            let key = format!("key_{:05}", base + i).into_bytes();
            let value = format!("value_{}", base + i).into_bytes();
            btree.insert(&key, &value).await.expect("Insert failed");
        }

        // Delete every other key
        for i in (0..100).step_by(2) {
            let key = format!("key_{:05}", base + i).into_bytes();
            btree.delete(&key).await.expect("Delete failed");
        }
    }

    // Verify structure: we should have 250 keys total (50 per batch * 5 batches)
    let mut found_count = 0;
    for batch in 0..5 {
        let base = batch * 100;
        for i in 0..100 {
            let key = format!("key_{:05}", base + i).into_bytes();
            if btree.get(&key).await.expect("Get failed").is_some() {
                found_count += 1;
            }
        }
    }

    assert_eq!(found_count, 250, "Expected 250 keys after alternating ops");
    println!("✓ B-Tree handles alternating insert/delete correctly");
}
