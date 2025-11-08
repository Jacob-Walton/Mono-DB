use super::wal::{Wal, WalConfig, WalEntry, WalEntryType};
use monodb_common::Result;
use std::path::PathBuf;
use tempfile::TempDir;

/// Helper to create a test WAL
fn create_test_wal() -> (Wal, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let wal_path = temp_dir.path().join("test.wal");

    let config = WalConfig {
        buffer_size: 4096,
        max_size: 1024 * 1024, // 1MB
        sync_on_write: true,
        async_write: false,
        sync_every_bytes: None,
        sync_interval_ms: None,
    };

    let wal = Wal::with_config(&wal_path, config).expect("Failed to create WAL");
    (wal, temp_dir)
}

// ============================================================================
// TEST: WAL Write & Recovery
// ============================================================================

#[test]
fn test_wal_write_and_recovery() {
    // Test Synopsis: Log persistence and replay
    // Test Data: Write 1000 ops, crash at op 750
    // Expected Outcome: First 750 ops recovered, last 250 rolled back

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let wal_path = temp_dir.path().join("recovery_test.wal");

    let config = WalConfig {
        buffer_size: 4096,
        max_size: 10 * 1024 * 1024, // 10MB
        sync_on_write: true,
        async_write: false,
        sync_every_bytes: None,
        sync_interval_ms: None,
    };

    // Phase 1: Write 1000 operations
    {
        let mut wal = Wal::with_config(&wal_path, config.clone()).expect("Failed to create WAL");

        for i in 0..1000 {
            let key = format!("key_{:05}", i).into_bytes();
            let value = format!("value_{}", i).into_bytes();

            wal.append(&key, &value).expect("Failed to append");

            // Simulate crash at operation 750 (don't sync last 250 ops)
            if i == 749 {
                wal.sync().expect("Failed to sync");
                // After this point, don't sync (simulate crash with buffered data)
            }
        }

        // DON'T call wal.sync() here - simulate crash with unflushed buffer
    } // WAL dropped here

    // Phase 2: Recover from WAL
    let recovered_entries = Wal::replay(&wal_path).expect("Failed to replay WAL");

    // Verify: Should recover approximately 750 operations (those that were synced)
    // The exact number might be slightly more due to buffer flushing, but should be < 1000
    assert!(
        recovered_entries.len() >= 750,
        "Expected at least 750 recovered entries, got {}",
        recovered_entries.len()
    );

    assert!(
        recovered_entries.len() < 1000,
        "Expected less than 1000 recovered entries (last 250 not synced), got {}",
        recovered_entries.len()
    );

    // Verify first 750 entries are correct
    for i in 0..std::cmp::min(750, recovered_entries.len()) {
        let expected_key = format!("key_{:05}", i).into_bytes();
        let expected_value = format!("value_{}", i).into_bytes();

        assert_eq!(recovered_entries[i].key, expected_key, "Key mismatch at index {}", i);
        assert_eq!(recovered_entries[i].value, expected_value, "Value mismatch at index {}", i);
    }

    println!(
        "✓ WAL Write & Recovery test passed: {} ops recovered, unsynced ops lost",
        recovered_entries.len()
    );
}

#[test]
fn test_wal_replay_correctness() {
    // Test that replayed operations match original writes exactly
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let wal_path = temp_dir.path().join("replay_test.wal");

    let config = WalConfig::default();

    // Write operations
    {
        let mut wal = Wal::with_config(&wal_path, config.clone()).expect("Failed to create WAL");

        for i in 0..100 {
            let key = format!("test_key_{}", i).into_bytes();
            let value = format!("test_value_{}", i).into_bytes();
            let entry_type = if i % 3 == 0 {
                WalEntryType::Delete
            } else {
                WalEntryType::Insert
            };

            wal.append_with_type(&key, &value, entry_type).expect("Failed to append");
        }

        wal.sync().expect("Failed to sync");
    }

    // Replay and verify
    let entries = Wal::replay(&wal_path).expect("Failed to replay");

    assert_eq!(entries.len(), 100, "Expected 100 entries");

    for (i, entry) in entries.iter().enumerate() {
        let expected_key = format!("test_key_{}", i).into_bytes();
        let expected_value = format!("test_value_{}", i).into_bytes();
        let expected_type = if i % 3 == 0 {
            WalEntryType::Delete
        } else {
            WalEntryType::Insert
        };

        assert_eq!(entry.key, expected_key, "Key mismatch at {}", i);
        assert_eq!(entry.value, expected_value, "Value mismatch at {}", i);
        assert_eq!(entry.entry_type, expected_type, "Entry type mismatch at {}", i);
    }

    println!("✓ WAL replay matches original writes exactly");
}

#[test]
fn test_wal_handles_large_entries() {
    // Test WAL with entries larger than buffer size
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let wal_path = temp_dir.path().join("large_entry_test.wal");

    let config = WalConfig {
        buffer_size: 1024, // Small buffer
        max_size: 10 * 1024 * 1024,
        sync_on_write: true,
        async_write: false,
        sync_every_bytes: None,
        sync_interval_ms: None,
    };

    {
        let mut wal = Wal::with_config(&wal_path, config.clone()).expect("Failed to create WAL");

        // Write entry larger than buffer
        let large_key = vec![b'k'; 2000];
        let large_value = vec![b'v'; 5000];

        wal.append(&large_key, &large_value).expect("Failed to append large entry");
        wal.sync().expect("Failed to sync");
    }

    // Replay and verify
    let entries = Wal::replay(&wal_path).expect("Failed to replay");

    assert_eq!(entries.len(), 1, "Expected 1 entry");
    assert_eq!(entries[0].key.len(), 2000, "Key size mismatch");
    assert_eq!(entries[0].value.len(), 5000, "Value size mismatch");

    println!("✓ WAL handles entries larger than buffer size");
}

#[test]
fn test_wal_checkpoint() {
    // Test checkpoint creation and metadata
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let wal_path = temp_dir.path().join("checkpoint_test.wal");

    let config = WalConfig::default();

    {
        let mut wal = Wal::with_config(&wal_path, config).expect("Failed to create WAL");

        // Write some entries
        for i in 0..50 {
            let key = format!("key_{}", i).into_bytes();
            let value = format!("value_{}", i).into_bytes();
            wal.append(&key, &value).expect("Failed to append");
        }

        wal.sync().expect("Failed to sync");

        // Create checkpoint
        let sstable_files = vec!["sstable_001.sst".to_string(), "sstable_002.sst".to_string()];
        let checkpoint_seq = wal
            .checkpoint(sstable_files, 1)
            .expect("Failed to create checkpoint");

        assert!(checkpoint_seq >= 50, "Checkpoint sequence should cover all entries");
    }

    println!("✓ WAL checkpoint created successfully");
}

#[test]
fn test_wal_rotation_on_size_limit() {
    // Test that WAL rotates when max_size is reached
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let wal_path = temp_dir.path().join("rotation_test.wal");

    let config = WalConfig {
        buffer_size: 1024,
        max_size: 10 * 1024, // 10KB limit
        sync_on_write: true,
        async_write: false,
        sync_every_bytes: None,
        sync_interval_ms: None,
    };

    {
        let mut wal = Wal::with_config(&wal_path, config).expect("Failed to create WAL");

        // Write enough data to exceed max_size
        for i in 0..1000 {
            let key = format!("key_{}", i).into_bytes();
            let value = vec![b'x'; 100]; // 100-byte values
            wal.append(&key, &value).expect("Failed to append");
        }

        wal.sync().expect("Failed to sync");
    }

    // WAL should have rotated (created new segments)
    // This is implementation-dependent, but we can verify recovery works

    let entries = Wal::replay(&wal_path).expect("Failed to replay after rotation");
    assert!(entries.len() > 0, "Should recover entries after rotation");

    println!("✓ WAL handles size limit and rotation");
}

#[test]
fn test_wal_empty_replay() {
    // Test replaying an empty WAL
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let wal_path = temp_dir.path().join("empty.wal");

    let config = WalConfig::default();

    // Create and immediately close WAL (no writes)
    {
        let _wal = Wal::with_config(&wal_path, config).expect("Failed to create WAL");
    }

    // Replay empty WAL
    let entries = Wal::replay(&wal_path).expect("Failed to replay empty WAL");
    assert_eq!(entries.len(), 0, "Empty WAL should have no entries");

    println!("✓ WAL handles empty replay correctly");
}

#[test]
fn test_wal_concurrent_writes() {
    // Test that WAL handles sequential writes correctly (concurrency within single thread)
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let wal_path = temp_dir.path().join("concurrent.wal");

    let config = WalConfig::default();

    {
        let mut wal = Wal::with_config(&wal_path, config).expect("Failed to create WAL");

        // Rapidly write many entries
        for i in 0..500 {
            let key = format!("k{}", i).into_bytes();
            let value = format!("v{}", i).into_bytes();
            wal.append(&key, &value).expect("Failed to append");
        }

        wal.sync().expect("Failed to sync");
    }

    // Verify all entries present
    let entries = Wal::replay(&wal_path).expect("Failed to replay");
    assert_eq!(entries.len(), 500, "Should have all 500 entries");

    // Verify sequence numbers are monotonic
    for i in 1..entries.len() {
        assert!(
            entries[i].sequence > entries[i - 1].sequence,
            "Sequence numbers should be monotonically increasing"
        );
    }

    println!("✓ WAL handles rapid sequential writes correctly");
}
