//! Merge iterator for LSM tree
//!
//! Merges multiple sorted iterators using a binary heap,
//! ensuring results come out in sorted order.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// A wrapper for heap entries
struct HeapEntry {
    key: Vec<u8>,
    value: Vec<u8>,
    /// Source priority: lower = more recent (memtable=0, immutable=1, sstable=2+)
    source_priority: usize,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.source_priority == other.source_priority
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap, so we reverse the comparison for min-heap behavior
        match other.key.cmp(&self.key) {
            Ordering::Equal => {
                // Same key: prefer lower source_priority (more recent data)
                other.source_priority.cmp(&self.source_priority)
            }
            ord => ord,
        }
    }
}

/// Merge iterator that combines multiple sorted sources
pub struct MergeIterator {
    heap: BinaryHeap<HeapEntry>,
    last_key: Option<Vec<u8>>,
}

impl MergeIterator {
    /// Create a new merge iterator from multiple sorted sources
    /// Each source is a Vec of (key, value) pairs, assumed to be sorted by key
    /// Sources should be ordered by recency: index 0 = most recent (memtable)
    pub fn new(sources: Vec<Vec<(Vec<u8>, Vec<u8>)>>) -> Self {
        let mut heap = BinaryHeap::new();

        // Add all entries to heap with their source priority
        for (source_idx, source) in sources.into_iter().enumerate() {
            for (key, value) in source {
                heap.push(HeapEntry {
                    key,
                    value,
                    source_priority: source_idx,
                });
            }
        }

        MergeIterator {
            heap,
            last_key: None,
        }
    }

    /// Collect all unique entries in sorted order
    /// For duplicate keys, keeps the entry from the most recent source (lowest priority)
    pub fn collect_sorted(mut self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut results = Vec::new();

        while let Some(entry) = self.heap.pop() {
            // Skip duplicates (we already have a more recent version)
            if self.last_key.as_ref() == Some(&entry.key) {
                continue;
            }

            self.last_key = Some(entry.key.clone());
            results.push((entry.key, entry.value));
        }

        results
    }

    /// Collect up to `limit` unique entries in sorted order
    pub fn collect_sorted_limit(mut self, limit: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut results = Vec::with_capacity(limit);

        while let Some(entry) = self.heap.pop() {
            if results.len() >= limit {
                break;
            }

            // Skip duplicates
            if self.last_key.as_ref() == Some(&entry.key) {
                continue;
            }

            self.last_key = Some(entry.key.clone());
            results.push((entry.key, entry.value));
        }

        results
    }
}

/// A wrapper for heap entries for descending order (max-heap)
struct ReverseHeapEntry {
    key: Vec<u8>,
    value: Vec<u8>,
    /// Source priority: lower = more recent (memtable=0, immutable=1, sstable=2+)
    source_priority: usize,
}

impl PartialEq for ReverseHeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.source_priority == other.source_priority
    }
}

impl Eq for ReverseHeapEntry {}

impl PartialOrd for ReverseHeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ReverseHeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap, so larger keys come out first naturally
        // For DESC order, we want largest keys first
        match self.key.cmp(&other.key) {
            Ordering::Equal => {
                // Same key: prefer lower source_priority (more recent data)
                // We flip this so lower priority (more recent) wins
                other.source_priority.cmp(&self.source_priority)
            }
            ord => ord,
        }
    }
}

/// Reverse merge iterator that combines multiple sorted sources in DESCENDING order.
/// This is the key to efficient DESC queries, it merges sources while
/// returning results from highest to lowest key.
pub struct ReverseMergeIterator {
    heap: BinaryHeap<ReverseHeapEntry>,
    last_key: Option<Vec<u8>>,
}

impl ReverseMergeIterator {
    /// Create a new reverse merge iterator from multiple sorted sources.
    /// Each source is a Vec of (key, value) pairs, assumed to be sorted by key ASCENDING.
    /// Sources should be ordered by recency: index 0 = most recent (memtable).
    /// Output will be in DESCENDING key order.
    pub fn new(sources: Vec<Vec<(Vec<u8>, Vec<u8>)>>) -> Self {
        let mut heap = BinaryHeap::new();

        // Add all entries to heap with their source priority
        for (source_idx, source) in sources.into_iter().enumerate() {
            for (key, value) in source {
                heap.push(ReverseHeapEntry {
                    key,
                    value,
                    source_priority: source_idx,
                });
            }
        }

        ReverseMergeIterator {
            heap,
            last_key: None,
        }
    }

    /// Collect all unique entries in DESCENDING sorted order.
    /// For duplicate keys, keeps the entry from the most recent source (lowest priority).
    pub fn collect_sorted(mut self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut results = Vec::new();

        while let Some(entry) = self.heap.pop() {
            // Skip duplicates (we already have a more recent version)
            if self.last_key.as_ref() == Some(&entry.key) {
                continue;
            }

            self.last_key = Some(entry.key.clone());
            results.push((entry.key, entry.value));
        }

        results
    }

    /// Collect up to `limit` unique entries in DESCENDING sorted order.
    /// This is the key optimization - we can stop early once we have enough entries.
    pub fn collect_sorted_limit(mut self, limit: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut results = Vec::with_capacity(limit);

        while let Some(entry) = self.heap.pop() {
            if results.len() >= limit {
                break;
            }

            // Skip duplicates
            if self.last_key.as_ref() == Some(&entry.key) {
                continue;
            }

            self.last_key = Some(entry.key.clone());
            results.push((entry.key, entry.value));
        }

        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_sorted_order() {
        let source1 = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
        ];
        let source2 = vec![
            (b"b".to_vec(), b"2".to_vec()),
            (b"d".to_vec(), b"4".to_vec()),
        ];

        let merger = MergeIterator::new(vec![source1, source2]);
        let result = merger.collect_sorted();

        assert_eq!(result.len(), 4);
        assert_eq!(result[0].0, b"a".to_vec());
        assert_eq!(result[1].0, b"b".to_vec());
        assert_eq!(result[2].0, b"c".to_vec());
        assert_eq!(result[3].0, b"d".to_vec());
    }

    #[test]
    fn test_merge_duplicates_prefer_recent() {
        // source1 is more recent (memtable)
        let source1 = vec![(b"key".to_vec(), b"new_value".to_vec())];
        // source2 is older (sstable)
        let source2 = vec![(b"key".to_vec(), b"old_value".to_vec())];

        let merger = MergeIterator::new(vec![source1, source2]);
        let result = merger.collect_sorted();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].1, b"new_value".to_vec());
    }

    #[test]
    fn test_merge_with_limit() {
        let source = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
        ];

        let merger = MergeIterator::new(vec![source]);
        let result = merger.collect_sorted_limit(2);

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0, b"a".to_vec());
        assert_eq!(result[1].0, b"b".to_vec());
    }

    // Tests for ReverseMergeIterator

    #[test]
    fn test_reverse_merge_sorted_order() {
        let source1 = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
        ];
        let source2 = vec![
            (b"b".to_vec(), b"2".to_vec()),
            (b"d".to_vec(), b"4".to_vec()),
        ];

        let merger = ReverseMergeIterator::new(vec![source1, source2]);
        let result = merger.collect_sorted();

        // Should be in DESCENDING order: d, c, b, a
        assert_eq!(result.len(), 4);
        assert_eq!(result[0].0, b"d".to_vec());
        assert_eq!(result[1].0, b"c".to_vec());
        assert_eq!(result[2].0, b"b".to_vec());
        assert_eq!(result[3].0, b"a".to_vec());
    }

    #[test]
    fn test_reverse_merge_duplicates_prefer_recent() {
        // source1 is more recent (memtable)
        let source1 = vec![(b"key".to_vec(), b"new_value".to_vec())];
        // source2 is older (sstable)
        let source2 = vec![(b"key".to_vec(), b"old_value".to_vec())];

        let merger = ReverseMergeIterator::new(vec![source1, source2]);
        let result = merger.collect_sorted();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].1, b"new_value".to_vec());
    }

    #[test]
    fn test_reverse_merge_with_limit() {
        let source = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
            (b"d".to_vec(), b"4".to_vec()),
        ];

        let merger = ReverseMergeIterator::new(vec![source]);
        let result = merger.collect_sorted_limit(2);

        // Should get the 2 LARGEST keys: d, c
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0, b"d".to_vec());
        assert_eq!(result[1].0, b"c".to_vec());
    }

    #[test]
    fn test_reverse_merge_numeric_keys() {
        // Simulate sortable-encoded numeric keys (larger bytes = larger number)
        let source = vec![
            (vec![0x10, 0x80, 0x00, 0x00, 0x01], b"1".to_vec()), // represents 1
            (vec![0x10, 0x80, 0x00, 0x00, 0x02], b"2".to_vec()), // represents 2
            (vec![0x10, 0x80, 0x00, 0x00, 0x0A], b"10".to_vec()), // represents 10
        ];

        let merger = ReverseMergeIterator::new(vec![source]);
        let result = merger.collect_sorted_limit(2);

        // Should get the 2 largest: 10, 2
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].1, b"10".to_vec());
        assert_eq!(result[1].1, b"2".to_vec());
    }
}
