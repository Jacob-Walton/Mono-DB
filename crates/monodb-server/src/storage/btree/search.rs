//! Search operations for B-tree

use crate::storage::{
    btree::{BTree, node::NodeOps},
    page::{Page, PageId, PageType},
};
use monodb_common::{MonoError, Result};
use std::collections::HashSet;

/// Search for a key in the B-tree
pub async fn get(btree: &BTree, key: &[u8]) -> Result<Option<Vec<u8>>> {
    let root_page_id = btree.root_page_id();
    let root_guard = root_page_id.read();

    match *root_guard {
        Some(root_id) => {
            drop(root_guard);
            search_recursive(btree, root_id, key)
        }
        None => Ok(None),
    }
}

/// Recursive search through the tree
fn search_recursive(btree: &BTree, page_id: PageId, key: &[u8]) -> Result<Option<Vec<u8>>> {
    let page = btree.buffer_pool().fetch_page(page_id)?;
    let page_guard = page.read();

    match page_guard.header.page_type {
        PageType::Leaf => search_leaf(&page_guard, btree, page_id, key),
        PageType::Interior => search_interior(&page_guard, btree, page_id, key),
        _ => {
            btree.buffer_pool().unpin_page(page_id, false)?;
            Err(MonoError::Storage("invalid page type in b-tree".into()))
        }
    }
}

/// Search within a leaf node using binary scan
fn search_leaf(
    page_guard: &Page,
    btree: &BTree,
    page_id: PageId,
    key: &[u8],
) -> Result<Option<Vec<u8>>> {
    // lInear scan through cells (TODO: Binary search)
    for i in 0..page_guard.cell_count() {
        let offset = page_guard.get_cell_offset(i)?;
        let (k, v) = page_guard.get_cell(offset)?;

        if k.as_slice() == key {
            btree.buffer_pool().unpin_page(page_id, false)?;
            return Ok(Some(v));
        }

        // Since cells are sorted, we can stop early
        if k.as_slice() > key {
            break;
        }
    }

    btree.buffer_pool().unpin_page(page_id, false)?;
    Ok(None)
}

/// Search within an interior node by finding the correct child to descend into
fn search_interior(
    page_guard: &Page,
    btree: &BTree,
    page_id: PageId,
    key: &[u8],
) -> Result<Option<Vec<u8>>> {
    let child_id = NodeOps::find_child_for_key(page_guard, key)?;

    btree.buffer_pool().unpin_page(page_id, false)?;

    // Recurse into the appropriate child
    search_recursive(btree, child_id, key)
}

/// Range scan (TODO)
#[allow(dead_code)]
pub struct BTreeIterator {
    // Maintain a cursor position for range scans
}

impl BTreeIterator {
    /// Create an iterator starting at the given key
    #[allow(dead_code)]
    pub fn new(_btree: &BTree, _start_key: &[u8]) -> Result<Self> {
        // TODO: Implement range scan iterator
        Ok(Self {})
    }
}

/// Range scan: returns all key-value pairs with start_key <= key <= end_key (inclusive)
pub async fn scan(
    btree: &BTree,
    start_key: &[u8],
    end_key: &[u8],
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    // Validate order
    if start_key > end_key {
        return Ok(Vec::new());
    }

    // Get root (hold Arc, then guard)
    let root_arc = btree.root_page_id();
    let root_guard = root_arc.read();
    let root_id = match *root_guard {
        Some(id) => id,
        None => return Ok(Vec::new()),
    };
    drop(root_guard);

    // Descend to leaf that may contain start_key
    let mut current = descend_to_leaf_for_key(btree, root_id, start_key)?;

    let mut results = Vec::new();
    let mut visited: HashSet<u64> = HashSet::new();
    let mut steps: usize = 0;

    loop {
        steps += 1;
        if steps > 1_000 {
            tracing::warn!("B-Tree scan aborting after too many steps (possible cycle)");
            break;
        }
        // Fetch current leaf page
        let page = match btree.buffer_pool().fetch_page(current) {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!("B-Tree scan failed to fetch page {}: {}", current.0, e);
                break;
            }
        };
        let page_guard = page.read();

        if page_guard.header.page_type != PageType::Leaf {
            drop(page_guard);
            btree.buffer_pool().unpin_page(current, false)?;
            return Err(MonoError::Storage(
                "range scan reached non-leaf page".into(),
            ));
        }

        // Cycle detection on current page id
        if !visited.insert(current.0) {
            tracing::warn!("B-Tree scan detected cycle visiting page {}", current.0);
            btree.buffer_pool().unpin_page(current, false)?;
            break;
        }

        // Find first index >= start_key in this leaf
        let mut idx: u16 = 0;
        while idx < page_guard.cell_count() {
            let off = page_guard.get_cell_offset(idx)?;
            let (k, _v) = page_guard.get_cell(off)?;
            if k.as_slice() >= start_key {
                break;
            }
            idx += 1;
        }

        // Collect while key <= end_key
        while idx < page_guard.cell_count() {
            let off = page_guard.get_cell_offset(idx)?;
            let (k, v) = page_guard.get_cell(off)?;
            if k.as_slice() > end_key {
                break;
            }
            results.push((k, v));
            idx += 1;
        }

        // Move to right sibling if needed
        let next = page_guard.header.right_sibling;
        drop(page_guard);
        btree.buffer_pool().unpin_page(current, false)?;

        if next.0 == 0 {
            break;
        }

        // Continue with the sibling (no peek)
        if next == current {
            tracing::warn!("B-Tree scan: next sibling equals current {}", current.0);
            break;
        }
        // Cycle detection on sibling hop
        if !visited.insert(next.0) {
            tracing::warn!("B-Tree scan detected cycle on sibling hop to page {}", next.0);
            break;
        }
        current = next;
    }

    Ok(results)
}

fn descend_to_leaf_for_key(btree: &BTree, mut page_id: PageId, key: &[u8]) -> Result<PageId> {
    let mut visited: HashSet<u64> = HashSet::new();
    let mut steps: usize = 0;
    loop {
        steps += 1;
        if steps > 1000 {
            return Err(MonoError::Storage(
                "descend_to_leaf exceeded 1000 steps (possible cycle)".into(),
            ));
        }

        // Detect cycles on interior traversal
        if !visited.insert(page_id.0) {
            return Err(MonoError::Storage(format!(
                "cycle detected during descent at page {}",
                page_id.0
            )));
        }

        let page = btree.buffer_pool().fetch_page(page_id)?;
        let page_guard = page.read();
        match page_guard.header.page_type {
            PageType::Leaf => {
                drop(page_guard);
                btree.buffer_pool().unpin_page(page_id, false)?;
                return Ok(page_id);
            }
            PageType::Interior => {
                let child_id = NodeOps::find_child_for_key(&page_guard, key)?;
                drop(page_guard);
                btree.buffer_pool().unpin_page(page_id, false)?;
                if child_id == page_id {
                    return Err(MonoError::Storage(format!(
                        "interior node points to itself (page {})",
                        page_id.0
                    )));
                }
                page_id = child_id;
            }
            _ => {
                drop(page_guard);
                btree.buffer_pool().unpin_page(page_id, false)?;
                return Err(MonoError::Storage("invalid page type in range scan".into()));
            }
        }
    }
}
