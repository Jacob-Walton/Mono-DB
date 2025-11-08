//! Node-level operations for B-tree manipulation

use monodb_common::{MonoError, Result};
use parking_lot::RwLock;
use std::sync::Arc;

use crate::storage::{
    buffer_pool::BufferPool,
    page::{PAGE_HEADER_SIZE, Page, PageId, PageType},
};

/// Node operations trait
pub struct NodeOps;

impl NodeOps {
    /// Check if a page has enough space for a new entry.
    ///
    /// Accounts for key length, value length, and cell overhead (10 bytes).
    pub fn has_space_for(page: &Page, key_len: usize, value_len: usize) -> bool {
        page.available_space() >= key_len + value_len + 10
    }

    /// Split a leaf node into two balanced nodes.
    ///
    /// Returns the new right sibling page ID and the separator key that
    /// should be promoted to the parent.
    pub fn split_leaf(
        buffer_pool: &BufferPool,
        page_id: PageId,
        page: &Arc<RwLock<Page>>,
        new_key: &[u8],
        new_value: &[u8],
    ) -> Result<(PageId, Vec<u8>)> {
        // Collect all existing cells plus the new one
        let mut all_cells = Vec::new();

        {
            let page_guard = page.read();
            for i in 0..page_guard.cell_count() {
                let offset = page_guard.get_cell_offset(i)?;
                let (k, v) = page_guard.get_cell(offset)?;
                all_cells.push((k, v));
            }
        }

        // Insert new cell in sorted order
        let insert_pos = all_cells
            .binary_search_by(|probe| probe.0.as_slice().cmp(new_key))
            .unwrap_or_else(|i| i);
        all_cells.insert(insert_pos, (new_key.to_vec(), new_value.to_vec()));

        // Split point: half the entries
        let mid = all_cells.len() / 2;
        let separator_key = all_cells[mid].0.clone();

        // Create new right sibling
        let right_page = buffer_pool.new_page(PageType::Leaf)?;
        let right_id = right_page.read().header.page_id;

        // Repopulate left node with first half
        {
            let mut page_guard = page.write();
            page_guard.header.cell_count = 0;
            page_guard.header.free_space_start = PAGE_HEADER_SIZE as u16;
            page_guard.data.fill(0);

            for (k, v) in &all_cells[..mid] {
                page_guard.add_cell(k, v)?;
            }

            page_guard.header.right_sibling = right_id;
        }

        // Populate right node with second half
        {
            let mut right_guard = right_page.write();
            for (k, v) in &all_cells[mid..] {
                right_guard.add_cell(k, v)?;
            }
        }

        // Mark both pages dirty
        buffer_pool.unpin_page(page_id, true)?;
        buffer_pool.unpin_page(right_id, true)?;

        Ok((right_id, separator_key))
    }

    pub fn split_interior(
        buffer_pool: &BufferPool,
        page_id: PageId,
        page: &Arc<RwLock<Page>>,
        new_separator: &[u8],
        new_child: PageId,
    ) -> Result<(PageId, Vec<u8>)> {
        // Collect all existing entries plus the new one
        let mut all_entries = Vec::new();

        {
            let page_guard = page.read();
            for i in 0..page_guard.cell_count() {
                let offset = page_guard.get_cell_offset(i)?;
                let (k, v) = page_guard.get_cell(offset)?;

                if v.len() >= 8 {
                    let child_id = PageId(u64::from_le_bytes([
                        v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7],
                    ]));
                    all_entries.push((k, child_id));
                }
            }
        }

        // Insert new entry in sorted order
        let insert_pos = all_entries
            .binary_search_by(|probe| probe.0.as_slice().cmp(new_separator))
            .unwrap_or_else(|i| i);
        all_entries.insert(insert_pos, (new_separator.to_vec(), new_child));

        // Split
        let mid = all_entries.len() / 2;
        let promoted_key = all_entries[mid].0.clone();

        // Create new right sibling
        let right_page = buffer_pool.new_page(PageType::Interior)?;
        let right_id = right_page.read().header.page_id;

        // Repopulate left node with entries [0..mid]
        {
            let mut page_guard = page.write();
            page_guard.header.cell_count = 0;
            page_guard.header.free_space_start = PAGE_HEADER_SIZE as u16;
            page_guard.data.fill(0);

            for (k, child_id) in &all_entries[..mid] {
                let child_bytes = child_id.0.to_le_bytes();
                page_guard.add_cell(k, &child_bytes)?;
            }
        }

        // Populate right node with entries [mid+1..]
        {
            let mut right_guard = right_page.write();
            for (k, child_id) in &all_entries[mid + 1..] {
                let child_bytes = child_id.0.to_le_bytes();
                right_guard.add_cell(k, &child_bytes)?;
            }
        }

        buffer_pool.unpin_page(page_id, true)?;
        buffer_pool.unpin_page(right_id, true)?;

        Ok((right_id, promoted_key))
    }

    /// Find the appropriate child page to descned into a given key.
    ///
    /// In interior nodes, cell 0 contains the leftmost child (with empty key),
    /// and subsequent cells contain separator keys pointing to their respective
    /// children.
    pub fn find_child_for_key(page: &Page, key: &[u8]) -> Result<PageId> {
        if page.cell_count() == 0 {
            return Err(MonoError::Storage("interior node has no children".into()));
        }

        // Get leftmost child from cell 0 (empty key)
        let offset0 = page.get_cell_offset(0)?;
        let (_, v0) = page.get_cell(offset0)?;

        if v0.len() < 8 {
            return Err(MonoError::Storage("invalid child pointer".into()));
        }

        let mut target_child = PageId(u64::from_le_bytes([
            v0[0], v0[1], v0[2], v0[3], v0[4], v0[5], v0[6], v0[7],
        ]));

        // Scan separator keys to find the correct child
        // Key goes into child_i if separator[i-1] <= key < separator[i]
        for i in 1..page.cell_count() {
            let offset = page.get_cell_offset(i)?;
            let (k, v) = page.get_cell(offset)?;

            if key < k.as_slice() {
                break; // Use previous child
            }

            if v.len() >= 8 {
                target_child = PageId(u64::from_le_bytes([
                    v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7],
                ]));
            }
        }

        Ok(target_child)
    }

    /// Create a new root node after the old root splits.
    ///
    /// The new root is an interior node with the leftmost child and one
    /// separator key pointing to the right child.
    pub fn create_new_root(
        buffer_pool: &BufferPool,
        left_child: PageId,
        right_child: PageId,
        separator_key: &[u8],
    ) -> Result<PageId> {
        let new_root = buffer_pool.new_page(PageType::Interior)?;
        let new_root_id = new_root.read().header.page_id;

        {
            let mut root_guard = new_root.write();

            // Cell 0: empty key -> leftmost child
            let left_bytes = left_child.0.to_le_bytes();
            root_guard.add_cell(&[], &left_bytes)?;

            // Cell 1: separator key -> right child
            let right_bytes = right_child.0.to_le_bytes();
            root_guard.add_cell(separator_key, &right_bytes)?;
        }

        buffer_pool.unpin_page(new_root_id, true)?;

        Ok(new_root_id)
    }

    /// Add a cell to a leaf node
    pub fn add_to_leaf(page: &Arc<RwLock<Page>>, key: &[u8], value: &[u8]) -> Result<()> {
        page.write().add_cell(key, value)?;

        Ok(())
    }

    /// Add a separator to an interior node
    pub fn add_to_interior(
        page: &Arc<RwLock<Page>>,
        separator_key: &[u8],
        child_id: PageId,
    ) -> Result<()> {
        let child_bytes = child_id.0.to_le_bytes();
        page.write().add_cell(separator_key, &child_bytes)?;

        Ok(())
    }
}
