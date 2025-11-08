//! Insert operations for B-tree

use std::sync::Arc;

use crate::storage::{
    btree::{BTree, node::NodeOps},
    page::{Page, PageId, PageType},
};
use monodb_common::{MonoError, Result};
use parking_lot::RwLock;

/// Result of an insert operation at any level of the tree
#[derive(Debug)]
pub enum InsertResult {
    /// Insert completed without splitting
    Ok,
    /// Node split occurred, parent must insert separator
    Split {
        left_page: PageId,
        right_page: PageId,
        separator_key: Vec<u8>,
    },
}

/// Main insert entry point
pub async fn insert(btree: &BTree, key: &[u8], value: &[u8]) -> Result<()> {
    let root_page_id = btree.root_page_id();
    let mut root_guard = root_page_id.write();

    // Handle empty tree
    if root_guard.is_none() {
        let root_page = btree.buffer_pool().new_page(PageType::Leaf)?;
        let page_id = root_page.read().header.page_id;

        root_page.write().add_cell(key, value)?;
        btree.buffer_pool().unpin_page(page_id, true)?;

        *root_guard = Some(page_id);
        // Persist root page id to disk
        btree.buffer_pool().set_root_page_id(Some(page_id))?;
        return Ok(());
    }

    let root_id = root_guard.unwrap();
    drop(root_guard);

    // Recursive insert
    match insert_recursive(btree, root_id, key, value)? {
        InsertResult::Ok => Ok(()),
        InsertResult::Split {
            left_page,
            right_page,
            separator_key,
        } => {
            // Root split, create new root above
            let new_root_id = NodeOps::create_new_root(
                btree.buffer_pool(),
                left_page,
                right_page,
                &separator_key,
            )?;

            *btree.root_page_id().write() = Some(new_root_id);
            // Persist new root to disk metadata
            btree.buffer_pool().set_root_page_id(Some(new_root_id))?;
            Ok(())
        }
    }
}

/// Recursive insert that handles splits bottom-up
fn insert_recursive(
    btree: &BTree,
    page_id: PageId,
    key: &[u8],
    value: &[u8],
) -> Result<InsertResult> {
    let page = btree.buffer_pool().fetch_page(page_id)?;
    let page_type = page.read().header.page_type;

    match page_type {
        PageType::Leaf => insert_into_leaf(btree, page_id, page, key, value),
        PageType::Interior => insert_into_interior(btree, page_id, page, key, value),
        _ => {
            btree.buffer_pool().unpin_page(page_id, false)?;
            Err(MonoError::Storage("invalid page type in B-tree".into()))
        }
    }
}

/// Insert into a leaf node, splitting if necessary
fn insert_into_leaf(
    btree: &BTree,
    page_id: PageId,
    page: Arc<RwLock<Page>>,
    key: &[u8],
    value: &[u8],
) -> Result<InsertResult> {
    let needs_split = {
        let page_guard = page.read();
        !NodeOps::has_space_for(&page_guard, key.len(), value.len())
    };

    if !needs_split {
        // Simple case, add and done
        NodeOps::add_to_leaf(&page, key, value)?;
        btree.buffer_pool().unpin_page(page_id, true)?;
        Ok(InsertResult::Ok)
    } else {
        // Split the leaf node
        let (right_id, separator_key) =
            NodeOps::split_leaf(btree.buffer_pool(), page_id, &page, key, value)?;

        Ok(InsertResult::Split {
            left_page: page_id,
            right_page: right_id,
            separator_key,
        })
    }
}

/// Insert into an interior node recursing to the appropriate child
fn insert_into_interior(
    btree: &BTree,
    page_id: PageId,
    page: Arc<RwLock<Page>>,
    key: &[u8],
    value: &[u8],
) -> Result<InsertResult> {
    // Find the child to descend into
    let child_id = {
        let page_guard = page.read();
        NodeOps::find_child_for_key(&page_guard, key)?
    };

    btree.buffer_pool().unpin_page(page_id, false)?;
    drop(page);

    // Recurse into child
    match insert_recursive(btree, child_id, key, value)? {
        InsertResult::Ok => Ok(InsertResult::Ok),
        InsertResult::Split {
            left_page,
            right_page,
            separator_key,
        } => {
            // Child split, insert separator into this interior node
            handle_child_split(btree, page_id, &separator_key, left_page, right_page)
        }
    }
}

/// Handle a child split by inserting the separator into the parent interior node.
fn handle_child_split(
    btree: &BTree,
    page_id: PageId,
    separator_key: &[u8],
    _left_child: PageId,
    right_child: PageId,
) -> Result<InsertResult> {
    let page = btree.buffer_pool().fetch_page(page_id)?;

    let needs_split = {
        let page_guard = page.read();
        !NodeOps::has_space_for(&page_guard, separator_key.len(), 8)
    };

    if !needs_split {
        // Simple case, add separator and done
        NodeOps::add_to_interior(&page, separator_key, right_child)?;
        btree.buffer_pool().unpin_page(page_id, true)?;
        Ok(InsertResult::Ok)
    } else {
        // Interior node is full, split it too
        let (_, promoted_key) = NodeOps::split_interior(
            btree.buffer_pool(),
            page_id,
            &page,
            separator_key,
            right_child,
        )?;

        Ok(InsertResult::Split {
            left_page: page_id,
            right_page: page_id,
            separator_key: promoted_key,
        })
    }
}
