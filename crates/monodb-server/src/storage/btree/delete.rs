//! Delete operations for B-tree

use crate::storage::btree::BTree;
use crate::storage::page::{Page, PageId, PageType};
use monodb_common::{MonoError, Result};

/// Delete a key from the B-Tree if present.
///
/// Basic implementation: descends to the target leaf and removes the cell.
/// No rebalancing/merging is performed on underflow.
pub async fn delete(btree: &BTree, key: &[u8]) -> Result<()> {
    let root_page_id = btree.root_page_id();
    let root_guard = root_page_id.read();
    let root = match *root_guard {
        Some(id) => id,
        None => return Ok(()),
    };
    drop(root_guard);

    let mut path: Vec<(PageId, usize)> = Vec::new();
    delete_from_page(btree, root, key, &mut path)
}

fn delete_from_page(
    btree: &BTree,
    page_id: PageId,
    key: &[u8],
    path: &mut Vec<(PageId, usize)>,
) -> Result<()> {
    let page = btree.buffer_pool().fetch_page(page_id)?;
    let page_type = page.read().header.page_type;

    match page_type {
        PageType::Leaf => {
            // Find the key and remove it if present
            let index_opt = {
                let guard = page.read();
                let mut idx: Option<u16> = None;
                for i in 0..guard.cell_count() {
                    let off = guard.get_cell_offset(i)?;
                    let (k, _v) = guard.get_cell(off)?;
                    if k.as_slice() == key {
                        idx = Some(i);
                        break;
                    }
                    if k.as_slice() > key {
                        break;
                    }
                }
                idx
            };

            if let Some(i) = index_opt {
                // Remove the cell and mark page dirty
                page.write().remove_cell(i)?;
                let became_empty = page.read().cell_count() == 0;
                let right_sibling = page.read().header.right_sibling;
                btree.buffer_pool().unpin_page(page_id, true)?;

                // Basic rebalance: if page is empty and has a right sibling,
                // redirect parent pointer to the right sibling and update left sibling linkage.
                if became_empty && !path.is_empty() && right_sibling.0 != 0 {
                    if let Some((parent_id, idx_in_parent)) = path.last().cloned() {
                        // Update parent to point to right sibling
                        update_parent_child_pointer(
                            btree,
                            parent_id,
                            idx_in_parent,
                            right_sibling,
                        )?;

                        // Update left sibling's right_sibling to skip the removed page
                        if idx_in_parent > 0 {
                            if let Some(left_id) = left_sibling_id(btree, parent_id, idx_in_parent)?
                            {
                                // Avoid creating self-loops or redundant links
                                if left_id != right_sibling && left_id != page_id {
                                    let left_page = btree.buffer_pool().fetch_page(left_id)?;
                                    {
                                        let mut g = left_page.write();
                                        g.header.right_sibling = right_sibling;
                                    }
                                    btree.buffer_pool().unpin_page(left_id, true)?;
                                }
                            }
                        }

                        // Clear removed page's sibling link to avoid accidental back-links
                        if let Ok(p) = btree.buffer_pool().fetch_page(page_id) {
                            {
                                let mut g = p.write();
                                g.header.right_sibling = PageId(0);
                            }
                            btree.buffer_pool().unpin_page(page_id, true)?;
                        }

                        // Finally, delete the now-empty page
                        btree.buffer_pool().delete_page(page_id)?;
                    }
                } else if became_empty && path.is_empty() {
                    // Deleted last key in root leaf → empty tree
                    btree.buffer_pool().set_root_page_id(None)?;
                    *btree.root_page_id().write() = None;
                    btree.buffer_pool().delete_page(page_id)?;
                }
            } else {
                btree.buffer_pool().unpin_page(page_id, false)?;
            }
            Ok(())
        }
        PageType::Interior => {
            // Descend to the appropriate child
            let (child, child_index) = {
                let guard = page.read();
                find_child_for_key_with_index(&guard, key)?
            };
            // Unpin parent before descending
            btree.buffer_pool().unpin_page(page_id, false)?;
            path.push((page_id, child_index));
            let res = delete_from_page(btree, child, key, path);
            path.pop();
            res
        }
        _ => {
            btree.buffer_pool().unpin_page(page_id, false)?;
            Err(MonoError::Storage(
                "invalid page type in B-Tree delete".into(),
            ))
        }
    }
}

fn find_child_for_key_with_index(page: &Page, key: &[u8]) -> Result<(PageId, usize)> {
    if page.cell_count() == 0 {
        return Err(MonoError::Storage("interior node has no children".into()));
    }
    // Cell 0 leftmost child
    let off0 = page.get_cell_offset(0)?;
    let (_k0, v0) = page.get_cell(off0)?;
    if v0.len() < 8 {
        return Err(MonoError::Storage("invalid child pointer".into()));
    }
    let mut target = PageId(u64::from_le_bytes([
        v0[0], v0[1], v0[2], v0[3], v0[4], v0[5], v0[6], v0[7],
    ]));
    let mut index = 0usize;

    for i in 1..page.cell_count() {
        let off = page.get_cell_offset(i)?;
        let (k, v) = page.get_cell(off)?;
        if key >= k.as_slice() {
            if v.len() >= 8 {
                target = PageId(u64::from_le_bytes([
                    v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7],
                ]));
                index = i as usize;
            }
        } else {
            break;
        }
    }
    Ok((target, index))
}

fn update_parent_child_pointer(
    btree: &BTree,
    parent_id: PageId,
    index: usize,
    new_child: PageId,
) -> Result<()> {
    let parent = btree.buffer_pool().fetch_page(parent_id)?;
    {
        let mut g = parent.write();
        // Read existing key to preserve it
        let off = g.get_cell_offset(index as u16)?;
        let (k, _v) = g.get_cell(off)?;
        let child_bytes = new_child.0.to_le_bytes();
        g.update_cell(off, &k, &child_bytes)?;
    }
    btree.buffer_pool().unpin_page(parent_id, true)?;
    Ok(())
}

fn left_sibling_id(btree: &BTree, parent_id: PageId, index: usize) -> Result<Option<PageId>> {
    let parent = btree.buffer_pool().fetch_page(parent_id)?;
    let res = {
        let g = parent.read();
        if index == 0 {
            None
        } else if index == 1 {
            // Left sibling is leftmost child (cell 0)
            let off0 = g.get_cell_offset(0)?;
            let (_k, v) = g.get_cell(off0)?;
            if v.len() >= 8 {
                Some(PageId(u64::from_le_bytes([
                    v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7],
                ])))
            } else {
                None
            }
        } else {
            // Left sibling is child id stored at cell index-1
            let off = g.get_cell_offset((index as u16) - 1)?;
            let (_k, v) = g.get_cell(off)?;
            if v.len() >= 8 {
                Some(PageId(u64::from_le_bytes([
                    v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7],
                ])))
            } else {
                None
            }
        }
    };
    btree.buffer_pool().unpin_page(parent_id, false)?;
    Ok(res)
}
