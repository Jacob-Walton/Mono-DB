#![cfg_attr(target_arch = "aarch64", feature(stdarch_aarch64_prefetch))]

use std::fmt::Debug;
use std::mem::MaybeUninit;
use std::ptr;

use comfy_table::{Table, presets::UTF8_FULL};

/// Minimum degree (t)
///
/// Larger leads to wider nodes, fewer levels, and better cache locality.
/// Width t=256, max keys = 511, max children = 512, is a reasonable default.
const T: usize = 256;
const MAX_KEYS: usize = 2 * T - 1;
const MAX_CHILDREN: usize = 2 * T;

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};

#[cfg(target_arch = "aarch64")]
use std::arch::aarch64::{_PREFETCH_LOCALITY3, _PREFETCH_READ, _prefetch};

/// Prefetch memory for upcoming access
#[inline(always)]
fn prefetch<T>(ptr: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        _mm_prefetch(ptr as *const i8, _MM_HINT_T0);
    }

    #[cfg(target_arch = "aarch64")]
    unsafe {
        _prefetch(ptr as *const i8, _PREFETCH_READ, _PREFETCH_LOCALITY3);
    }

    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        let _ = ptr;
    }
}

pub struct BTree<K, V> {
    nodes: Vec<BTreeNode<K, V>>,
    root: usize,
}

/// A B-tree node with fixed-size arrays for better cache locality.
///
/// Uses MaybeUninit to avoid initialization overhead and allow
/// efficient bulk copying operations.
pub struct BTreeNode<K, V> {
    keys: [MaybeUninit<K>; MAX_KEYS],
    values: [MaybeUninit<V>; MAX_KEYS],
    children: [usize; MAX_CHILDREN],
    key_count: u16,
    child_count: u16,
    is_leaf: bool,
    next: Option<usize>,
}

impl<K, V> BTreeNode<K, V> {
    /// Create a new B-tree node with fixed-size arrays
    #[inline(always)]
    fn new(is_leaf: bool) -> Self {
        BTreeNode {
            // SAFETY: MaybeUninit doesn't require initialization
            keys: unsafe { MaybeUninit::uninit().assume_init() },
            values: unsafe { MaybeUninit::uninit().assume_init() },
            children: [0; MAX_CHILDREN],
            key_count: 0,
            child_count: 0,
            is_leaf,
            next: None,
        }
    }

    #[inline(always)]
    fn key_count(&self) -> usize {
        self.key_count as usize
    }

    #[inline(always)]
    fn child_count(&self) -> usize {
        self.child_count as usize
    }

    /// Get a reference to key at index (unchecked)
    #[inline(always)]
    unsafe fn key(&self, index: usize) -> &K {
        unsafe { self.keys.get_unchecked(index).assume_init_ref() }
    }

    /// Get a mutable reference to key at index (unchecked)
    #[inline(always)]
    unsafe fn key_mut(&mut self, index: usize) -> &mut K {
        unsafe { self.keys.get_unchecked_mut(index).assume_init_mut() }
    }

    /// Get a reference to value at index (unchecked)
    #[inline(always)]
    unsafe fn value(&self, index: usize) -> &V {
        unsafe { self.values.get_unchecked(index).assume_init_ref() }
    }

    /// Get a mutable reference to value at index (unchecked)
    #[inline(always)]
    #[allow(dead_code)]
    unsafe fn value_mut(&mut self, index: usize) -> &mut V {
        unsafe { self.values.get_unchecked_mut(index).assume_init_mut() }
    }

    /// Get child index at position (unchecked)
    #[inline(always)]
    unsafe fn child(&self, index: usize) -> usize {
        unsafe { *self.children.get_unchecked(index) }
    }

    /// Set child at position (unchecked)
    #[inline(always)]
    unsafe fn set_child(&mut self, index: usize, child: usize) {
        unsafe { *self.children.get_unchecked_mut(index) = child };
    }

    /// Insert a key-value pair at position, shifting existing elements right
    #[inline(always)]
    unsafe fn insert_key_value(&mut self, pos: usize, key: K, value: V) {
        unsafe {
            let count = self.key_count();
            if pos < count {
                // Shift keys and values right
                ptr::copy(
                    self.keys.as_ptr().add(pos),
                    self.keys.as_mut_ptr().add(pos + 1),
                    count - pos,
                );
                ptr::copy(
                    self.values.as_ptr().add(pos),
                    self.values.as_mut_ptr().add(pos + 1),
                    count - pos,
                );
            }
            self.keys.get_unchecked_mut(pos).write(key);
            self.values.get_unchecked_mut(pos).write(value);
            self.key_count += 1;
        }
    }

    /// Push a key-value pair at the end
    #[inline(always)]
    unsafe fn push_key_value(&mut self, key: K, value: V) {
        unsafe {
            let count = self.key_count();
            self.keys.get_unchecked_mut(count).write(key);
            self.values.get_unchecked_mut(count).write(value);
            self.key_count += 1;
        }
    }

    /// Insert a key at position (for internal nodes), shifting existing elements right
    #[inline(always)]
    unsafe fn insert_key(&mut self, pos: usize, key: K) {
        unsafe {
            let count = self.key_count();
            if pos < count {
                ptr::copy(
                    self.keys.as_ptr().add(pos),
                    self.keys.as_mut_ptr().add(pos + 1),
                    count - pos,
                );
            }
            self.keys.get_unchecked_mut(pos).write(key);
            self.key_count += 1;
        }
    }

    /// Insert a child at position, shifting existing children right
    #[inline(always)]
    unsafe fn insert_child(&mut self, pos: usize, child: usize) {
        unsafe {
            let count = self.child_count();
            if pos < count {
                ptr::copy(
                    self.children.as_ptr().add(pos),
                    self.children.as_mut_ptr().add(pos + 1),
                    count - pos,
                );
            }
            *self.children.get_unchecked_mut(pos) = child;
            self.child_count += 1;
        }
    }

    /// Push a child at the end
    #[inline(always)]
    unsafe fn push_child(&mut self, child: usize) {
        unsafe {
            let count = self.child_count();
            *self.children.get_unchecked_mut(count) = child;
            self.child_count += 1;
        }
    }

    /// Remove key at position, shifting remaining elements left
    /// Returns the removed key
    #[inline(always)]
    unsafe fn remove_key(&mut self, pos: usize) -> K {
        unsafe {
            let count = self.key_count();
            let key = ptr::read(self.keys.get_unchecked(pos).as_ptr());
            if pos + 1 < count {
                ptr::copy(
                    self.keys.as_ptr().add(pos + 1),
                    self.keys.as_mut_ptr().add(pos),
                    count - pos - 1,
                );
            }
            self.key_count -= 1;
            key
        }
    }

    /// Remove key-value at position, shifting remaining elements left
    /// Returns the removed value
    #[inline(always)]
    unsafe fn remove_key_value(&mut self, pos: usize) -> (K, V) {
        unsafe {
            let count = self.key_count();
            let key = ptr::read(self.keys.get_unchecked(pos).as_ptr());
            let value = ptr::read(self.values.get_unchecked(pos).as_ptr());
            if pos + 1 < count {
                ptr::copy(
                    self.keys.as_ptr().add(pos + 1),
                    self.keys.as_mut_ptr().add(pos),
                    count - pos - 1,
                );
                ptr::copy(
                    self.values.as_ptr().add(pos + 1),
                    self.values.as_mut_ptr().add(pos),
                    count - pos - 1,
                );
            }
            self.key_count -= 1;
            (key, value)
        }
    }

    /// Remove child at position, shifting remaining children left
    #[inline(always)]
    unsafe fn remove_child(&mut self, pos: usize) -> usize {
        unsafe {
            let count = self.child_count();
            let child = *self.children.get_unchecked(pos);
            if pos + 1 < count {
                ptr::copy(
                    self.children.as_ptr().add(pos + 1),
                    self.children.as_mut_ptr().add(pos),
                    count - pos - 1,
                );
            }
            self.child_count -= 1;
            child
        }
    }

    /// Pop the last key (for internal nodes)
    #[inline(always)]
    unsafe fn pop_key(&mut self) -> K {
        unsafe {
            self.key_count -= 1;
            ptr::read(self.keys.get_unchecked(self.key_count as usize).as_ptr())
        }
    }

    /// Pop the last key-value pair
    #[inline(always)]
    unsafe fn pop_key_value(&mut self) -> (K, V) {
        unsafe {
            self.key_count -= 1;
            let idx = self.key_count as usize;
            let key = ptr::read(self.keys.get_unchecked(idx).as_ptr());
            let value = ptr::read(self.values.get_unchecked(idx).as_ptr());
            (key, value)
        }
    }

    /// Pop the last child
    #[inline(always)]
    unsafe fn pop_child(&mut self) -> usize {
        unsafe {
            self.child_count -= 1;
            *self.children.get_unchecked(self.child_count as usize)
        }
    }

    /// Prepend a key-value pair (insert at position 0)
    #[inline(always)]
    unsafe fn prepend_key_value(&mut self, key: K, value: V) {
        unsafe { self.insert_key_value(0, key, value) };
    }

    /// Prepend a key (for internal nodes)
    #[inline(always)]
    unsafe fn prepend_key(&mut self, key: K) {
        unsafe { self.insert_key(0, key) };
    }

    /// Prepend a child
    #[inline(always)]
    unsafe fn prepend_child(&mut self, child: usize) {
        unsafe { self.insert_child(0, child) };
    }
}

impl<K: Clone, V: Clone> BTreeNode<K, V> {
    /// Clone a key at index
    #[inline(always)]
    unsafe fn clone_key(&self, index: usize) -> K {
        unsafe { self.key(index).clone() }
    }
}

/// Branchless lower bound binary search
///
/// Finds the first index where `keys[index] >= key`
#[inline(always)]
fn lower_bound<K: Ord>(node: &BTreeNode<K, impl Sized>, key: &K) -> usize {
    let len = node.key_count();
    let mut size = len;
    let mut base = 0usize;

    while size > 1 {
        let half = size / 2;
        let mid = base + half;
        base = if unsafe { node.key(mid) } < key {
            mid
        } else {
            base
        };
        size -= half;
    }

    base + (size > 0 && unsafe { node.key(base) } < key) as usize
}

/// Branchless upper bound binary search
///
/// Finds the first index where `keys[index] > key`
#[inline(always)]
fn upper_bound<K: Ord>(node: &BTreeNode<K, impl Sized>, key: &K) -> usize {
    let len = node.key_count();
    let mut size = len;
    let mut base = 0usize;

    while size > 1 {
        let half = size / 2;
        let mid = base + half;
        base = if unsafe { node.key(mid) } <= key {
            mid
        } else {
            base
        };
        size -= half;
    }

    base + (size > 0 && unsafe { node.key(base) } <= key) as usize
}

impl<K: Ord + Clone, V: Clone> BTree<K, V> {
    pub fn new() -> Self {
        let mut tree = BTree {
            nodes: Vec::with_capacity(8192),
            root: 0,
        };
        tree.nodes.push(BTreeNode::new(true));
        tree
    }

    #[inline(always)]
    fn add_node(&mut self, node: BTreeNode<K, V>) -> usize {
        self.nodes.push(node);
        self.nodes.len() - 1
    }

    #[inline(always)]
    fn is_full(&self, index: usize) -> bool {
        unsafe { self.nodes.get_unchecked(index).key_count() >= MAX_KEYS }
    }

    pub fn insert(&mut self, key: K, value: V) {
        if self.is_full(self.root) {
            let old_root = self.root;
            let new_root = self.add_node(BTreeNode::new(false));
            unsafe {
                self.nodes.get_unchecked_mut(new_root).push_child(old_root);
            }
            self.root = new_root;
            self.split_child(new_root, 0);
        }

        let mut node_idx = self.root;

        loop {
            let is_leaf = unsafe { self.nodes.get_unchecked(node_idx).is_leaf };

            if is_leaf {
                let node = unsafe { self.nodes.get_unchecked_mut(node_idx) };
                let len = node.key_count();

                // Fast path for sequential/append inserts
                if len == 0 || key > *unsafe { node.key(len - 1) } {
                    unsafe { node.push_key_value(key, value) };
                } else {
                    let pos = lower_bound(node, &key);
                    unsafe { node.insert_key_value(pos, key, value) };
                }

                return;
            }

            let i = {
                let node = unsafe { self.nodes.get_unchecked(node_idx) };
                upper_bound(node, &key)
            };

            let child_idx = unsafe { self.nodes.get_unchecked(node_idx).child(i) };

            // Prefetch child node
            prefetch(unsafe { self.nodes.as_ptr().add(child_idx) });

            if self.is_full(child_idx) {
                self.split_child(node_idx, i);
                let go_right = key >= *unsafe { self.nodes.get_unchecked(node_idx).key(i) };
                if go_right {
                    node_idx = unsafe { self.nodes.get_unchecked(node_idx).child(i + 1) };
                } else {
                    node_idx = unsafe { self.nodes.get_unchecked(node_idx).child(i) };
                }
            } else {
                node_idx = child_idx;
            }
        }
    }

    fn split_child(&mut self, parent_idx: usize, i: usize) {
        let child_idx = unsafe { self.nodes.get_unchecked(parent_idx).child(i) };
        let child_is_leaf = unsafe { self.nodes.get_unchecked(child_idx).is_leaf };

        let new_node_idx = self.add_node(BTreeNode::new(child_is_leaf));

        if child_is_leaf {
            // For leaf nodes: copy right half to new node
            let split_key = unsafe { self.nodes.get_unchecked(child_idx).clone_key(T) };

            // Copy keys T..MAX_KEYS to new node
            let keys_to_copy = unsafe { self.nodes.get_unchecked(child_idx).key_count() } - T;
            unsafe {
                ptr::copy_nonoverlapping(
                    self.nodes.get_unchecked(child_idx).keys.as_ptr().add(T),
                    self.nodes.get_unchecked_mut(new_node_idx).keys.as_mut_ptr(),
                    keys_to_copy,
                );
                ptr::copy_nonoverlapping(
                    self.nodes.get_unchecked(child_idx).values.as_ptr().add(T),
                    self.nodes
                        .get_unchecked_mut(new_node_idx)
                        .values
                        .as_mut_ptr(),
                    keys_to_copy,
                );
                self.nodes.get_unchecked_mut(new_node_idx).key_count = keys_to_copy as u16;
                self.nodes.get_unchecked_mut(child_idx).key_count = T as u16;
            }

            // Update leaf linked list
            let child_next = unsafe { self.nodes.get_unchecked(child_idx).next };
            unsafe {
                self.nodes.get_unchecked_mut(child_idx).next = Some(new_node_idx);
                self.nodes.get_unchecked_mut(new_node_idx).next = child_next;
            }

            // Insert new child and separator key into parent
            unsafe {
                self.nodes
                    .get_unchecked_mut(parent_idx)
                    .insert_child(i + 1, new_node_idx);
                self.nodes
                    .get_unchecked_mut(parent_idx)
                    .insert_key(i, split_key);
            }
        } else {
            // For internal nodes: median goes up, right half to new node
            let median_key = unsafe { self.nodes.get_unchecked(child_idx).clone_key(T - 1) };

            // Copy keys T..key_count to new node (skip median at T-1)
            let child_key_count = unsafe { self.nodes.get_unchecked(child_idx).key_count() };
            let keys_to_copy = child_key_count - T;
            unsafe {
                ptr::copy_nonoverlapping(
                    self.nodes.get_unchecked(child_idx).keys.as_ptr().add(T),
                    self.nodes.get_unchecked_mut(new_node_idx).keys.as_mut_ptr(),
                    keys_to_copy,
                );
                self.nodes.get_unchecked_mut(new_node_idx).key_count = keys_to_copy as u16;
                // Child keeps keys 0..T-1
                self.nodes.get_unchecked_mut(child_idx).key_count = (T - 1) as u16;
            }

            // Copy children T..child_count to new node
            let child_count = unsafe { self.nodes.get_unchecked(child_idx).child_count() };
            let children_to_copy = child_count - T;
            unsafe {
                ptr::copy_nonoverlapping(
                    self.nodes.get_unchecked(child_idx).children.as_ptr().add(T),
                    self.nodes
                        .get_unchecked_mut(new_node_idx)
                        .children
                        .as_mut_ptr(),
                    children_to_copy,
                );
                self.nodes.get_unchecked_mut(new_node_idx).child_count = children_to_copy as u16;
                self.nodes.get_unchecked_mut(child_idx).child_count = T as u16;
            }

            // Insert new child and median key into parent
            unsafe {
                self.nodes
                    .get_unchecked_mut(parent_idx)
                    .insert_child(i + 1, new_node_idx);
                self.nodes
                    .get_unchecked_mut(parent_idx)
                    .insert_key(i, median_key);
            }
        }
    }

    #[inline(always)]
    pub fn get(&self, key: &K) -> Option<&V> {
        let mut node_idx = self.root;

        loop {
            let node = unsafe { self.nodes.get_unchecked(node_idx) };

            if node.is_leaf {
                let i = lower_bound(node, key);
                if i < node.key_count() && unsafe { node.key(i) } == key {
                    return Some(unsafe { node.value(i) });
                }
                return None;
            }

            let i = upper_bound(node, key);
            let next_idx = unsafe { node.child(i) };

            // Prefetch next node
            prefetch(unsafe { self.nodes.as_ptr().add(next_idx) });

            node_idx = next_idx;
        }
    }

    #[inline(always)]
    pub fn contains(&self, key: &K) -> bool {
        self.get(key).is_some()
    }

    #[inline(always)]
    fn find_first_leaf(&self) -> usize {
        let mut node_idx = self.root;
        loop {
            let node = unsafe { self.nodes.get_unchecked(node_idx) };
            if node.is_leaf {
                return node_idx;
            }
            node_idx = unsafe { node.child(0) };
        }
    }

    #[inline(always)]
    fn find_leaf(&self, key: &K) -> usize {
        let mut node_idx = self.root;
        loop {
            let node = unsafe { self.nodes.get_unchecked(node_idx) };
            if node.is_leaf {
                return node_idx;
            }
            let i = upper_bound(node, key);
            node_idx = unsafe { node.child(i) };
        }
    }

    #[inline]
    pub fn iter(&self) -> BTreeIter<'_, K, V> {
        BTreeIter {
            tree: self,
            current_node: Some(self.find_first_leaf()),
            current_index: 0,
        }
    }

    pub fn range(&self, start: &K, end: &K) -> Vec<(&K, &V)> {
        let mut results = Vec::new();
        let mut node_idx = self.find_leaf(start);

        'outer: loop {
            let node = unsafe { self.nodes.get_unchecked(node_idx) };
            let start_pos = lower_bound(node, start);

            for i in start_pos..node.key_count() {
                let key = unsafe { node.key(i) };
                if key >= end {
                    break 'outer;
                }
                results.push((key, unsafe { node.value(i) }));
            }

            match node.next {
                Some(next) => node_idx = next,
                None => break,
            }
        }

        results
    }

    pub fn range_inclusive(&self, start: &K, end: &K) -> Vec<(&K, &V)> {
        let mut results = Vec::new();
        let mut node_idx = self.find_leaf(start);

        'outer: loop {
            let node = unsafe { self.nodes.get_unchecked(node_idx) };
            let start_pos = lower_bound(node, start);

            for i in start_pos..node.key_count() {
                let key = unsafe { node.key(i) };
                if key > end {
                    break 'outer;
                }
                results.push((key, unsafe { node.value(i) }));
            }

            match node.next {
                Some(next) => node_idx = next,
                None => break,
            }
        }
        results
    }

    pub fn less_than(&self, key: &K) -> Vec<(&K, &V)> {
        let mut results = Vec::new();
        let mut node_idx = self.find_first_leaf();

        'outer: loop {
            let node = unsafe { self.nodes.get_unchecked(node_idx) };
            for i in 0..node.key_count() {
                let k = unsafe { node.key(i) };
                if k >= key {
                    break 'outer;
                }
                results.push((k, unsafe { node.value(i) }));
            }

            match node.next {
                Some(next) => node_idx = next,
                None => break,
            }
        }
        results
    }

    pub fn less_than_or_equal(&self, key: &K) -> Vec<(&K, &V)> {
        let mut results = Vec::new();
        let mut node_idx = self.find_first_leaf();

        'outer: loop {
            let node = unsafe { self.nodes.get_unchecked(node_idx) };
            for i in 0..node.key_count() {
                let k = unsafe { node.key(i) };
                if k > key {
                    break 'outer;
                }
                results.push((k, unsafe { node.value(i) }));
            }

            match node.next {
                Some(next) => node_idx = next,
                None => break,
            }
        }
        results
    }

    pub fn greater_than(&self, key: &K) -> Vec<(&K, &V)> {
        let mut results = Vec::new();
        let mut node_idx = self.find_leaf(key);

        loop {
            let node = unsafe { self.nodes.get_unchecked(node_idx) };
            let start_pos = upper_bound(node, key);

            for i in start_pos..node.key_count() {
                let k = unsafe { node.key(i) };
                results.push((k, unsafe { node.value(i) }));
            }

            match node.next {
                Some(next) => node_idx = next,
                None => break,
            }
        }
        results
    }

    pub fn greater_than_or_equal(&self, key: &K) -> Vec<(&K, &V)> {
        let mut results = Vec::new();
        let mut node_idx = self.find_leaf(key);

        loop {
            let node = unsafe { self.nodes.get_unchecked(node_idx) };
            let start_pos = lower_bound(node, key);

            for i in start_pos..node.key_count() {
                let k = unsafe { node.key(i) };
                results.push((k, unsafe { node.value(i) }));
            }

            match node.next {
                Some(next) => node_idx = next,
                None => break,
            }
        }
        results
    }

    #[inline(always)]
    pub fn min(&self) -> Option<(&K, &V)> {
        let node = unsafe { self.nodes.get_unchecked(self.find_first_leaf()) };
        if node.key_count() == 0 {
            None
        } else {
            Some(unsafe { (node.key(0), node.value(0)) })
        }
    }

    #[inline(always)]
    pub fn max(&self) -> Option<(&K, &V)> {
        let mut node_idx = self.root;
        loop {
            let node = unsafe { self.nodes.get_unchecked(node_idx) };
            if node.is_leaf {
                if node.key_count() == 0 {
                    return None;
                }
                let last = node.key_count() - 1;
                return Some(unsafe { (node.key(last), node.value(last)) });
            }
            node_idx = unsafe { node.child(node.child_count() - 1) };
        }
    }

    #[inline]
    pub fn count_range(&self, start: &K, end: &K) -> usize {
        self.range(start, end).len()
    }

    #[inline(always)]
    fn min_keys(&self) -> usize {
        T - 1
    }

    pub fn delete(&mut self, key: &K) -> Option<V> {
        self.delete_recursive(self.root, key, None, None)
    }

    fn delete_recursive(
        &mut self,
        node_idx: usize,
        key: &K,
        parent_idx: Option<usize>,
        child_index_in_parent: Option<usize>,
    ) -> Option<V> {
        let is_leaf = unsafe { self.nodes.get_unchecked(node_idx).is_leaf };

        if is_leaf {
            let pos = {
                let node = unsafe { self.nodes.get_unchecked(node_idx) };
                let i = lower_bound(node, key);
                if i < node.key_count() && unsafe { node.key(i) } == key {
                    i
                } else {
                    return None;
                }
            };

            let (_, value) =
                unsafe { self.nodes.get_unchecked_mut(node_idx).remove_key_value(pos) };

            if let (Some(parent), Some(idx)) = (parent_idx, child_index_in_parent) {
                if unsafe { self.nodes.get_unchecked(node_idx).key_count() } < self.min_keys() {
                    self.handle_underflow(parent, idx);
                }
            }

            Some(value)
        } else {
            let i = upper_bound(unsafe { self.nodes.get_unchecked(node_idx) }, key);
            let child_idx = unsafe { self.nodes.get_unchecked(node_idx).child(i) };
            let result = self.delete_recursive(child_idx, key, Some(node_idx), Some(i));

            if result.is_some() && i > 0 {
                let new_min = self.find_min_key(child_idx);
                if let Some(min_key) = new_min {
                    unsafe {
                        ptr::write(
                            self.nodes
                                .get_unchecked_mut(node_idx)
                                .keys
                                .get_unchecked_mut(i - 1)
                                .as_mut_ptr(),
                            min_key,
                        );
                    }
                }
            }

            if let (Some(parent), Some(idx)) = (parent_idx, child_index_in_parent) {
                if unsafe { self.nodes.get_unchecked(node_idx).key_count() } < self.min_keys() {
                    self.handle_underflow(parent, idx);
                }
            }

            let node = unsafe { self.nodes.get_unchecked(node_idx) };
            if node_idx == self.root && node.key_count() == 0 && node.child_count() > 0 {
                self.root = unsafe { node.child(0) };
            }

            result
        }
    }

    fn find_min_key(&self, node_idx: usize) -> Option<K> {
        let mut idx = node_idx;
        loop {
            let node = unsafe { self.nodes.get_unchecked(idx) };
            if node.is_leaf {
                return if node.key_count() > 0 {
                    Some(unsafe { node.clone_key(0) })
                } else {
                    None
                };
            }
            if node.child_count() == 0 {
                return None;
            }
            idx = unsafe { node.child(0) };
        }
    }

    fn handle_underflow(&mut self, parent_idx: usize, child_idx_in_parent: usize) {
        let parent_child_count = unsafe { self.nodes.get_unchecked(parent_idx).child_count() };

        if child_idx_in_parent > 0 {
            let left_sibling_idx = unsafe {
                self.nodes
                    .get_unchecked(parent_idx)
                    .child(child_idx_in_parent - 1)
            };
            if unsafe { self.nodes.get_unchecked(left_sibling_idx).key_count() } > self.min_keys() {
                self.borrow_from_left(parent_idx, child_idx_in_parent);
                return;
            }
        }

        if child_idx_in_parent < parent_child_count - 1 {
            let right_sibling_idx = unsafe {
                self.nodes
                    .get_unchecked(parent_idx)
                    .child(child_idx_in_parent + 1)
            };
            if unsafe { self.nodes.get_unchecked(right_sibling_idx).key_count() } > self.min_keys()
            {
                self.borrow_from_right(parent_idx, child_idx_in_parent);
                return;
            }
        }

        if child_idx_in_parent > 0 {
            self.merge_children(parent_idx, child_idx_in_parent - 1);
        } else {
            self.merge_children(parent_idx, child_idx_in_parent);
        }
    }

    fn borrow_from_left(&mut self, parent_idx: usize, child_idx_in_parent: usize) {
        let child_idx = unsafe {
            self.nodes
                .get_unchecked(parent_idx)
                .child(child_idx_in_parent)
        };
        let left_sibling_idx = unsafe {
            self.nodes
                .get_unchecked(parent_idx)
                .child(child_idx_in_parent - 1)
        };
        let child_is_leaf = unsafe { self.nodes.get_unchecked(child_idx).is_leaf };

        if child_is_leaf {
            let (key, value) = unsafe {
                self.nodes
                    .get_unchecked_mut(left_sibling_idx)
                    .pop_key_value()
            };
            unsafe {
                self.nodes
                    .get_unchecked_mut(child_idx)
                    .prepend_key_value(key, value)
            };
            let new_sep = unsafe { self.nodes.get_unchecked(child_idx).clone_key(0) };
            unsafe {
                ptr::write(
                    self.nodes
                        .get_unchecked_mut(parent_idx)
                        .keys
                        .get_unchecked_mut(child_idx_in_parent - 1)
                        .as_mut_ptr(),
                    new_sep,
                );
            }
        } else {
            let parent_key = unsafe {
                self.nodes
                    .get_unchecked(parent_idx)
                    .clone_key(child_idx_in_parent - 1)
            };
            let sibling_key = unsafe { self.nodes.get_unchecked_mut(left_sibling_idx).pop_key() };
            let sibling_child =
                unsafe { self.nodes.get_unchecked_mut(left_sibling_idx).pop_child() };
            unsafe {
                self.nodes
                    .get_unchecked_mut(child_idx)
                    .prepend_key(parent_key)
            };
            unsafe {
                self.nodes
                    .get_unchecked_mut(child_idx)
                    .prepend_child(sibling_child)
            };
            unsafe {
                ptr::write(
                    self.nodes
                        .get_unchecked_mut(parent_idx)
                        .keys
                        .get_unchecked_mut(child_idx_in_parent - 1)
                        .as_mut_ptr(),
                    sibling_key,
                );
            }
        }
    }

    fn borrow_from_right(&mut self, parent_idx: usize, child_idx_in_parent: usize) {
        let child_idx = unsafe {
            self.nodes
                .get_unchecked(parent_idx)
                .child(child_idx_in_parent)
        };
        let right_sibling_idx = unsafe {
            self.nodes
                .get_unchecked(parent_idx)
                .child(child_idx_in_parent + 1)
        };
        let child_is_leaf = unsafe { self.nodes.get_unchecked(child_idx).is_leaf };

        if child_is_leaf {
            let (key, value) = unsafe {
                self.nodes
                    .get_unchecked_mut(right_sibling_idx)
                    .remove_key_value(0)
            };
            unsafe {
                self.nodes
                    .get_unchecked_mut(child_idx)
                    .push_key_value(key, value)
            };
            let new_sep = unsafe { self.nodes.get_unchecked(right_sibling_idx).clone_key(0) };
            unsafe {
                ptr::write(
                    self.nodes
                        .get_unchecked_mut(parent_idx)
                        .keys
                        .get_unchecked_mut(child_idx_in_parent)
                        .as_mut_ptr(),
                    new_sep,
                );
            }
        } else {
            let parent_key = unsafe {
                self.nodes
                    .get_unchecked(parent_idx)
                    .clone_key(child_idx_in_parent)
            };
            let sibling_key = unsafe {
                self.nodes
                    .get_unchecked_mut(right_sibling_idx)
                    .remove_key(0)
            };
            let sibling_child = unsafe {
                self.nodes
                    .get_unchecked_mut(right_sibling_idx)
                    .remove_child(0)
            };
            unsafe {
                let child = self.nodes.get_unchecked_mut(child_idx);
                let insert_pos = child.key_count();
                child.insert_key(insert_pos, parent_key);
                child.push_child(sibling_child);
            }
            unsafe {
                ptr::write(
                    self.nodes
                        .get_unchecked_mut(parent_idx)
                        .keys
                        .get_unchecked_mut(child_idx_in_parent)
                        .as_mut_ptr(),
                    sibling_key,
                );
            }
        }
    }

    fn merge_children(&mut self, parent_idx: usize, i: usize) {
        let left_idx = unsafe { self.nodes.get_unchecked(parent_idx).child(i) };
        let right_idx = unsafe { self.nodes.get_unchecked(parent_idx).child(i + 1) };
        let is_leaf = unsafe { self.nodes.get_unchecked(left_idx).is_leaf };

        if is_leaf {
            // Copy all keys/values from right to left
            let right_count = unsafe { self.nodes.get_unchecked(right_idx).key_count() };
            let left_count = unsafe { self.nodes.get_unchecked(left_idx).key_count() };

            unsafe {
                ptr::copy_nonoverlapping(
                    self.nodes.get_unchecked(right_idx).keys.as_ptr(),
                    self.nodes
                        .get_unchecked_mut(left_idx)
                        .keys
                        .as_mut_ptr()
                        .add(left_count),
                    right_count,
                );
                ptr::copy_nonoverlapping(
                    self.nodes.get_unchecked(right_idx).values.as_ptr(),
                    self.nodes
                        .get_unchecked_mut(left_idx)
                        .values
                        .as_mut_ptr()
                        .add(left_count),
                    right_count,
                );
                self.nodes.get_unchecked_mut(left_idx).key_count =
                    (left_count + right_count) as u16;
            }

            // Update linked list
            let right_next = unsafe { self.nodes.get_unchecked(right_idx).next };
            unsafe { self.nodes.get_unchecked_mut(left_idx).next = right_next };
        } else {
            // For internal nodes: bring down separator key, then copy from right
            let parent_key = unsafe { self.nodes.get_unchecked(parent_idx).clone_key(i) };
            let left_count = unsafe { self.nodes.get_unchecked(left_idx).key_count() };
            let right_key_count = unsafe { self.nodes.get_unchecked(right_idx).key_count() };
            let left_child_count = unsafe { self.nodes.get_unchecked(left_idx).child_count() };
            let right_child_count = unsafe { self.nodes.get_unchecked(right_idx).child_count() };

            unsafe {
                // Add separator key
                self.nodes
                    .get_unchecked_mut(left_idx)
                    .keys
                    .get_unchecked_mut(left_count)
                    .write(parent_key);

                // Copy right keys
                ptr::copy_nonoverlapping(
                    self.nodes.get_unchecked(right_idx).keys.as_ptr(),
                    self.nodes
                        .get_unchecked_mut(left_idx)
                        .keys
                        .as_mut_ptr()
                        .add(left_count + 1),
                    right_key_count,
                );
                self.nodes.get_unchecked_mut(left_idx).key_count =
                    (left_count + 1 + right_key_count) as u16;

                // Copy right children
                ptr::copy_nonoverlapping(
                    self.nodes.get_unchecked(right_idx).children.as_ptr(),
                    self.nodes
                        .get_unchecked_mut(left_idx)
                        .children
                        .as_mut_ptr()
                        .add(left_child_count),
                    right_child_count,
                );
                self.nodes.get_unchecked_mut(left_idx).child_count =
                    (left_child_count + right_child_count) as u16;
            }
        }

        // Remove separator key and right child from parent
        unsafe {
            self.nodes.get_unchecked_mut(parent_idx).remove_key(i);
            self.nodes.get_unchecked_mut(parent_idx).remove_child(i + 1);
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.iter().count()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        unsafe { self.nodes.get_unchecked(self.root).key_count() == 0 }
    }

    pub fn get_node(&self, index: usize) -> Option<&BTreeNode<K, V>> {
        self.nodes.get(index)
    }
}

impl<K: Ord + Clone, V: Clone> Default for BTree<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

pub struct BTreeIter<'a, K, V> {
    tree: &'a BTree<K, V>,
    current_node: Option<usize>,
    current_index: usize,
}

impl<'a, K: Ord + Clone, V: Clone> Iterator for BTreeIter<'a, K, V> {
    type Item = (&'a K, &'a V);

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let node_idx = self.current_node?;
            let node = unsafe { self.tree.nodes.get_unchecked(node_idx) };

            if self.current_index < node.key_count() {
                let key = unsafe { node.key(self.current_index) };
                let value = unsafe { node.value(self.current_index) };
                self.current_index += 1;

                return Some((key, value));
            }

            self.current_node = node.next;
            self.current_index = 0;
        }
    }
}

impl<K: Debug + Clone, V: Debug + Clone> std::fmt::Display for BTree<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut table = Table::new();
        table.load_preset(UTF8_FULL);
        table.set_header(vec!["Index", "Keys", "Values", "Children", "Leaf", "Next"]);

        for (i, node) in self.nodes.iter().enumerate() {
            let keys: Vec<_> = (0..node.key_count())
                .map(|j| format!("{:?}", unsafe { node.key(j) }))
                .collect();
            let values: Vec<_> = if node.is_leaf {
                (0..node.key_count())
                    .map(|j| format!("{:?}", unsafe { node.value(j) }))
                    .collect()
            } else {
                vec!["-".to_string()]
            };
            let children: Vec<_> = (0..node.child_count())
                .map(|j| unsafe { node.child(j) }.to_string())
                .collect();

            table.add_row(vec![
                i.to_string(),
                format!("[{}]", keys.join(", ")),
                format!("[{}]", values.join(", ")),
                format!("[{}]", children.join(", ")),
                node.is_leaf.to_string(),
                node.next.map_or("-".to_string(), |n| n.to_string()),
            ]);
        }

        write!(f, "{table}")
    }
}
