//! Memory manager implementation
//!
//! Provides memory allocation and management for database operations.

use crate::{MonoError, MonoResult};
use std::alloc::{Layout, alloc, dealloc};
use std::collections::HashMap;
use std::ptr::NonNull;
use std::sync::{Arc, Mutex};

/// Memory manager
pub struct MemoryManager {
	pools: HashMap<usize, MemoryPool>,
	total_allocated: usize,
	max_memory: usize,
}

/// A memory pool for allocations of a specific size
pub struct MemoryPool {
	block_size: usize,
	free_blocks: Vec<NonNull<u8>>,
	allocated_blocks: usize,
	max_blocks: usize,
}

/// Represents an allocated block of memory
pub struct Allocation {
	ptr: NonNull<u8>,
	size: usize,
	pool_size: usize,
}

impl MemoryManager {
	/// Create a new memory manager with the given maximum memory limit
	pub fn new(max_memory: usize) -> Self {
		Self {
			pools: HashMap::new(),
			total_allocated: 0,
			max_memory,
		}
	}

	/// Allocate memory of the specified size
	pub fn allocate(&mut self, size: usize) -> MonoResult<Allocation> {
		if size == 0 {
			return Err(MonoError::InvalidInput(
				"Cannot allocate zero bytes".to_string(),
			));
		}

		// Round up to the nearest power of 2
		let pool_size = size.next_power_of_two().max(8);

		// Check if we would exceed the memory limit
		if self.total_allocated + pool_size > self.max_memory {
			return Err(MonoError::OutOfMemory);
		}

		// Get or create the appropriate pool
		let pool = self
			.pools
			.entry(pool_size)
			.or_insert_with(|| MemoryPool::new(pool_size, self.max_memory / pool_size));

		// Allocate from the pool
		let ptr = pool.allocate()?;
		self.total_allocated += pool_size;

		Ok(Allocation {
			ptr,
			size,
			pool_size,
		})
	}

	/// Deallocate memory
	pub fn deallocate(&mut self, allocation: Allocation) -> MonoResult<()> {
		let pool = self
			.pools
			.get_mut(&allocation.pool_size)
			.ok_or_else(|| MonoError::InvalidInput("Invalid allocation".to_string()))?;

		pool.deallocate(allocation.ptr)?;
		self.total_allocated -= allocation.pool_size;

		Ok(())
	}

	/// Get current memory usage statistics
	pub fn get_stats(&self) -> MemoryStats {
		let pool_stats: Vec<PoolStats> = self
			.pools
			.iter()
			.map(|(size, pool)| PoolStats {
				block_size: *size,
				allocated_blocks: pool.allocated_blocks,
				free_blocks: pool.free_blocks.len(),
				max_blocks: pool.max_blocks,
			})
			.collect();

		MemoryStats {
			total_allocated: self.total_allocated,
			max_memory: self.max_memory,
			pool_stats,
		}
	}

	/// Compact memory pools by releasing unused blocks
	pub fn compact(&mut self) -> MonoResult<usize> {
		let mut released = 0;

		for pool in self.pools.values_mut() {
			released += pool.compact()?;
		}

		self.total_allocated -= released;
		Ok(released)
	}
}

impl MemoryPool {
	/// Create a new memory pool
	fn new(block_size: usize, max_blocks: usize) -> Self {
		Self {
			block_size,
			free_blocks: Vec::new(),
			allocated_blocks: 0,
			max_blocks,
		}
	}

	/// Allocate a block from this pool
	fn allocate(&mut self) -> MonoResult<NonNull<u8>> {
		// Try to reuse a free block
		if let Some(ptr) = self.free_blocks.pop() {
			self.allocated_blocks += 1;
			return Ok(ptr);
		}

		// Check if we can allocate a new block
		if self.allocated_blocks >= self.max_blocks {
			return Err(MonoError::OutOfMemory);
		}

		// Allocate a new block
		let layout = Layout::from_size_align(self.block_size, 8)
			.map_err(|_| MonoError::InvalidInput("Invalid layout".to_string()))?;

		let ptr = unsafe { alloc(layout) };
		if ptr.is_null() {
			return Err(MonoError::OutOfMemory);
		}

		self.allocated_blocks += 1;
		Ok(NonNull::new(ptr).unwrap())
	}

	/// Deallocate a block back to this pool
	fn deallocate(&mut self, ptr: NonNull<u8>) -> MonoResult<()> {
		self.free_blocks.push(ptr);
		self.allocated_blocks -= 1;
		Ok(())
	}

	/// Compact this pool by releasing some free blocks
	fn compact(&mut self) -> MonoResult<usize> {
		let blocks_to_release = self.free_blocks.len() / 2; // Release half of free blocks
		let mut released_bytes = 0;

		let layout = Layout::from_size_align(self.block_size, 8)
			.map_err(|_| MonoError::InvalidInput("Invalid layout".to_string()))?;

		for _ in 0..blocks_to_release {
			if let Some(ptr) = self.free_blocks.pop() {
				unsafe {
					dealloc(ptr.as_ptr(), layout);
				}
				released_bytes += self.block_size;
			}
		}

		Ok(released_bytes)
	}
}

impl Allocation {
	/// Get a pointer to the allocated memory
	pub fn as_ptr(&self) -> *mut u8 {
		self.ptr.as_ptr()
	}

	/// Get the size of the allocation
	pub fn size(&self) -> usize {
		self.size
	}

	/// Get a slice view of the allocated memory
	pub unsafe fn as_slice(&self) -> &[u8] {
		unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.size) }
	}

	/// Get a mutable slice view of the allocated memory
	pub unsafe fn as_mut_slice(&mut self) -> &mut [u8] {
		unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.size) }
	}
}

unsafe impl Send for Allocation {}
unsafe impl Sync for Allocation {}

/// Memory usage statistics
#[derive(Debug, Clone)]
pub struct MemoryStats {
	pub total_allocated: usize,
	pub max_memory: usize,
	pub pool_stats: Vec<PoolStats>,
}

/// Statistics for a single memory pool
#[derive(Debug, Clone)]
pub struct PoolStats {
	pub block_size: usize,
	pub allocated_blocks: usize,
	pub free_blocks: usize,
	pub max_blocks: usize,
}

impl MemoryStats {
	/// Get the memory utilization as a percentage
	pub fn utilization_percent(&self) -> f64 {
		if self.max_memory == 0 {
			0.0
		} else {
			(self.total_allocated as f64 / self.max_memory as f64) * 100.0
		}
	}
}

/// Thread-safe wrapper around MemoryManager
pub type SharedMemoryManager = Arc<Mutex<MemoryManager>>;
