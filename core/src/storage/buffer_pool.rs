//! Buffer pool implementation

use crate::error::{MonoError, MonoResult};
use crate::storage::{DiskManager, Page, PageId, PageType, TableSchema};
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::{
	Arc,
	atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};
use std::time::{SystemTime, UNIX_EPOCH};

/// Frame ID in the buffer pool
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FrameId(pub usize);

/// Buffer pool frame
pub struct Frame {
	pub page: RwLock<Option<Page>>,
	pub table_name: RwLock<Option<String>>,
	pub page_id: RwLock<Option<PageId>>,
	pub table_schema: RwLock<Option<TableSchema>>,
	pub pin_count: AtomicUsize,
	pub is_dirty: AtomicBool,
	pub last_accessed: AtomicU64,
}

impl Frame {
	fn new() -> Self {
		Self {
			page: RwLock::new(None),
			table_name: RwLock::new(None),
			page_id: RwLock::new(None),
			table_schema: RwLock::new(None),
			pin_count: AtomicUsize::new(0),
			is_dirty: AtomicBool::new(false),
			last_accessed: AtomicU64::new(0),
		}
	}

	fn update_access_time(&self) {
		let now = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.unwrap()
			.as_secs();
		self.last_accessed.store(now, Ordering::Relaxed);
	}
}

/// Buffer pool for caching pages
pub struct BufferPool {
	frames: Vec<Arc<Frame>>,
	page_table: Mutex<HashMap<(String, PageId), FrameId>>,
	disk_manager: Arc<DiskManager>,
	clock_hand: AtomicUsize,
}

impl BufferPool {
	/// Create a new buffer pool
	pub fn new(size: usize, disk_manager: Arc<DiskManager>) -> Self {
		let mut frames = Vec::with_capacity(size);
		for _ in 0..size {
			frames.push(Arc::new(Frame::new()));
		}

		Self {
			frames,
			page_table: Mutex::new(HashMap::new()),
			disk_manager,
			clock_hand: AtomicUsize::new(0),
		}
	}

	/// Get frame ID from frame reference
	fn get_frame_id(&self, frame: &Arc<Frame>) -> FrameId {
		// Find the frame in our vector
		for (i, f) in self.frames.iter().enumerate() {
			if Arc::ptr_eq(f, frame) {
				return FrameId(i);
			}
		}
		// This should never happen
		panic!("Frame not found in buffer pool");
	}

	/// Fetch a page from the buffer pool
	pub fn fetch_page(&self, table_name: &str, page_id: PageId) -> MonoResult<Arc<Frame>> {
		// Check if page is already in buffer
		{
			let page_table = self.page_table.lock();
			if let Some(&frame_id) = page_table.get(&(table_name.to_string(), page_id)) {
				let frame = &self.frames[frame_id.0];
				frame.pin_count.fetch_add(1, Ordering::AcqRel);
				frame.update_access_time();
				return Ok(Arc::clone(frame));
			}
		}

		// Find a victim frame
		let victim_frame = self.find_victim_frame()?;

		// Evict old page if necessary
		self.evict_frame(&victim_frame)?;

		// Load new page
		let page = self.disk_manager.read_page(table_name, page_id)?;

		// Install new page
		{
			let mut page_guard = victim_frame.page.write();
			let mut table_guard = victim_frame.table_name.write();
			let mut id_guard = victim_frame.page_id.write();

			*page_guard = Some(page);
			*table_guard = Some(table_name.to_string());
			*id_guard = Some(page_id);
		}

		// Update page table
		{
			let mut page_table = self.page_table.lock();
			let frame_id = self.get_frame_id(&victim_frame);
			page_table.insert((table_name.to_string(), page_id), frame_id);
		}

		victim_frame.pin_count.fetch_add(1, Ordering::AcqRel);
		victim_frame.update_access_time();
		victim_frame.is_dirty.store(false, Ordering::Relaxed);

		Ok(Arc::clone(&victim_frame))
	}

	/// Fetch a page from the buffer pool with schema information
	pub fn fetch_page_with_schema(
		&self,
		table_name: &str,
		page_id: PageId,
		schema: Option<TableSchema>,
	) -> MonoResult<Arc<Frame>> {
		// Check if page is already in buffer
		{
			let page_table = self.page_table.lock();
			if let Some(&frame_id) = page_table.get(&(table_name.to_string(), page_id)) {
				let frame = &self.frames[frame_id.0];

				// Update schema if provided
				if let Some(schema) = schema {
					*frame.table_schema.write() = Some(schema);
				}

				frame.pin_count.fetch_add(1, Ordering::AcqRel);
				frame.update_access_time();
				return Ok(Arc::clone(frame));
			}
		}

		// Find a victim frame
		let victim_frame = self.find_victim_frame()?;

		// Evict old page if necessary
		self.evict_frame(&victim_frame)?;

		// Load new page
		let page = self.disk_manager.read_page(table_name, page_id)?;

		// Install new page
		{
			let mut page_guard = victim_frame.page.write();
			let mut table_guard = victim_frame.table_name.write();
			let mut id_guard = victim_frame.page_id.write();
			let mut schema_guard = victim_frame.table_schema.write();

			*page_guard = Some(page);
			*table_guard = Some(table_name.to_string());
			*id_guard = Some(page_id);
			*schema_guard = schema;
		}

		// Update page table
		{
			let mut page_table = self.page_table.lock();
			let frame_id = self.get_frame_id(&victim_frame);
			page_table.insert((table_name.to_string(), page_id), frame_id);
		}

		victim_frame.pin_count.fetch_add(1, Ordering::AcqRel);
		victim_frame.update_access_time();
		victim_frame.is_dirty.store(false, Ordering::Relaxed);

		Ok(Arc::clone(&victim_frame))
	}

	/// Create a new page
	pub fn new_page(
		&self,
		table_name: &str,
		page_type: PageType,
	) -> MonoResult<(PageId, Arc<Frame>)> {
		let page_id = self.disk_manager.allocate_page(table_name)?;
		let page = Page::new(page_id, page_type);

		// Find a victim frame
		let victim_frame = self.find_victim_frame()?;

		// Evict old page if necessary
		self.evict_frame(&victim_frame)?;

		// Install new page
		{
			let mut page_guard = victim_frame.page.write();
			let mut table_guard = victim_frame.table_name.write();
			let mut id_guard = victim_frame.page_id.write();

			*page_guard = Some(page);
			*table_guard = Some(table_name.to_string());
			*id_guard = Some(page_id);
		}

		// Update page table
		{
			let mut page_table = self.page_table.lock();
			let frame_id = self.get_frame_id(&victim_frame);
			page_table.insert((table_name.to_string(), page_id), frame_id);
		}

		victim_frame.pin_count.fetch_add(1, Ordering::AcqRel);
		victim_frame.update_access_time();
		victim_frame.is_dirty.store(true, Ordering::Relaxed);

		Ok((page_id, Arc::clone(&victim_frame)))
	}

	/// Unpin a page
	pub fn unpin_page(&self, table_name: &str, page_id: PageId, is_dirty: bool) -> MonoResult<()> {
		let page_table = self.page_table.lock();
		if let Some(&frame_id) = page_table.get(&(table_name.to_string(), page_id)) {
			let frame = &self.frames[frame_id.0];

			if is_dirty {
				frame.is_dirty.store(true, Ordering::Relaxed);
			}

			let old_count = frame.pin_count.fetch_sub(1, Ordering::AcqRel);
			if old_count == 0 {
				return Err(MonoError::Storage("Unpinning already unpinned page".into()));
			}
		}

		Ok(())
	}

	/// Flush all dirty pages
	pub fn flush_all(&self) -> MonoResult<()> {
		for frame in &self.frames {
			if frame.is_dirty.load(Ordering::Relaxed) {
				self.flush_frame(frame)?;
			}
		}
		Ok(())
	}

	/// Find a victim frame using clock algorithm
	fn find_victim_frame(&self) -> MonoResult<Arc<Frame>> {
		let num_frames = self.frames.len();
		let mut clock_hand = self.clock_hand.load(Ordering::Relaxed);

		for _ in 0..num_frames * 2 {
			let frame = &self.frames[clock_hand % num_frames];

			if frame.pin_count.load(Ordering::Acquire) == 0 {
				self.clock_hand
					.store((clock_hand + 1) % num_frames, Ordering::Relaxed);
				return Ok(Arc::clone(frame));
			}

			clock_hand = (clock_hand + 1) % num_frames;
		}

		Err(MonoError::Storage("All frames are pinned".into()))
	}

	/// Evict a frame
	fn evict_frame(&self, frame: &Frame) -> MonoResult<()> {
		let table_name = frame.table_name.read().clone();
		let page_id = frame.page_id.read().clone();

		if let (Some(table), Some(id)) = (table_name, page_id) {
			// Flush if dirty
			if frame.is_dirty.load(Ordering::Relaxed) {
				self.flush_frame(frame)?;
			}

			// Remove from page table
			let mut page_table = self.page_table.lock();
			page_table.remove(&(table, id));
		}

		Ok(())
	}

	/// Flush a frame to disk
	fn flush_frame(&self, frame: &Frame) -> MonoResult<()> {
		let page_guard = frame.page.read();
		let table_guard = frame.table_name.read();

		if let (Some(ref page), Some(ref table)) = (page_guard.as_ref(), table_guard.as_ref()) {
			self.disk_manager.write_page(table, page)?;
			frame.is_dirty.store(false, Ordering::Relaxed);
		}

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use tempfile::tempdir;

	#[test]
	fn test_buffer_pool() {
		let temp_dir = tempdir().unwrap();
		let disk_manager = Arc::new(DiskManager::new(temp_dir.path()).unwrap());
		let buffer_pool = BufferPool::new(10, disk_manager);

		// Create a new page
		let (page_id, frame) = buffer_pool.new_page("test", PageType::Table).unwrap();

		// Modify the page
		{
			let mut page_guard = frame.page.write();
			if let Some(ref mut page) = *page_guard {
				page.add_tuple(b"test data").unwrap();
			}
		}

		// Unpin the page
		buffer_pool.unpin_page("test", page_id, true).unwrap();

		// Fetch it again
		let frame2 = buffer_pool.fetch_page("test", page_id).unwrap();
		let page_guard = frame2.page.read();
		let page = page_guard.as_ref().unwrap();
		assert_eq!(page.get_tuple(0).unwrap(), b"test data");
	}
}
