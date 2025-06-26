//! Write-Ahead Logging implementation

use crate::error::{MonoError, MonoResult};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Mutex;

/// Log Sequence Number
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Lsn(pub u64);

/// Transaction ID
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TxnId(pub u64);

/// WAL record types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalRecord {
	Begin {
		txn_id: TxnId,
	},
	Commit {
		txn_id: TxnId,
	},
	Abort {
		txn_id: TxnId,
	},
	Insert {
		txn_id: TxnId,
		table_id: u32,
		page_id: u32,
		tuple_data: Vec<u8>,
	},
	Update {
		txn_id: TxnId,
		table_id: u32,
		page_id: u32,
		old_data: Vec<u8>,
		new_data: Vec<u8>,
	},
	Delete {
		txn_id: TxnId,
		table_id: u32,
		page_id: u32,
		tuple_data: Vec<u8>,
	},
	Checkpoint,
}

/// WAL entry with metadata
#[derive(Debug, Serialize, Deserialize)]
pub struct WalEntry {
	pub lsn: Lsn,
	pub record: WalRecord,
	pub timestamp: u64,
}

/// WAL manager
pub struct WalManager {
	file: Mutex<File>,
	current_lsn: Mutex<Lsn>,
}

impl WalManager {
	/// Create a new WAL manager
	pub fn new<P: AsRef<Path>>(path: P) -> MonoResult<Self> {
		let file = OpenOptions::new()
			.create(true)
			.read(true)
			.write(true)
			.open(path)?;

		let current_lsn = Self::find_last_lsn(&file)?;

		Ok(Self {
			file: Mutex::new(file),
			current_lsn: Mutex::new(current_lsn),
		})
	}

	/// Write a WAL record
	pub fn write(&self, record: WalRecord) -> MonoResult<Lsn> {
		let mut file = self.file.lock().unwrap();
		let mut current_lsn = self.current_lsn.lock().unwrap();

		// Increment LSN
		current_lsn.0 += 1;
		let lsn = *current_lsn;

		let entry = WalEntry {
			lsn,
			record,
			timestamp: std::time::SystemTime::now()
				.duration_since(std::time::UNIX_EPOCH)
				.unwrap()
				.as_secs(),
		};

		// Serialize entry using bincode
		let data = bincode::serialize(&entry)
			.map_err(|e| MonoError::Wal(format!("Failed to serialize WAL entry: {}", e)))?;

		// Write length prefix
		let len = data.len() as u32;
		file.seek(SeekFrom::End(0))?;
		file.write_all(&len.to_le_bytes())?;
		file.write_all(&data)?;
		file.sync_all()?;

		Ok(lsn)
	}

	/// Recover from WAL
	pub fn recover(&self) -> MonoResult<Vec<WalEntry>> {
		let mut file = self.file.lock().unwrap();
		file.seek(SeekFrom::Start(0))?;

		let mut entries = Vec::new();

		loop {
			// Read length prefix
			let mut len_bytes = [0u8; 4];
			match file.read_exact(&mut len_bytes) {
				Ok(_) => {}
				Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
				Err(e) => return Err(MonoError::Io(e)),
			}

			let len = u32::from_le_bytes(len_bytes) as usize;

			// Sanity check
			if len > 1024 * 1024 {
				eprintln!("WAL entry too large: {} bytes", len);
				break;
			}

			// Read entry data
			let mut data = vec![0u8; len];
			file.read_exact(&mut data)?;

			// Deserialize entry
			match bincode::deserialize::<WalEntry>(&data) {
				Ok(entry) => entries.push(entry),
				Err(e) => {
					eprintln!("Failed to deserialize WAL entry: {}", e);
					break;
				}
			}
		}

		Ok(entries)
	}

	/// Checkpoint the WAL
	pub fn checkpoint(&self) -> MonoResult<()> {
		self.write(WalRecord::Checkpoint)?;
		Ok(())
	}

	/// Get current LSN
	pub fn current_lsn(&self) -> Lsn {
		*self.current_lsn.lock().unwrap()
	}

	/// Find the last LSN in the file
	fn find_last_lsn(file: &File) -> MonoResult<Lsn> {
		let mut file = file.try_clone()?;
		file.seek(SeekFrom::Start(0))?;

		let mut last_lsn = Lsn(0);

		loop {
			// Read length prefix
			let mut len_bytes = [0u8; 4];
			match file.read_exact(&mut len_bytes) {
				Ok(_) => {}
				Err(_) => break,
			}

			let len = u32::from_le_bytes(len_bytes) as usize;

			// Read entry data
			let mut data = vec![0u8; len];
			if file.read_exact(&mut data).is_err() {
				break;
			}

			// Try to get LSN
			if let Ok(entry) = bincode::deserialize::<WalEntry>(&data) {
				last_lsn = entry.lsn;
			}
		}

		Ok(last_lsn)
	}

	/// Flush WAL to disk
	pub fn flush(&self) -> MonoResult<()> {
		let file = self.file.lock().unwrap();
		file.sync_all()?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use tempfile::tempdir;

	#[test]
	fn test_wal_write_and_recover() {
		let dir = tempdir().unwrap();
		let wal_path = dir.path().join("test.wal");

		let wal = WalManager::new(&wal_path).unwrap();

		// Write some records
		let txn_id = TxnId(1);
		wal.write(WalRecord::Begin { txn_id }).unwrap();
		wal.write(WalRecord::Insert {
			txn_id,
			table_id: 1,
			page_id: 0,
			tuple_data: b"test data".to_vec(),
		})
		.unwrap();
		wal.write(WalRecord::Commit { txn_id }).unwrap();

		// Recover and verify
		let entries = wal.recover().unwrap();
		assert_eq!(entries.len(), 3);

		match &entries[0].record {
			WalRecord::Begin {
				txn_id: recovered_txn,
			} => assert_eq!(*recovered_txn, txn_id),
			_ => panic!("Expected Begin record"),
		}
	}
}
