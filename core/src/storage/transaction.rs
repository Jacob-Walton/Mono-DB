//! Transaction management

use crate::error::{MonoError, MonoResult};
use crate::storage::{Lsn, TxnId, WalManager, WalRecord};
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

/// Transaction isolation levels
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

/// Transaction states
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TransactionState {
    Active,
    Committed,
    Aborted,
}

/// A database transaction
pub struct Transaction {
    pub id: TxnId,
    pub state: RwLock<TransactionState>,
    pub isolation_level: IsolationLevel,
    pub read_set: RwLock<HashSet<(String, u32)>>,
    pub write_set: RwLock<HashSet<(String, u32)>>,
}

impl Transaction {
    /// Create a new transaction
    pub fn new(id: TxnId, isolation_level: IsolationLevel) -> Self {
        Self {
            id,
            state: RwLock::new(TransactionState::Active),
            isolation_level,
            read_set: RwLock::new(HashSet::new()),
            write_set: RwLock::new(HashSet::new()),
        }
    }

    /// Add to read set
    pub fn add_to_read_set(&self, table: &str, page_id: u32) {
        self.read_set.write().insert((table.to_string(), page_id));
    }

    /// Add to write set
    pub fn add_to_write_set(&self, table: &str, page_id: u32) {
        self.write_set.write().insert((table.to_string(), page_id));
    }

    /// Check if transaction is active
    pub fn is_active(&self) -> bool {
        *self.state.read() == TransactionState::Active
    }
}

/// Lock manager
pub struct LockManager {
    locks: RwLock<HashMap<(String, u32), LockInfo>>,
}

#[derive(Debug)]
struct LockInfo {
    exclusive_holder: Option<TxnId>,
    shared_holders: HashSet<TxnId>,
}

impl LockManager {
    pub fn new() -> Self {
        Self {
            locks: RwLock::new(HashMap::new()),
        }
    }

    /// Acquire a shared lock
    pub fn acquire_shared(&self, txn_id: TxnId, table: &str, page_id: u32) -> MonoResult<()> {
        let mut locks = self.locks.write();
        let key = (table.to_string(), page_id);

        let lock_info = locks.entry(key).or_insert_with(|| LockInfo {
            exclusive_holder: None,
            shared_holders: HashSet::new(),
        });

        // Check for exclusive lock
        if let Some(holder) = lock_info.exclusive_holder {
            if holder != txn_id {
                return Err(MonoError::Transaction("Page is exclusively locked".into()));
            }
        }

        lock_info.shared_holders.insert(txn_id);
        Ok(())
    }

    /// Acquire an exclusive lock
    pub fn acquire_exclusive(&self, txn_id: TxnId, table: &str, page_id: u32) -> MonoResult<()> {
        let mut locks = self.locks.write();
        let key = (table.to_string(), page_id);

        let lock_info = locks.entry(key).or_insert_with(|| LockInfo {
            exclusive_holder: None,
            shared_holders: HashSet::new(),
        });

        // Check for other holders
        if lock_info.exclusive_holder.is_some() && lock_info.exclusive_holder != Some(txn_id) {
            return Err(MonoError::Transaction(
                "Page is already exclusively locked".into(),
            ));
        }

        if !lock_info.shared_holders.is_empty()
            && !(lock_info.shared_holders.len() == 1 && lock_info.shared_holders.contains(&txn_id))
        {
            return Err(MonoError::Transaction(
                "Page has other shared lock holders".into(),
            ));
        }

        lock_info.exclusive_holder = Some(txn_id);
        lock_info.shared_holders.remove(&txn_id);
        Ok(())
    }

    /// Release all locks held by a transaction
    pub fn release_all(&self, txn_id: TxnId) {
        let mut locks = self.locks.write();

        locks.retain(|_, lock_info| {
            lock_info.shared_holders.remove(&txn_id);
            if lock_info.exclusive_holder == Some(txn_id) {
                lock_info.exclusive_holder = None;
            }

            !lock_info.shared_holders.is_empty() || lock_info.exclusive_holder.is_some()
        });
    }
}

/// Transaction manager
pub struct TransactionManager {
    next_txn_id: AtomicU64,
    active_transactions: RwLock<HashMap<TxnId, Arc<Transaction>>>,
    wal_manager: Arc<WalManager>,
    lock_manager: Arc<LockManager>,
}

impl TransactionManager {
    /// Create a new transaction manager
    pub fn new(wal_manager: Arc<WalManager>) -> Self {
        Self {
            next_txn_id: AtomicU64::new(1),
            active_transactions: RwLock::new(HashMap::new()),
            wal_manager,
            lock_manager: Arc::new(LockManager::new()),
        }
    }

    /// Begin a new transaction
    pub fn begin_transaction(
        &self,
        isolation_level: IsolationLevel,
    ) -> MonoResult<Arc<Transaction>> {
        let txn_id = TxnId(self.next_txn_id.fetch_add(1, Ordering::SeqCst));
        let transaction = Arc::new(Transaction::new(txn_id, isolation_level));

        self.wal_manager.write(WalRecord::Begin { txn_id })?;

        self.active_transactions
            .write()
            .insert(txn_id, Arc::clone(&transaction));

        Ok(transaction)
    }

    /// Commit a transaction
    pub fn commit_transaction(&self, txn_id: TxnId) -> MonoResult<()> {
        let transaction = {
            let txns = self.active_transactions.read();
            txns.get(&txn_id)
                .cloned()
                .ok_or_else(|| MonoError::Transaction("Transaction not found".into()))?
        };

        if !transaction.is_active() {
            return Err(MonoError::Transaction("Transaction is not active".into()));
        }

        // Write commit record
        self.wal_manager.write(WalRecord::Commit { txn_id })?;

        // Update state
        *transaction.state.write() = TransactionState::Committed;

        // Release locks
        self.lock_manager.release_all(txn_id);

        // Remove from active transactions
        self.active_transactions.write().remove(&txn_id);

        Ok(())
    }

    /// Abort a transaction
    pub fn abort_transaction(&self, txn_id: TxnId) -> MonoResult<()> {
        let transaction = {
            let txns = self.active_transactions.read();
            txns.get(&txn_id)
                .cloned()
                .ok_or_else(|| MonoError::Transaction("Transaction not found".into()))?
        };

        if !transaction.is_active() {
            return Err(MonoError::Transaction("Transaction is not active".into()));
        }

        // Write abort record
        self.wal_manager.write(WalRecord::Abort { txn_id })?;

        // Update state
        *transaction.state.write() = TransactionState::Aborted;

        // Release locks
        self.lock_manager.release_all(txn_id);

        // Remove from active transactions
        self.active_transactions.write().remove(&txn_id);

        Ok(())
    }

    /// Acquire a lock
    pub fn acquire_lock(
        &self,
        txn_id: TxnId,
        table: &str,
        page_id: u32,
        exclusive: bool,
    ) -> MonoResult<()> {
        if exclusive {
            self.lock_manager.acquire_exclusive(txn_id, table, page_id)
        } else {
            self.lock_manager.acquire_shared(txn_id, table, page_id)
        }
    }

    /// Get active transaction count
    pub fn active_transaction_count(&self) -> usize {
        self.active_transactions.read().len()
    }

    /// Get a transaction by ID
    pub fn get_transaction(&self, txn_id: TxnId) -> Option<Arc<Transaction>> {
        self.active_transactions.read().get(&txn_id).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_transaction_lifecycle() {
        let temp_dir = tempdir().unwrap();
        let wal_path = temp_dir.path().join("test.wal");
        let wal_manager = Arc::new(WalManager::new(wal_path).unwrap());
        let txn_manager = TransactionManager::new(wal_manager);

        // Begin transaction
        let txn = txn_manager
            .begin_transaction(IsolationLevel::ReadCommitted)
            .unwrap();
        assert!(txn.is_active());

        // Commit transaction
        txn_manager.commit_transaction(txn.id).unwrap();
        assert_eq!(*txn.state.read(), TransactionState::Committed);
    }

    #[test]
    fn test_lock_manager() {
        let lock_manager = LockManager::new();
        let txn1 = TxnId(1);
        let txn2 = TxnId(2);

        // Acquire shared locks
        lock_manager.acquire_shared(txn1, "test", 0).unwrap();
        lock_manager.acquire_shared(txn2, "test", 0).unwrap();

        // Cannot acquire exclusive lock
        assert!(lock_manager.acquire_exclusive(txn2, "test", 0).is_err());

        // Release locks
        lock_manager.release_all(txn1);
        lock_manager.release_all(txn2);

        // Now can acquire exclusive
        lock_manager.acquire_exclusive(txn1, "test", 0).unwrap();
    }
}
