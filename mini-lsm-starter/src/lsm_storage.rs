#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::collections::HashMap;
use std::fs::File;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::MemTable;
use crate::mvcc::LsmMvccInner;
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        if let Some(flush_thread) = self.flush_thread.lock().take() {
            self.flush_notifier.send(())?;
            flush_thread.join().unwrap();
        }
        if let Some(compaction_thread) = self.compaction_thread.lock().take() {
            self.compaction_notifier.send(())?;
            compaction_thread.join().unwrap();
        }

        // Flush all memtables
        if !self.inner.state.read().memtable.is_empty() {
            let _state_lock = self.inner.state_lock.lock();
            self.inner.force_freeze_memtable(&_state_lock)?;
        }

        while !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }

        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let mut state = LsmStorageState::create(&options);
        let mut next_sst_id = 0;
        let block_cache = Arc::new(BlockCache::new(1024));

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        if !path.exists() {
            std::fs::create_dir_all(path)?;
        }

        let manifest_path = path.join("MANIFEST");

        let manifest = if !manifest_path.exists() {
            // Starting afresh
            let manifest = Manifest::create(&manifest_path)?;
            manifest.add_record_when_init(ManifestRecord::NewMemtable(next_sst_id))?;
            next_sst_id += 1;
            manifest
        } else {
            let (manifest, records) = Manifest::recover(&manifest_path)?;
            for record in records {
                match record {
                    ManifestRecord::Flush(id) => {
                        if compaction_controller.flush_to_l0() {
                            state.l0_sstables.insert(0, id);
                        } else {
                            state.levels.insert(0, (id, vec![id]));
                        }
                        next_sst_id = next_sst_id.max(id + 1);
                    }
                    ManifestRecord::NewMemtable(id) => {
                        next_sst_id = next_sst_id.max(id + 1);
                    }
                    ManifestRecord::Compaction(task, output) => {
                        let (new_state, _removed_ssts) =
                            compaction_controller.apply_compaction_result(&state, &task, &output);
                        next_sst_id = next_sst_id.max(output.iter().max().unwrap() + 1);
                        state = new_state;
                    }
                }
            }

            // Load all ssts
            for &sst_id in state
                .l0_sstables
                .iter()
                .chain(state.levels.iter().flat_map(|(_id, ssts)| ssts.into_iter()))
            {
                let sst_path = Self::path_of_sst_static(path, sst_id);
                let table = SsTable::open(
                    sst_id,
                    Some(block_cache.clone()),
                    FileObject::open(&sst_path)?,
                )?;
                state.sstables.insert(sst_id, Arc::new(table));
            }

            // Sort SSTs on all levels
            for (_id, ssts) in state.levels.iter_mut() {
                ssts.sort_by_key(|id| {
                    let table = state.sstables.get(id).unwrap();
                    table.first_key()
                });
            }

            // Recover memtable
            state.memtable = Arc::new(MemTable::create(next_sst_id));
            next_sst_id += 1;
            manifest.add_record_when_init(ManifestRecord::NewMemtable(next_sst_id))?;

            manifest
        };

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(next_sst_id),
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        let value = snapshot.memtable.get(key);
        match value {
            None => Self::probe_imm_memtable(snapshot, key),
            Some(v) if v.is_empty() => Ok(None),
            Some(v) => Ok(Some(v)),
        }
    }

    fn probe_imm_memtable(snapshot: Arc<LsmStorageState>, key: &[u8]) -> Result<Option<Bytes>> {
        for imm_memtable in &snapshot.imm_memtables {
            match imm_memtable.get(key) {
                Some(v) if v.is_empty() => return Ok(None),
                Some(v) => return Ok(Some(v)),
                None => (),
            }
        }

        Self::probe_sstable(snapshot, key)
    }

    fn probe_sstable(snapshot: Arc<LsmStorageState>, key: &[u8]) -> Result<Option<Bytes>> {
        let h = farmhash::fingerprint32(key);
        let lower = Bound::Included(key);
        let upper = Bound::Unbounded;

        // Construct l0 sstable iterators
        let mut l0_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for id in &snapshot.l0_sstables {
            let table = Arc::clone(snapshot.sstables.get(id).unwrap());

            if let Some(bloom) = table.bloom.as_ref() {
                if !bloom.may_contain(h) {
                    continue;
                }
            }

            // Skip SSTs that does not contain the entry we are looking for.
            if !table.range_overlap(lower, upper) {
                continue;
            }

            let cur_sst_iter =
                SsTableIterator::create_and_seek_to_key(table, KeySlice::from_slice(key))?;

            l0_iters.push(Box::new(cur_sst_iter));
        }
        let l0_iter = MergeIterator::create(l0_iters);

        // Construct iterators over levels
        let mut level_iters = Vec::with_capacity(snapshot.levels.len());
        for (_, ssts) in &snapshot.levels {
            let tables = ssts
                .iter()
                .map(|id| snapshot.sstables.get(id).unwrap().clone())
                .collect();
            let concat_iter =
                SstConcatIterator::create_and_seek_to_key(tables, KeySlice::from_slice(key))?;
            level_iters.push(Box::new(concat_iter));
        }
        let level_iter = MergeIterator::create(level_iters);

        let iter = TwoMergeIterator::create(l0_iter, level_iter)?;

        if !iter.is_valid() {
            return Ok(None);
        }

        let cur_key = iter.key();
        if cur_key == KeySlice::from_slice(key) && !iter.value().is_empty() {
            return Ok(Some(Bytes::copy_from_slice(iter.value())));
        }

        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let size = {
            let guard = self.state.write();
            let _ = guard.memtable.put(key, value);
            guard.memtable.approximate_size()
        };

        self.try_freeze(size)?;

        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        const EMPTY_VALUE: &[u8] = &[];
        self.put(key, EMPTY_VALUE)
    }

    /// Try freeze memtable.
    fn try_freeze(&self, memtable_size: usize) -> Result<()> {
        if memtable_size >= self.options.target_sst_size {
            let state_lock = self.state_lock.lock();
            let read_guard = self.state.read();
            if read_guard.memtable.approximate_size() >= self.options.target_sst_size {
                drop(read_guard);
                return self.force_freeze_memtable(&state_lock);
            }
        }

        Ok(())
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let new_memtable_id = self.next_sst_id();
        let new_memtable = Arc::new(MemTable::create(new_memtable_id));

        let mut guard = self.state.write();
        let mut snapshot = guard.as_ref().clone();
        let old_memtable = std::mem::replace(&mut snapshot.memtable, new_memtable);
        snapshot.imm_memtables.insert(0, old_memtable);

        if let Some(manifest) = &self.manifest {
            let record = ManifestRecord::NewMemtable(new_memtable_id);
            manifest.add_record(_state_lock_observer, record)?;
        }

        self.sync_dir()?;

        *guard = Arc::new(snapshot);

        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let _state_guard = self.state_lock.lock();

        // Step 1: Take a read lock and get a cloned reference the earliest
        // immutable memtable
        let next_imm_memtable = {
            let guard = self.state.read();
            Arc::clone(guard.imm_memtables.last().unwrap())
        };

        // Step 2: Construct SsTable on the next memtable
        let mut builder = SsTableBuilder::new(self.options.block_size);
        next_imm_memtable.flush(&mut builder)?;
        let next_sst_id = self.next_sst_id();
        let next_sst_path = self.sst_path(next_sst_id)?;
        let table = builder.build(next_sst_id, Some(self.block_cache.clone()), next_sst_path)?;

        // Step 3: Take a write lock and modify the state snapshot
        let mut guard = self.state.write();
        let mut snapshot = guard.as_ref().clone();
        let _ = snapshot.imm_memtables.pop();
        snapshot.sstables.insert(next_sst_id, Arc::new(table));

        if self.compaction_controller.flush_to_l0() {
            snapshot.l0_sstables.insert(0, next_sst_id);
        } else {
            snapshot.levels.insert(0, (next_sst_id, vec![next_sst_id]));
        }

        // Update manifest
        if let Some(manifest) = self.manifest.as_ref() {
            let record = ManifestRecord::Flush(next_sst_id);
            manifest.add_record(&_state_guard, record)?;
        }

        self.sync_dir()?;

        *guard = Arc::new(snapshot);

        Ok(())
    }

    pub(crate) fn sst_path(&self, id: usize) -> Result<PathBuf> {
        let path = self.path_of_sst(id);

        // Ensure that the directory exists
        std::fs::create_dir_all(path.clone().parent().unwrap())?;
        // Create SST file
        let _ = std::fs::File::create(&path)?;
        Ok(path)
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        Self::scan_on_snapshot(snapshot, lower, upper)
    }

    fn scan_on_snapshot(
        snapshot: Arc<LsmStorageState>,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        // Construct memtable iterators
        let mut mem_iters = Vec::with_capacity(snapshot.imm_memtables.len() + 1);

        mem_iters.push(Box::new(snapshot.memtable.scan(lower, upper)));
        mem_iters.extend(
            snapshot
                .imm_memtables
                .iter()
                .map(|table| Box::new(table.scan(lower, upper))),
        );

        let mem_iter = MergeIterator::create(mem_iters);

        // Construct l0 sstable iterators
        let mut l0_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for id in &snapshot.l0_sstables {
            let table = Arc::clone(snapshot.sstables.get(id).unwrap());

            // Skip SSTs that does not contain the entry we are looking for.
            if !table.range_overlap(lower, upper) {
                continue;
            }

            let cur_sst_iter = match lower {
                Bound::Excluded(key) => {
                    let mut iter =
                        SsTableIterator::create_and_seek_to_key(table, KeySlice::from_slice(key))?;
                    if iter.is_valid() {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Included(key) => {
                    SsTableIterator::create_and_seek_to_key(table, KeySlice::from_slice(key))?
                }
                Bound::Unbounded => SsTableIterator::create_and_seek_to_first(table)?,
            };

            l0_iters.push(Box::new(cur_sst_iter));
        }
        let l0_iter = MergeIterator::create(l0_iters);

        let mem_l0_iter = TwoMergeIterator::create(mem_iter, l0_iter)?;

        let mut level_iters = Vec::with_capacity(snapshot.levels.len());
        for (_, ssts) in &snapshot.levels {
            let tables = ssts
                .iter()
                .map(|id| snapshot.sstables.get(id).unwrap().clone())
                .collect();
            let concat_iter = match lower {
                Bound::Excluded(key) => {
                    let mut iter = SstConcatIterator::create_and_seek_to_key(
                        tables,
                        KeySlice::from_slice(key),
                    )?;
                    if iter.is_valid() {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Included(key) => {
                    SstConcatIterator::create_and_seek_to_key(tables, KeySlice::from_slice(key))?
                }
                Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(tables)?,
            };
            level_iters.push(Box::new(concat_iter));
        }
        let level_iter = MergeIterator::create(level_iters);

        let lsm_iter_inner = TwoMergeIterator::create(mem_l0_iter, level_iter)?;
        Ok(FusedIterator::new(LsmIterator::new(lsm_iter_inner, upper)?))
    }
}
