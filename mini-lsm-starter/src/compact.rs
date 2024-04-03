#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use parking_lot::MutexGuard;
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::manifest::ManifestRecord;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        match task {
            CompactionTask::Leveled(task) => self.compact_leveled(task),
            CompactionTask::Tiered(task) => self.compact_tiered(task),
            CompactionTask::Simple(task) => self.compact_simple(task),
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => self.compact_full(l0_sstables, l1_sstables),
        }
    }

    fn compact_leveled(&self, task: &LeveledCompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        match task.upper_level {
            Some(_level) => {
                let iter = Self::build_levels_iter(
                    &snapshot,
                    &task.upper_level_sst_ids,
                    &task.lower_level_sst_ids,
                )?;
                self.compact_inner(iter)
            }
            None => {
                let iter = Self::build_l0_and_l1_iter(
                    &snapshot,
                    &task.upper_level_sst_ids,
                    &task.lower_level_sst_ids,
                )?;
                self.compact_inner(iter)
            }
        }
    }

    fn compact_tiered(&self, task: &TieredCompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        let mut iters = Vec::with_capacity(task.tiers.len());

        for (_tier_id, tier_ssts) in &task.tiers {
            let tables = tier_ssts
                .iter()
                .map(|id| snapshot.sstables.get(id).unwrap().clone())
                .collect();
            let iter = SstConcatIterator::create_and_seek_to_first(tables)?;
            iters.push(Box::new(iter));
        }

        let merge_iter = MergeIterator::create(iters);

        self.compact_inner(merge_iter)
    }

    fn compact_simple(&self, task: &SimpleLeveledCompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        match task.upper_level {
            Some(_level) => {
                let iter = Self::build_levels_iter(
                    &snapshot,
                    &task.upper_level_sst_ids,
                    &task.lower_level_sst_ids,
                )?;
                self.compact_inner(iter)
            }
            None => {
                let iter = Self::build_l0_and_l1_iter(
                    &snapshot,
                    &task.upper_level_sst_ids,
                    &task.lower_level_sst_ids,
                )?;
                self.compact_inner(iter)
            }
        }
    }

    fn compact_full(
        &self,
        l0_sstables: &[usize],
        l1_sstables: &[usize],
    ) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        let iter = Self::build_l0_and_l1_iter(&snapshot, l0_sstables, l1_sstables)?;

        self.compact_inner(iter)
    }

    fn build_l0_and_l1_iter(
        snapshot: &LsmStorageState,
        l0_sstables: &[usize],
        l1_sstables: &[usize],
    ) -> Result<TwoMergeIterator<MergeIterator<SsTableIterator>, SstConcatIterator>> {
        let mut l0s = Vec::with_capacity(l0_sstables.len());
        for table_id in l0_sstables {
            let table = snapshot.sstables.get(table_id).unwrap().clone();
            let iterator = SsTableIterator::create_and_seek_to_first(table)?;
            l0s.push(Box::new(iterator));
        }
        let l0_iter = MergeIterator::create(l0s);

        let mut l1s = Vec::with_capacity(l1_sstables.len());
        for table_id in l1_sstables {
            let table = snapshot.sstables.get(table_id).unwrap().clone();
            l1s.push(table);
        }
        let l1_iter = SstConcatIterator::create_and_seek_to_first(l1s)?;

        TwoMergeIterator::create(l0_iter, l1_iter)
    }

    fn build_levels_iter(
        snapshot: &LsmStorageState,
        upper_level_sstables: &[usize],
        lower_level_sstables: &[usize],
    ) -> Result<TwoMergeIterator<SstConcatIterator, SstConcatIterator>> {
        let mut uppers = Vec::with_capacity(upper_level_sstables.len());
        for table_id in upper_level_sstables {
            let table = snapshot.sstables.get(table_id).unwrap().clone();
            uppers.push(table);
        }
        let upper_iter = SstConcatIterator::create_and_seek_to_first(uppers)?;

        let mut lowers = Vec::with_capacity(lower_level_sstables.len());
        for table_id in lower_level_sstables {
            let table = snapshot.sstables.get(table_id).unwrap().clone();
            lowers.push(table);
        }
        let lower_iter = SstConcatIterator::create_and_seek_to_first(lowers)?;

        TwoMergeIterator::create(upper_iter, lower_iter)
    }

    /// Core logic of compaction. The function takes an iterator and produces
    /// a sequence of SsTables by draining entries from the iterator.
    fn compact_inner<I: for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>>(
        &self,
        mut iterator: I,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut res = Vec::new();

        let sst_size = self.options.target_sst_size;
        let block_size = self.options.block_size;

        let mut current_builder = SsTableBuilder::new(block_size);

        let mut prev_key = Vec::new();
        while iterator.is_valid() {
            if current_builder.estimated_size() >= sst_size && iterator.key().key_ref() != prev_key
            {
                self.finish_current_table(&mut current_builder, &mut res)?;
            }
            prev_key = iterator.key().key_ref().to_vec();
            current_builder.add(iterator.key(), iterator.value());
            iterator.next()?;
        }

        if current_builder.estimated_size() > 0 {
            self.finish_current_table(&mut current_builder, &mut res)?;
        }

        Ok(res)
    }

    fn finish_current_table(
        &self,
        current_builder: &mut SsTableBuilder,
        res: &mut Vec<Arc<SsTable>>,
    ) -> Result<()> {
        let block_size = self.options.block_size;
        let old_builder = std::mem::replace(current_builder, SsTableBuilder::new(block_size));
        let id = self.next_sst_id();
        let path = self.sst_path(id)?;
        let table = old_builder.build(id, Some(Arc::clone(&self.block_cache)), path)?;
        res.push(Arc::new(table));
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        // Step 1: Grab read lock and get tables for merge
        let (l0_sstables, l1_sstables) = {
            let guard = self.state.read();
            (guard.l0_sstables.clone(), guard.levels[0].1.clone())
        };

        // Step 2: Merge and create new SSTs outside of critical section
        let new_sstables = self.compact(&CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        })?;
        let new_sstable_ids: Vec<usize> = new_sstables.iter().map(|table| table.sst_id()).collect();

        // Step 3: Produce a new LSM state by removing merged sstables and add new sstables
        {
            let _state_lock = self.state_lock.lock();
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();

            // When we are merging the last snapshot of l0 ssts, other thread could flush new memtables to be
            // new l0 ssts. Be sure not to remove those new l0 tables.
            snapshot.l0_sstables.retain(|id| !l0_sstables.contains(id));
            snapshot.levels[0].1 = new_sstable_ids;
            for table in new_sstables {
                snapshot.sstables.insert(table.sst_id(), table);
            }
            for l0 in &l0_sstables {
                snapshot.sstables.remove(l0);
            }
            for l1 in &l1_sstables {
                snapshot.sstables.remove(l1);
            }

            *guard = Arc::new(snapshot);
        }

        // Step 4: Remove deleted SST files on disk
        for deleted_l0 in &l0_sstables {
            let path = self.path_of_sst(*deleted_l0);
            std::fs::remove_file(path)?;
        }

        for deleted_l1 in &l1_sstables {
            let path = self.path_of_sst(*deleted_l1);
            std::fs::remove_file(path)?;
        }

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let task = {
            let guard = self.state.read();
            self.compaction_controller.generate_compaction_task(&guard)
        };

        if let Some(task) = task {
            // Perform compaction outside of critical section
            let compaction_result = self.compact(&task)?;

            // Take the state lock here to synchronize with l0 flushing operations.
            // We don't want the snapshot to change between this point in time and when we replace the whole snapshot
            // after applying compaction result.
            let guard = self.state_lock.lock();
            let deleted_ssts = self.finish_compaction(guard, task, compaction_result)?;

            // Delete SST files
            for sst_id in deleted_ssts {
                let path = self.path_of_sst(sst_id);
                std::fs::remove_file(path)?;
            }
        }

        Ok(())
    }

    fn finish_compaction(
        &self,
        _state_lock_observer: MutexGuard<'_, ()>,
        task: CompactionTask,
        compaction_result: Vec<Arc<SsTable>>,
    ) -> Result<Vec<usize>> {
        let new_sst_ids: Vec<usize> = compaction_result
            .iter()
            .map(|table| table.sst_id())
            .collect();

        let mut old_snapshot = self.state.read().as_ref().clone();
        for table in compaction_result {
            old_snapshot.sstables.insert(table.sst_id(), table);
        }

        let (mut new_snapshot, deleted_ssts) =
            self.compaction_controller
                .apply_compaction_result(&old_snapshot, &task, &new_sst_ids);

        if let CompactionTask::Leveled(leveled_task) = &task {
            let new_lower_level_ssts = &mut new_snapshot.levels[leveled_task.lower_level - 1].1;
            new_lower_level_ssts.sort_by(|x, y| {
                new_snapshot
                    .sstables
                    .get(x)
                    .unwrap()
                    .first_key()
                    .cmp(new_snapshot.sstables.get(y).unwrap().first_key())
            });
        }

        // for id in &deleted_ssts {
        //     assert!(new_snapshot.sstables.remove(id).is_some());
        // }
        self.sync_dir()?;

        if let Some(manifest) = &self.manifest {
            manifest.add_record(
                &_state_lock_observer,
                ManifestRecord::Compaction(task, new_sst_ids),
            )?;
        }

        {
            let mut guard = self.state.write();
            *guard = Arc::new(new_snapshot);
        }

        Ok(deleted_ssts)
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let should_flush = {
            let snapshot = self.state.read();
            snapshot.imm_memtables.len() + 1 > self.options.num_memtable_limit
        };

        if should_flush {
            self.force_flush_next_imm_memtable()?;
        }

        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
