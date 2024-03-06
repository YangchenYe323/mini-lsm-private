#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::StorageIterator;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
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
            CompactionTask::Leveled(_) => unimplemented!(),
            CompactionTask::Tiered(_) => unimplemented!(),
            CompactionTask::Simple(_) => unimplemented!(),
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => self.compact_full(l0_sstables, l1_sstables),
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

        let mut res = Vec::new();

        let sst_size = self.options.target_sst_size;
        let block_size = self.options.block_size;

        let mut current_builder = SsTableBuilder::new(block_size);

        let mut finish_current_table = |builder: &mut SsTableBuilder| -> Result<()> {
            let old_builder = std::mem::replace(builder, SsTableBuilder::new(block_size));
            let id = self.next_sst_id();
            let path = self.sst_path(id)?;
            let table = old_builder.build(id, Some(Arc::clone(&self.block_cache)), path)?;
            res.push(Arc::new(table));
            Ok(())
        };

        // Construct merge iterators over the tables to be merged
        let mut iters = Vec::with_capacity(l0_sstables.len() + l1_sstables.len());
        for l0 in l0_sstables {
            let table = Arc::clone(snapshot.sstables.get(l0).unwrap());
            let iter = SsTableIterator::create_and_seek_to_first(table)?;
            iters.push(Box::new(iter));
        }

        for l1 in l1_sstables {
            let table = Arc::clone(snapshot.sstables.get(l1).unwrap());
            let iter = SsTableIterator::create_and_seek_to_first(table)?;
            iters.push(Box::new(iter));
        }
        let mut merge_iter = MergeIterator::create(iters);

        while merge_iter.is_valid() {
            if !merge_iter.value().is_empty() {
                if current_builder.estimated_size() >= sst_size {
                    finish_current_table(&mut current_builder)?;
                }
                current_builder.add(merge_iter.key(), merge_iter.value());
            }
            merge_iter.next()?;
        }

        if current_builder.estimated_size() > 0 {
            finish_current_table(&mut current_builder)?;
        }

        Ok(res)
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
        unimplemented!()
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
