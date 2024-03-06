use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        for level in 0..self.options.max_levels {
            let next_level = level + 1;
            let ssts_level = if level == 0 {
                &snapshot.l0_sstables
            } else {
                &snapshot.levels[level - 1].1
            };
            let ssts_next_level = &snapshot.levels[next_level - 1].1;
            let should_compact = if level == 0 {
                should_compact(
                    ssts_next_level.len(),
                    ssts_level.len(),
                    self.options.size_ratio_percent,
                ) && ssts_level.len() >= self.options.level0_file_num_compaction_trigger
            } else {
                should_compact(
                    ssts_next_level.len(),
                    ssts_level.len(),
                    self.options.size_ratio_percent,
                )
            };
            if should_compact {
                let task = SimpleLeveledCompactionTask {
                    upper_level: if level == 0 { None } else { Some(level) },
                    upper_level_sst_ids: ssts_level.clone(),
                    lower_level: next_level,
                    lower_level_sst_ids: ssts_next_level.clone(),
                    is_lower_level_bottom_level: next_level == self.options.max_levels,
                };
                return Some(task);
            }
        }

        None
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &SimpleLeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let removed_ssts = task
            .upper_level_sst_ids
            .iter()
            .chain(task.lower_level_sst_ids.iter())
            .copied()
            .collect();

        let mut snapshot = snapshot.clone();
        match task.upper_level {
            Some(level) => {
                snapshot.levels[level - 1].1.clear();
            }
            None => {
                snapshot
                    .l0_sstables
                    .retain(|id| !task.upper_level_sst_ids.contains(id));
            }
        }
        snapshot.levels[task.lower_level - 1].1 = output.to_vec();

        (snapshot, removed_ssts)
    }
}

fn should_compact(
    lower_level_file_number: usize,
    upper_level_file_number: usize,
    ratio_pecent: usize,
) -> bool {
    if upper_level_file_number == 0 {
        return false;
    }
    let actual_ratio_percent =
        ((lower_level_file_number as f64 / upper_level_file_number as f64) * 100.0) as usize;
    actual_ratio_percent < ratio_pecent
}
