use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        snapshot: &LsmStorageState,
        sst_ids: &[usize],
        in_level: usize,
    ) -> Vec<usize> {
        snapshot.levels[in_level - 1]
            .1
            .iter()
            .copied()
            .filter(|cur_id| {
                let table = snapshot.sstables.get(cur_id).unwrap();
                for id in sst_ids {
                    let other = snapshot.sstables.get(id).unwrap();
                    let first_key = other.first_key();
                    let last_key = other.last_key();

                    let lower = std::ops::Bound::Included(first_key.as_key_slice());
                    let upper = std::ops::Bound::Included(last_key.as_key_slice());

                    if table.range_overlap(lower, upper) {
                        return true;
                    }
                }
                false
            })
            .collect()
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        // Step 1: Compute size in bytes of each level
        let level_sizes_bytes: Vec<usize> = snapshot
            .levels
            .iter()
            .map(|(_id, tables)| {
                tables
                    .iter()
                    .map(|id| snapshot.sstables.get(id).unwrap().file.size())
                    .sum::<u64>() as usize
            })
            .collect();
        let target_sizes_bytes = self.compute_target_size_bytes(&level_sizes_bytes);

        if let Some(task) = self.generate_l0_compaction_task(snapshot, &target_sizes_bytes) {
            return Some(task);
        }

        self.generate_level_compaction_task(snapshot, &level_sizes_bytes, &target_sizes_bytes)
    }

    fn generate_l0_compaction_task(
        &self,
        snapshot: &LsmStorageState,
        target_sizes_bytes: &[usize],
    ) -> Option<LeveledCompactionTask> {
        let l0_file_num = snapshot.l0_sstables.len();

        if l0_file_num >= self.options.level0_file_num_compaction_trigger {
            // Compact l0 into the first level where the target size is > 0
            let level_to_compact = target_sizes_bytes
                .iter()
                .position(|size| *size > 0)
                .unwrap()
                + 1;

            println!("flush L0 SST to base level {}", level_to_compact);

            let l0_ssts = snapshot.l0_sstables.clone();
            let level_ssts = self.find_overlapping_ssts(snapshot, &l0_ssts, level_to_compact);

            let task = LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: l0_ssts,
                lower_level: level_to_compact,
                lower_level_sst_ids: level_ssts,
                is_lower_level_bottom_level: level_to_compact == self.options.max_levels,
            };

            return Some(task);
        }

        None
    }

    fn generate_level_compaction_task(
        &self,
        snapshot: &LsmStorageState,
        level_sizes_bytes: &[usize],
        target_sizes_bytes: &[usize],
    ) -> Option<LeveledCompactionTask> {
        let priorities = level_sizes_bytes
            .iter()
            .enumerate()
            .take(target_sizes_bytes.len() - 1)
            .map(|(idx, actual_size)| {
                let actual_size = *actual_size;
                let target_size = target_sizes_bytes[idx];
                if target_size == 0 {
                    assert_eq!(0, actual_size);
                    return 0.0;
                }
                actual_size as f64 / target_size as f64
            });

        let (max_idx, max_priority) = priorities
            .enumerate()
            .max_by(|(_, p1), (_, p2)| p1.partial_cmp(p2).unwrap())
            .unwrap();

        if max_priority <= 1.0 {
            return None;
        }

        // Compaction is triggered
        println!(
            "target level sizes: {:?}, real level sizes: {:?}, base_level: {}",
            target_sizes_bytes
                .iter()
                .map(|x| format!("{:.3}MB", *x as f64 / 1024.0 / 1024.0))
                .collect::<Vec<_>>(),
            level_sizes_bytes
                .iter()
                .map(|x| format!("{:.3}MB", *x as f64 / 1024.0 / 1024.0))
                .collect::<Vec<_>>(),
            target_sizes_bytes.iter().position(|x| *x > 0).unwrap() + 1
        );

        let upper_level = max_idx + 1;
        let lower_level = upper_level + 1;

        println!("{:?}", snapshot.levels);

        // Find the oldest SST from the upper level and overlaping SSTs from the lower level
        let selected_sst = snapshot.levels[upper_level - 1]
            .1
            .iter()
            .min()
            .copied()
            .unwrap();
        println!(
            "compaction triggered by priority: level {} with priority {}, select {} out of {:?} for compaction",
            upper_level, max_priority, selected_sst, snapshot.levels[upper_level - 1]
        );
        let overlapping_ssts = self.find_overlapping_ssts(snapshot, &[selected_sst], lower_level);

        Some(LeveledCompactionTask {
            upper_level: Some(upper_level),
            upper_level_sst_ids: vec![selected_sst],
            lower_level,
            lower_level_sst_ids: overlapping_ssts,
            is_lower_level_bottom_level: lower_level == self.options.max_levels,
        })
    }

    fn compute_target_size_bytes(&self, level_sizes_bytes: &[usize]) -> Vec<usize> {
        let last_level_size = *level_sizes_bytes.last().unwrap();
        let mut target_size = vec![0; level_sizes_bytes.len()];
        let base_level_size_bytes = self.options.base_level_size_mb * 1024 * 1024;

        if last_level_size <= base_level_size_bytes {
            *target_size.last_mut().unwrap() = base_level_size_bytes;
            return target_size;
        }

        *target_size.last_mut().unwrap() = last_level_size;
        for i in (0..level_sizes_bytes.len() - 1).rev() {
            if target_size[i + 1] >= base_level_size_bytes {
                target_size[i] = target_size[i + 1] / self.options.level_size_multiplier;
            }
        }

        target_size
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut removed_ssts =
            Vec::with_capacity(task.upper_level_sst_ids.len() + task.lower_level_sst_ids.len());
        removed_ssts.extend(task.upper_level_sst_ids.iter().copied());
        removed_ssts.extend(task.lower_level_sst_ids.iter().copied());

        let mut new_snapshot = snapshot.clone();

        match task.upper_level {
            Some(level) => {
                // Remove deleted SSTs
                new_snapshot.levels[level - 1]
                    .1
                    .retain(|id| !task.upper_level_sst_ids.contains(id));
                new_snapshot.levels[task.lower_level - 1]
                    .1
                    .retain(|id| !task.lower_level_sst_ids.contains(id));
            }
            None => {
                // Remove deleted SSTs
                new_snapshot
                    .l0_sstables
                    .retain(|id| !task.upper_level_sst_ids.contains(id));
                new_snapshot.levels[task.lower_level - 1]
                    .1
                    .retain(|id| !task.lower_level_sst_ids.contains(id));
            }
        }

        new_snapshot.levels[task.lower_level - 1]
            .1
            .extend(output.iter().copied());

        (new_snapshot, removed_ssts)
    }
}
