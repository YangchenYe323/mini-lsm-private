use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        if snapshot.levels.is_empty() {
            return None;
        }

        if let Some(task) = self.check_space_amplification(snapshot) {
            return Some(task);
        }

        if let Some(task) = self.check_size_ratio(snapshot) {
            return Some(task);
        }

        self.check_num_sorted_runs(snapshot)
    }

    fn check_space_amplification(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        let num_files_total: usize = snapshot.levels.iter().map(|(_, ssts)| ssts.len()).sum();
        let num_last_tier_files = snapshot.levels.last().unwrap().1.len();
        if num_last_tier_files == 0 {
            return None;
        }
        let actual_size_ratio =
            (num_files_total as f64 - num_last_tier_files as f64) / num_last_tier_files as f64;

        if actual_size_ratio * 100.0 >= self.options.max_size_amplification_percent as f64 {
            println!(
                "compaction triggered by space amplification ratio: {}",
                actual_size_ratio * 100.0
            );
            let task = TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            };
            return Some(task);
        }
        None
    }

    fn check_size_ratio(&self, snapshot: &LsmStorageState) -> Option<TieredCompactionTask> {
        let mut accum_prev_size = 0;
        let n = snapshot.levels.len();
        let threshold = 100.0 + self.options.size_ratio as f64;
        for i in 0..n {
            let current_size = snapshot.levels[i].1.len();
            let actual_size_ratio = (accum_prev_size as f64 / current_size as f64) * 100.0;
            if actual_size_ratio >= threshold && i + 1 >= self.options.min_merge_width {
                println!("compaction triggered by size ratio: {}", actual_size_ratio);
                let task = TieredCompactionTask {
                    tiers: snapshot.levels[0..i + 1].to_vec(),
                    bottom_tier_included: i == n - 1,
                };
                return Some(task);
            }
            accum_prev_size += current_size;
        }

        None
    }

    fn check_num_sorted_runs(&self, snapshot: &LsmStorageState) -> Option<TieredCompactionTask> {
        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        println!("compaction triggered by reducing sorted runs");

        if self.options.num_tiers == 2 {
            return Some(TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }

        let task = TieredCompactionTask {
            tiers: snapshot.levels[0..self.options.num_tiers - 1].to_vec(),
            bottom_tier_included: false,
        };

        Some(task)
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        // Since tiered compaction performs a full compaction, every SST in the old tiers are removed
        let mut removed_ssts = Vec::new();
        for (_tier_id, tier_ssts) in &task.tiers {
            removed_ssts.extend(tier_ssts.iter().copied());
        }

        // We will search inside the old levels, and find the contiguous tiers that we used for the current
        // task, and replace that contiguous sequence of tiers with the new tier we created.
        let first_tier_id = task.tiers.first().unwrap().0;
        let last_tier_id = task.tiers.last().unwrap().0;
        let first_position = snapshot
            .levels
            .iter()
            .position(|(id, _)| *id == first_tier_id)
            .unwrap();
        let last_position = snapshot
            .levels
            .iter()
            .position(|(id, _)| *id == last_tier_id)
            .unwrap();

        let new_tier_id = output[0];
        let mut new_snapshot = snapshot.clone();
        let mut new_levels = Vec::new();
        new_levels.extend(snapshot.levels[..first_position].iter().cloned());
        new_levels.push((new_tier_id, output.to_vec()));
        new_levels.extend(snapshot.levels[last_position + 1..].iter().cloned());

        new_snapshot.levels = new_levels;
        (new_snapshot, removed_ssts)
    }
}
