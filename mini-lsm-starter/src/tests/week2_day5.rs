use std::time::Duration;

use bytes::{Buf, BufMut};
use tempfile::tempdir;

use crate::{
    compact::{
        CompactionOptions, LeveledCompactionOptions, SimpleLeveledCompactionOptions,
        TieredCompactionOptions,
    },
    iterators::StorageIterator,
    lsm_storage::{LsmStorageOptions, MiniLsm},
    table::SsTableIterator,
    tests::harness::dump_files_in_dir,
};

#[test]
fn test_integration_leveled() {
    test_integration(CompactionOptions::Leveled(LeveledCompactionOptions {
        level_size_multiplier: 2,
        level0_file_num_compaction_trigger: 2,
        max_levels: 3,
        base_level_size_mb: 1,
    }))
}

#[test]
fn test_integration_tiered() {
    test_integration(CompactionOptions::Tiered(TieredCompactionOptions {
        num_tiers: 3,
        max_size_amplification_percent: 200,
        size_ratio: 1,
        min_merge_width: 3,
    }))
}

#[test]
fn test_integration_simple() {
    test_integration(CompactionOptions::Simple(SimpleLeveledCompactionOptions {
        size_ratio_percent: 200,
        level0_file_num_compaction_trigger: 2,
        max_levels: 3,
    }));
}

/// Provision the storage such that base_level contains 2 SST files (target size is 2MB and each SST is 1MB)
/// so that compaction would generate multiple SST files.
#[test]
fn test_multiple_compacted_ssts_leveled() {
    let compaction_options = CompactionOptions::Leveled(LeveledCompactionOptions {
        level_size_multiplier: 4,
        level0_file_num_compaction_trigger: 2,
        max_levels: 2,
        base_level_size_mb: 2,
    });

    let lsm_storage_options = LsmStorageOptions::default_for_week2_test(compaction_options.clone());

    let dir = tempdir().unwrap();
    let storage = MiniLsm::open(&dir, lsm_storage_options).unwrap();

    // Insert approximately 10MB of data to ensure that at least on compaction
    // is triggered by priority.
    // Each k-v pair is 20KB, and Insert a total of 500 pairs
    for i in 0..500 {
        let (key, val) = key_value_pair_with_target_size(i, 20 * 1024);
        storage.put(&key, &val).unwrap();
    }

    let mut prev_snapshot = storage.inner.state.read().clone();
    while {
        std::thread::sleep(Duration::from_secs(1));
        let snapshot = storage.inner.state.read().clone();
        let to_cont = prev_snapshot.levels != snapshot.levels
            || prev_snapshot.l0_sstables != snapshot.l0_sstables;
        prev_snapshot = snapshot;
        to_cont
    } {
        println!("waiting for compaction to converge");
    }

    storage.close().unwrap();
    assert!(storage.inner.state.read().memtable.is_empty());
    assert!(storage.inner.state.read().imm_memtables.is_empty());

    storage.dump_structure();
    let l0_ssts = storage.inner.state.read().l0_sstables.clone();
    let l0_ssts: Vec<_> = l0_ssts
        .into_iter()
        .map(|table_id| {
            storage
                .inner
                .state
                .read()
                .sstables
                .get(&table_id)
                .unwrap()
                .clone()
        })
        .collect();
    for table in l0_ssts {
        let id = table.sst_id();
        let mut iterator = SsTableIterator::create_and_seek_to_first(table).unwrap();
        let mut keys = vec![];
        while iterator.is_valid() {
            let key = iterator.key().for_testing_key_ref();
            let mut buf = &key[key.len() - 4..];
            let key = buf.get_i32();
            keys.push(key);
            iterator.next().unwrap();
        }
        println!("{}: {:?}", id, keys);
    }

    drop(storage);
    dump_files_in_dir(&dir);

    let storage = MiniLsm::open(
        &dir,
        LsmStorageOptions::default_for_week2_test(compaction_options.clone()),
    )
    .unwrap();

    for i in 0..500 {
        let (key, val) = key_value_pair_with_target_size(i, 20 * 1024);
        assert_eq!(&storage.get(&key).unwrap().unwrap()[..], &val);
    }
}

fn test_integration(compaction_options: CompactionOptions) {
    let dir = tempdir().unwrap();
    let storage = MiniLsm::open(
        &dir,
        LsmStorageOptions::default_for_week2_test(compaction_options.clone()),
    )
    .unwrap();
    for i in 0..=20 {
        storage.put(b"0", format!("v{}", i).as_bytes()).unwrap();
        if i % 2 == 0 {
            storage.put(b"1", format!("v{}", i).as_bytes()).unwrap();
        } else {
            storage.delete(b"1").unwrap();
        }
        if i % 2 == 1 {
            storage.put(b"2", format!("v{}", i).as_bytes()).unwrap();
        } else {
            storage.delete(b"2").unwrap();
        }
        storage
            .inner
            .force_freeze_memtable(&storage.inner.state_lock.lock())
            .unwrap();
    }
    storage.close().unwrap();
    // ensure all SSTs are flushed
    assert!(storage.inner.state.read().memtable.is_empty());
    assert!(storage.inner.state.read().imm_memtables.is_empty());
    storage.dump_structure();
    drop(storage);
    dump_files_in_dir(&dir);

    let storage = MiniLsm::open(
        &dir,
        LsmStorageOptions::default_for_week2_test(compaction_options.clone()),
    )
    .unwrap();
    assert_eq!(&storage.get(b"0").unwrap().unwrap()[..], b"v20".as_slice());
    assert_eq!(&storage.get(b"1").unwrap().unwrap()[..], b"v20".as_slice());
    assert_eq!(storage.get(b"2").unwrap(), None);
}

/// Create a key value pair where key and value are of target size in bytes
fn key_value_pair_with_target_size(seed: i32, target_size_byte: usize) -> (Vec<u8>, Vec<u8>) {
    let mut key = vec![0; target_size_byte - 4];
    key.put_i32(seed);

    let mut val = vec![0; target_size_byte - 4];
    val.put_i32(seed);

    (key, val)
}
