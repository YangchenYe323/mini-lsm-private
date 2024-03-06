#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use anyhow::{Ok, Result};

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        let current = sstables
            .first()
            .map(|table| SsTableIterator::create_and_seek_to_first(Arc::clone(table)))
            .transpose()?;
        Ok(Self {
            current,
            next_sst_idx: 1,
            sstables,
        })
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        let lower_bound = sstables
            .binary_search_by(|table| match table.last_key().as_key_slice().cmp(&key) {
                std::cmp::Ordering::Equal => std::cmp::Ordering::Greater,
                ord => ord,
            })
            .unwrap_err();

        let current = sstables
            .get(lower_bound)
            .map(|table| SsTableIterator::create_and_seek_to_key(Arc::clone(table), key))
            .transpose()?;

        Ok(Self {
            current,
            next_sst_idx: lower_bound + 1,
            sstables,
        })
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {
        if let Some(current) = self.current.as_mut() {
            current.next()?;
            if !current.is_valid() {
                self.current = self
                    .sstables
                    .get(self.next_sst_idx)
                    .map(|table| SsTableIterator::create_and_seek_to_first(Arc::clone(table)))
                    .transpose()?;
                self.next_sst_idx += 1;
            }
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
