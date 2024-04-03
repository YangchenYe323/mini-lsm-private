use std::ops::Bound;

use anyhow::{anyhow, Result};

use crate::{
    iterators::{
        concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    key::{KeyBytes, KeySlice, KeyVec},
    mem_table::{map_bound, MemTableIterator},
    table::SsTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    MergeIterator<SstConcatIterator>,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    current_key: KeyVec,
    current_value: Vec<u8>,
    // value upper bound
    upper: Bound<KeyBytes>,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, upper: Bound<KeySlice>) -> Result<Self> {
        let upper = map_bound(upper);

        let mut it = Self {
            inner: iter,
            current_key: KeyVec::new(),
            current_value: Vec::new(),
            upper,
        };

        it.fetch_current();
        it.skip_older_version()?;
        if it.value().is_empty() {
            it.next()?;
        }

        Ok(it)
    }

    fn skip_older_version(&mut self) -> Result<()> {
        while self.inner.is_valid() && self.inner.key().key_ref() == self.current_key.key_ref() {
            self.inner.next()?;
        }
        Ok(())
    }

    fn check_upper(&self) -> bool {
        match &self.upper {
            Bound::Included(s) => self.current_key.as_key_slice() <= s.as_key_slice(),
            Bound::Excluded(s) => self.current_key.as_key_slice() < s.as_key_slice(),
            Bound::Unbounded => true,
        }
    }

    fn fetch_current(&mut self) {
        if self.inner.is_valid() {
            self.current_key = self.inner.key().to_key_vec();
            self.current_value.clear();
            self.current_value.extend(self.inner.value());
        }
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        !self.current_key.is_empty() && self.check_upper()
    }

    fn key(&self) -> &[u8] {
        self.current_key.key_ref()
    }

    fn value(&self) -> &[u8] {
        &self.current_value
    }

    fn next(&mut self) -> Result<()> {
        if !self.inner.is_valid() {
            self.current_key = KeyVec::new();
            return Ok(());
        }

        while self.inner.is_valid() {
            self.fetch_current();
            self.skip_older_version()?;
            if !self.value().is_empty() {
                break;
            }
            if !self.inner.is_valid() {
                self.current_key = KeyVec::new();
                return Ok(());
            }
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a> = I::KeyType<'a> where Self: 'a;

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            return Err(anyhow!("Calling next on an invalid iterator"));
        }

        if self.iter.is_valid() {
            match self.iter.next() {
                Ok(()) => return Ok(()),
                Err(e) => {
                    self.has_errored = true;
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
