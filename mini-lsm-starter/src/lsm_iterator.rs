use std::ops::Bound;

use anyhow::{anyhow, Result};
use bytes::Bytes;

use crate::{
    iterators::{
        concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    key::KeyVec,
    mem_table::MemTableIterator,
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
    // value upper bound
    upper: Bound<Bytes>,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, upper: Bound<&[u8]>) -> Result<Self> {
        let upper = match upper {
            Bound::Excluded(s) => Bound::Excluded(Bytes::copy_from_slice(s)),
            Bound::Included(s) => Bound::Included(Bytes::copy_from_slice(s)),
            Bound::Unbounded => Bound::Unbounded,
        };

        let mut it = Self {
            inner: iter,
            current_key: KeyVec::new(),
            upper,
        };

        it.skip_deleted()?;
        it.fetch_current_key();

        Ok(it)
    }

    fn skip_deleted(&mut self) -> Result<()> {
        while self.inner.is_valid() && self.inner.value().is_empty() {
            self.inner.next()?;
        }
        Ok(())
    }

    fn check_upper(&self) -> bool {
        let key = self.key();
        match &self.upper {
            Bound::Included(s) => key <= s,
            Bound::Excluded(s) => key < s,
            Bound::Unbounded => true,
        }
    }

    fn fetch_current_key(&mut self) {
        if self.inner.is_valid() {
            self.current_key.clear();
            self.current_key = self.inner.key().to_key_vec();
        }
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.inner.is_valid() && self.check_upper()
    }

    fn key(&self) -> &[u8] {
        self.current_key.key_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        self.inner.next()?;
        self.skip_deleted()?;
        self.fetch_current_key();
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
