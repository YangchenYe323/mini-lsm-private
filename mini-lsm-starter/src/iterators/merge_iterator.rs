use std::cmp::{self};
use std::collections::BinaryHeap;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    #[allow(clippy::non_canonical_partial_ord_impl)]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(&other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let heap: BinaryHeap<HeapWrapper<I>> = iters
            .into_iter()
            .filter(|i| i.is_valid())
            .enumerate()
            .map(|(idx, iter)| HeapWrapper(idx, iter))
            .collect();

        let mut it = Self {
            iters: heap,
            current: None,
        };

        it.set_new_current();

        it
    }
}

impl<I: StorageIterator> MergeIterator<I> {
    // Retrieve the new top iterator from the heap.
    fn set_new_current(&mut self) {
        if let Some(new_current) = self.iters.pop() {
            self.current = Some(new_current);
        };
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn num_active_iterators(&self) -> usize {
        let mut num = 0;
        if let Some(cur) = self.current.as_ref() {
            num += cur.1.num_active_iterators();
        }

        for wrapper in &self.iters {
            num += wrapper.1.num_active_iterators();
        }

        num
    }

    fn next(&mut self) -> Result<()> {
        let mut old_current = self.current.take().unwrap();
        let current_key = old_current.1.key();

        // Step 1: We have gone past the current key, so we should skip all entries with
        // current key in all the iterators.
        while let Some(mut it) = self.iters.pop() {
            if it.1.key() == current_key {
                while it.1.is_valid() && it.1.key() == current_key {
                    match it.1.next() {
                        Ok(()) => (),
                        Err(e) => {
                            // This iterator has encountered an error and is not usable,
                            // Skip to the next one.
                            return Err(e);
                        }
                    }
                }
                if it.1.is_valid() {
                    self.iters.push(it);
                }
            } else {
                self.iters.push(it);
                break;
            }
        }

        // Step 2: Skip current key in the current iterator
        if old_current.1.is_valid() {
            match old_current.1.next() {
                Ok(()) => {
                    if old_current.1.is_valid() {
                        self.iters.push(old_current);
                    }
                }
                Err(e) => return Err(e),
            }
        }

        // Step 3: Find the next current iterator
        self.set_new_current();

        Ok(())
    }

    /// For test purpose only
    fn dump_state(&self) {
        let current = self.current.as_ref().unwrap();
        print!(
            "{:?}-{:?}",
            current.1.key().for_testing_key_ref(),
            current.1.value()
        );
        for it in &self.iters {
            print!(
                ", {:?}-{:?}",
                it.1.key().for_testing_key_ref(),
                it.1.value()
            );
        }
        println!();
    }
}
