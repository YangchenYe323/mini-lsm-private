#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    use_a: bool,
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        let mut iter = Self { a, b, use_a: false };

        iter.determine_next_iter();
        Ok(iter)
    }

    fn determine_next_iter(&mut self) {
        self.use_a = match (self.a.is_valid(), self.b.is_valid()) {
            (false, true) => false,
            (true, false) => true,
            (false, false) => false,
            (true, true) => match self.a.key().cmp(&self.b.key()) {
                std::cmp::Ordering::Equal | std::cmp::Ordering::Less => true,
                _ => false,
            },
        };
    }

    fn forward<I, J>(current_iter: &mut I, other_iter: &mut J) -> Result<()>
    where
        I: 'static + StorageIterator,
        J: 'static + for<'a> StorageIterator<KeyType<'a> = I::KeyType<'a>>,
    {
        while other_iter.is_valid() && other_iter.key() <= current_iter.key() {
            other_iter.next()?;
        }
        current_iter.next()?;
        Ok(())
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if self.use_a {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.use_a {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        self.a.is_valid() || self.b.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        if self.use_a {
            Self::forward(&mut self.a, &mut self.b)?;
        } else {
            Self::forward(&mut self.b, &mut self.a)?;
        }
        self.determine_next_iter();
        Ok(())
    }
}
