use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;

use crate::iterators::StorageIterator;
use crate::key::KeyBytes;
use crate::key::KeySlice;
use crate::key::TS_DEFAULT;
use crate::key::TS_RANGE_BEGIN;
use crate::key::TS_RANGE_END;
use crate::table::SsTableBuilder;
use crate::wal::Wal;

/// A basic mem-table based on crossbeam-skiplist.
///
/// An initial implementation of memtable is part of week 1, day 1. It will be incrementally implemented in other
/// chapters of week 1 and week 2.
pub struct MemTable {
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}

/// Create a bound of `Bytes` from a bound of `KeySlice`.
pub(crate) fn map_bound(bound: Bound<KeySlice>) -> Bound<KeyBytes> {
    match bound {
        Bound::Included(x) => Bound::Included(x.to_key_vec().into_key_bytes()),
        Bound::Excluded(x) => Bound::Excluded(x.to_key_vec().into_key_bytes()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

/// Create a lower bound of `KeySlice` from a bound of `&[u8]` by applying the default timestamp
pub(crate) fn default_lower_bound(bound: Bound<&[u8]>) -> Bound<KeySlice> {
    match bound {
        Bound::Included(x) => Bound::Included(KeySlice::from_slice(x, TS_RANGE_BEGIN)),
        Bound::Excluded(x) => Bound::Excluded(KeySlice::from_slice(x, TS_RANGE_END)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

/// Create an upper bound of `KeySlice` from a bound of `&[u8]` by applying the default timestamp
pub(crate) fn default_upper_bound(bound: Bound<&[u8]>) -> Bound<KeySlice> {
    match bound {
        Bound::Included(x) => Bound::Included(KeySlice::from_slice(x, TS_RANGE_END)),
        Bound::Excluded(x) => Bound::Excluded(KeySlice::from_slice(x, TS_RANGE_BEGIN)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create(id: usize) -> Self {
        Self {
            map: Arc::new(SkipMap::new()),
            wal: None,
            id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Create a new mem-table with WAL
    pub fn create_with_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            map: Arc::new(SkipMap::new()),
            wal: Some(Wal::create(path)?),
            id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Create a memtable from WAL
    pub fn recover_from_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let map = SkipMap::new();
        let wal = Wal::recover(path, &map)?;
        let size = AtomicUsize::new(map.len());
        Ok(Self {
            id,
            map: Arc::new(map),
            wal: Some(wal),
            approximate_size: Arc::new(size),
        })
    }

    pub fn for_testing_put_slice(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put(KeySlice::from_slice(key, TS_DEFAULT), value)
    }

    pub fn for_testing_get_slice(&self, key: &[u8]) -> Option<Bytes> {
        self.get(KeySlice::from_slice(key, TS_DEFAULT))
    }

    pub fn for_testing_scan_slice(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> MemTableIterator {
        let lower_bound = default_lower_bound(lower);
        let upper_bound = default_upper_bound(upper);

        self.scan(lower_bound, upper_bound)
    }

    /// Get a value by key.
    pub fn get(&self, key: KeySlice) -> Option<Bytes> {
        let key = unsafe {
            KeyBytes::from_bytes_with_ts(
                Bytes::from_static(std::mem::transmute(key.key_ref())),
                key.ts(),
            )
        };
        self.map.get(&key).map(|entry| entry.value().clone())
    }

    /// Put a key-value pair into the mem-table.
    ///
    /// In week 1, day 1, simply put the key-value pair into the skipmap.
    /// In week 2, day 6, also flush the data to WAL.
    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        if let Some(wal) = self.wal.as_ref() {
            wal.put(key, value)?;
        }

        self.map.insert(
            key.to_key_vec().into_key_bytes(),
            Bytes::copy_from_slice(value),
        );
        self.approximate_size
            .fetch_add(key.raw_len() + value.len(), Ordering::Relaxed);
        Ok(())
    }

    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, lower: Bound<KeySlice>, upper: Bound<KeySlice>) -> MemTableIterator {
        let item = (KeyBytes::new(), Bytes::from_static(&[]));

        let mut iter = MemTableIterator::new(
            Arc::clone(&self.map),
            move |map| {
                let lower_bound = map_bound(lower);
                let upper_bound = map_bound(upper);

                map.range((lower_bound, upper_bound))
            },
            item,
        );
        iter.next().unwrap();

        iter
    }

    /// Flush the mem-table to SSTable. Implement in week 1 day 6.
    pub fn flush(&self, builder: &mut SsTableBuilder) -> Result<()> {
        for entry in self.map.iter() {
            let key = entry.key();
            let value = entry.value();
            builder.add(key.as_key_slice(), value);
        }

        Ok(())
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size.load(Ordering::Relaxed)
    }

    /// Only use this function when closing the database
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

type SkipMapRangeIter<'a> = crossbeam_skiplist::map::Range<
    'a,
    KeyBytes,
    (Bound<KeyBytes>, Bound<KeyBytes>),
    KeyBytes,
    Bytes,
>;

/// An iterator over a range of `SkipMap`. This is a self-referential structure and please refer to week 1, day 2
/// chapter for more information.
///
/// This is part of week 1, day 2.
#[self_referencing]
pub struct MemTableIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (KeyBytes, Bytes),
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        self.borrow_item().1.as_ref()
    }

    fn key(&self) -> KeySlice {
        self.borrow_item().0.as_key_slice()
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let next_entry = self.with_iter_mut(|iter| {
            // We use empty key to signal that the iterator has reached the end.
            // ! Note that empty values, which means the key has been deleted, will be returned by [MemTableIterator]
            iter.next()
                .map(|entry| (entry.key().clone(), entry.value().clone()))
                .unwrap_or_else(|| (KeyBytes::new(), Bytes::from_static(&[])))
        });

        self.with_item_mut(|item| {
            *item = next_entry;
        });
        Ok(())
    }
}
