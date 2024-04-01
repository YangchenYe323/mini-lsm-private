#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use bytes::Buf;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the value range from the block
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
    /// The offset of the first key in the block
    first_offset: usize,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        let offset = block.offsets[0] as usize;
        let first_key = Self::first_key(&block, offset);
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key,
            first_offset: offset,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        let offset = self.block.offsets[self.idx] as usize;
        self.value_at_offset(offset)
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.idx = 0;
        self.key = self.first_key.clone()
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;
        self.key = match self.block.offsets.get(self.idx) {
            Some(offset) => {
                Self::subsequent_key(&self.block, *offset as usize, self.first_key.as_key_slice())
            }
            None => KeyVec::new(),
        };
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let lower_bound = self
            .block
            .offsets
            .binary_search_by(|offset| {
                let cur_key = self.key_at_offset(*offset as usize);
                match cur_key.as_key_slice().cmp(&key) {
                    std::cmp::Ordering::Equal => std::cmp::Ordering::Greater,
                    ord => ord,
                }
            })
            .unwrap_err();
        self.idx = lower_bound;
        self.key = match self.block.offsets.get(self.idx) {
            Some(offset) => {
                if *offset as usize == self.first_offset {
                    self.first_key.clone()
                } else {
                    Self::subsequent_key(
                        &self.block,
                        *offset as usize,
                        self.first_key.as_key_slice(),
                    )
                }
            }
            None => KeyVec::new(),
        };
    }

    fn key_at_offset(&self, offset: usize) -> KeyVec {
        if offset == self.first_offset {
            return self.first_key.clone();
        }
        Self::subsequent_key(&self.block, offset, self.first_key.as_key_slice())
    }

    fn first_key(block: &Block, offset: usize) -> KeyVec {
        let mut buffer = &block.data[offset..];
        let key_len = buffer.get_u16() as usize;
        let key = &buffer[..key_len];
        buffer.advance(key_len);
        let timestamp = buffer.get_u64();
        KeyVec::from_vec_with_ts(key.to_vec(), timestamp)
    }

    fn subsequent_key(block: &Block, offset: usize, first_key: KeySlice) -> KeyVec {
        let mut key = Vec::new();
        let mut buffer = &block.data[offset..];
        let prefix_len = buffer.get_u16() as usize;
        let rest_len = buffer.get_u16() as usize;
        let rest = &buffer[..rest_len];
        buffer.advance(rest_len);
        let timestamp = buffer.get_u64();
        key.extend(&first_key.key_ref()[..prefix_len]);
        key.extend(rest);
        KeyVec::from_vec_with_ts(key, timestamp)
    }

    fn value_at_offset(&self, offset: usize) -> &[u8] {
        let mut buffer = &self.block.data[offset..];

        let mut value_buffer = if offset == self.first_offset {
            let key_len = buffer.get_u16() as usize;
            &buffer[key_len + 8..]
        } else {
            let _ = buffer.get_u16();
            let rest_len = buffer.get_u16() as usize;
            &buffer[rest_len + 8..]
        };

        let val_len = value_buffer.get_u16() as usize;
        &value_buffer[..val_len]
    }
}
