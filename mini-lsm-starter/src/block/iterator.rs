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
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
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
        self.fetch_key();
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;
        self.fetch_key();
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
                match cur_key.cmp(&key) {
                    std::cmp::Ordering::Equal => std::cmp::Ordering::Greater,
                    ord => ord,
                }
            })
            .unwrap_err();
        self.idx = lower_bound;
        self.fetch_key();
    }

    fn fetch_key(&mut self) {
        match self.block.offsets.get(self.idx) {
            Some(&offset) => self.key = self.key_at_offset(offset as usize).to_key_vec(),
            None => self.key = KeyVec::new(),
        }
    }

    fn key_at_offset(&self, offset: usize) -> KeySlice {
        let mut buffer = &self.block.data[offset..];
        let key_len = buffer.get_u16() as usize;
        let key = KeySlice::from_slice(&buffer[..key_len]);
        key
    }

    fn value_at_offset(&self, offset: usize) -> &[u8] {
        let mut buffer = &self.block.data[offset..];
        let key_len = buffer.get_u16() as usize;
        let mut value_buffer = &buffer[key_len..];
        let val_len = value_buffer.get_u16() as usize;
        &value_buffer[..val_len]
    }
}
