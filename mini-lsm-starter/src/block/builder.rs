#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::iter;

use bytes::BufMut;

use crate::key::{KeySlice, KeyVec};

use super::Block;

pub(crate) const KEY_LEN_SIZE: usize = 2;
pub(crate) const VALUE_LEN_SIZE: usize = 2;
pub(crate) const OFFSET_SIZE: usize = 2;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::with_capacity(block_size),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let should_add = match self.first_key.is_empty() {
            true => {
                self.first_key = key.to_key_vec();
                true
            }
            false => {
                let this_entry_size = Self::entry_size(key, value);
                !self.would_be_full_after_entry(this_entry_size)
            }
        };

        if should_add {
            self.add_force(key, value);
        }

        should_add
    }

    fn add_force(&mut self, key: KeySlice, value: &[u8]) {
        let this_offset = self.data.len();

        if key == self.first_key.as_key_slice() {
            self.put_first_key(key);
        } else {
            self.put_subsequent_key(key);
        }

        self.data.put_u16(value.len() as u16);
        self.data.put_slice(value);
        self.offsets.push(this_offset as u16);
    }

    fn put_first_key(&mut self, key: KeySlice) {
        self.data.put_u16(key.len() as u16);
        self.data.put_slice(key.raw_ref());
    }

    fn put_subsequent_key(&mut self, key: KeySlice) {
        let matching_prefix_len = mismatch(key.raw_ref(), self.first_key.raw_ref());
        let rest_len = key.raw_ref().len() - matching_prefix_len;
        self.data.put_u16(matching_prefix_len as u16);
        self.data.put_u16(rest_len as u16);
        self.data.put_slice(&key.raw_ref()[matching_prefix_len..]);
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }

    pub fn first_key(&self) -> KeySlice {
        self.first_key.as_key_slice()
    }

    fn entry_size(key: KeySlice, value: &[u8]) -> usize {
        KEY_LEN_SIZE + key.len() + VALUE_LEN_SIZE + value.len()
    }

    fn would_be_full_after_entry(&self, entry_size: usize) -> bool {
        self.data.len() + self.offsets.len() * 2 + entry_size + OFFSET_SIZE > self.block_size
    }
}

pub fn mismatch(xs: &[u8], ys: &[u8]) -> usize {
    mismatch_chunks::<128>(xs, ys)
}

fn mismatch_chunks<const N: usize>(xs: &[u8], ys: &[u8]) -> usize {
    let off = iter::zip(xs.chunks_exact(N), ys.chunks_exact(N))
        .take_while(|(x, y)| x == y)
        .count()
        * N;
    off + iter::zip(&xs[off..], &ys[off..])
        .take_while(|(x, y)| x == y)
        .count()
}
