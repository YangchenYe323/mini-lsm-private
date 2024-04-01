#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::{BufMut, Bytes};

use super::{bloom::Bloom, BlockMeta, SsTable};
use crate::{
    block::BlockBuilder,
    key::{KeyBytes, KeySlice, TS_DEFAULT},
    lsm_storage::BlockCache,
    table::FileObject,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    first_timestamp: u64,
    last_key: Vec<u8>,
    last_timestamp: u64,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    key_hashes: Vec<u32>,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            first_timestamp: TS_DEFAULT,
            last_key: Vec::new(),
            last_timestamp: TS_DEFAULT,
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
            key_hashes: Vec::new(),
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.add_inner(key, value) {
            return;
        }

        // Encode current BlockBuilder to a Block and make a fresh BlockBuilder.
        self.materialize_current_block();

        assert!(
            self.add_inner(key, value),
            "Failed to add entry to an empty block"
        );
    }

    fn add_inner(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let added_to_cur_block = self.builder.add(key, value);

        if added_to_cur_block {
            // Push key hash for building bloom filter
            let h = farmhash::fingerprint32(key.key_ref());
            self.key_hashes.push(h);

            if self.first_key.is_empty() {
                self.first_key = key.key_ref().to_vec();
                self.first_timestamp = key.ts();
            }
            self.last_key = key.key_ref().to_vec();
            self.last_timestamp = key.ts();
        }

        added_to_cur_block
    }

    fn materialize_current_block(&mut self) {
        let old_builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let old_offset = self.data.len();
        let old_first_key = old_builder.first_key().to_key_vec().into_key_bytes();
        let old_last_key =
            KeyBytes::from_bytes_with_ts(self.last_key.clone().into(), self.last_timestamp);
        let old_block = old_builder.build();
        let old_block_bytes = old_block.encode();

        let old_block_checksum = crc32fast::hash(&old_block_bytes);

        self.data.put(old_block.encode());
        self.data.put_u32(old_block_checksum);

        self.meta.push(BlockMeta {
            offset: old_offset,
            first_key: old_first_key,
            last_key: old_last_key,
        });
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len() + self.builder.estimated_size()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        // If there's remaining data in current builder, finish the last block
        if !self.builder.first_key().is_empty() {
            self.materialize_current_block();
        }

        let SsTableBuilder {
            builder: _,
            first_key,
            first_timestamp,
            last_key,
            last_timestamp,
            data,
            meta,
            block_size,
            key_hashes,
        } = self;

        // Build bloom filter
        let bloom = Bloom::build_from_key_hashes(
            &key_hashes,
            Bloom::bloom_bits_per_key(key_hashes.len(), 0.01),
        );

        // Step 1: Buffer starts with data section
        let mut buffer = data;

        // Step 2: Encode blockmeta
        let meta_offset = buffer.len();
        BlockMeta::encode_block_meta(&meta, &mut buffer);
        let meta_buffer = &buffer[meta_offset..];
        let meta_checksum = crc32fast::hash(meta_buffer);
        buffer.put_u32(meta_checksum);
        buffer.put_u32(meta_offset as u32);

        // Step 3: Encode bloom filter
        let bloom_offset = buffer.len();
        bloom.encode(&mut buffer);
        let bloom_buffer = &buffer[bloom_offset..];
        let bloom_checksum = crc32fast::hash(bloom_buffer);
        buffer.put_u32(bloom_checksum);
        buffer.put_u32(bloom_offset as u32);

        let file_object = FileObject::create(path.as_ref(), buffer)?;

        let mut table = SsTable::create_meta_only(
            id,
            file_object.size(),
            KeyBytes::from_bytes_with_ts(Bytes::from(first_key), first_timestamp),
            KeyBytes::from_bytes_with_ts(Bytes::from(last_key), last_timestamp),
        );
        table.file = file_object;
        table.block_cache = block_cache;
        table.block_meta = meta;
        table.block_meta_offset = meta_offset;
        table.bloom = Some(bloom);

        Ok(table)
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
