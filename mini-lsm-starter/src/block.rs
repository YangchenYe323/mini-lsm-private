#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

use crate::block::builder::{KEY_LEN_SIZE, VALUE_LEN_SIZE};

use self::builder::OFFSET_SIZE;

pub(crate) const NUM_ELEMENT_SIZE: usize = 2;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut output =
            Vec::with_capacity(self.data.len() + self.offsets.len() + NUM_ELEMENT_SIZE);
        output.put_slice(&self.data);
        self.offsets
            .iter()
            .for_each(|offset| output.put_u16(*offset));
        output.put_u16(self.offsets.len() as u16);

        Bytes::from(output)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let num_element_start = data.len() - NUM_ELEMENT_SIZE;
        let num_element = (&data[num_element_start..]).get_u16();
        let mut offset_start = 0;

        // Skip first key: KEY_LEN + KEY + TIMESTAMP
        let first_key_len = (&data[offset_start..offset_start + KEY_LEN_SIZE]).get_u16();
        offset_start += KEY_LEN_SIZE + first_key_len as usize + 8;
        // Skip first value
        let first_val_len = (&data[offset_start..offset_start + VALUE_LEN_SIZE]).get_u16();
        offset_start += VALUE_LEN_SIZE + first_val_len as usize;

        for _ in 1..num_element {
            let rest_len = (&data
                [offset_start + KEY_LEN_SIZE..offset_start + KEY_LEN_SIZE + KEY_LEN_SIZE])
                .get_u16();
            offset_start += KEY_LEN_SIZE + KEY_LEN_SIZE + rest_len as usize + 8;
            let val_len = (&data[offset_start..offset_start + VALUE_LEN_SIZE]).get_u16();
            offset_start += VALUE_LEN_SIZE + val_len as usize;
        }

        let data_section = data[..offset_start].to_vec();
        let offset_section = (offset_start..num_element_start)
            .step_by(OFFSET_SIZE)
            .map(|start| (&data[start..start + OFFSET_SIZE]).get_u16())
            .collect();

        Self {
            data: data_section,
            offsets: offset_section,
        }
    }
}
