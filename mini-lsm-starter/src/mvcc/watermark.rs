#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::collections::BTreeMap;

pub struct Watermark {
    readers: BTreeMap<u64, usize>,
}

impl Default for Watermark {
    fn default() -> Self {
        Self::new()
    }
}

impl Watermark {
    pub fn new() -> Self {
        Self {
            readers: BTreeMap::new(),
        }
    }

    pub fn add_reader(&mut self, ts: u64) {
        *self.readers.entry(ts).or_insert(0) += 1;
    }

    pub fn remove_reader(&mut self, ts: u64) {
        let new_val = *self.readers.get(&ts).unwrap() - 1;
        if new_val == 0 {
            self.readers.remove(&ts);
        } else {
            self.readers.insert(ts, new_val);
        }
    }

    pub fn watermark(&self) -> Option<u64> {
        self.readers.first_key_value().map(|(k, v)| *k)
    }

    pub fn num_retained_snapshots(&self) -> usize {
        self.readers.len()
    }
}
