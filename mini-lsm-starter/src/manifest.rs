#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::fs::File;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::{Buf, BufMut};
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::create(path.as_ref())?;
        Ok(Self {
            file: Arc::new(Mutex::new(file)),
        })
    }

    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let file = OpenOptions::new()
            .append(true)
            .read(true)
            .open(path.as_ref())?;
        let buffer = std::fs::read(path.as_ref())?;
        let mut buf = &buffer[..];
        let mut records = Vec::new();
        while buf.remaining() > 0 {
            let length = buf.get_u32();
            let record_buffer = &buf[..length as usize];
            let mut checksum_buffer = &buf[length as usize..length as usize + 4];
            let checksum = checksum_buffer.get_u32();
            let actual_checksum = crc32fast::hash(record_buffer);
            assert_eq!(
                checksum, actual_checksum,
                "Manifest checksum mismatch: {} != {}",
                checksum, actual_checksum
            );

            let record = serde_json::from_slice(record_buffer)?;
            records.push(record);
            buf.advance(length as usize + 4);
        }

        Ok((
            Self {
                file: Arc::new(Mutex::new(file)),
            },
            records,
        ))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, record: ManifestRecord) -> Result<()> {
        let mut buffer = serde_json::to_vec(&record)?;
        let length = buffer.len();
        let checksum = crc32fast::hash(&buffer);

        let mut buf = Vec::with_capacity(buffer.len() + 8);
        buf.put_u32(length as u32);
        buf.append(&mut buffer);
        buf.put_u32(checksum);

        let mut guard = self.file.lock();
        guard.write_all(&buf)?;
        Ok(())
    }
}
