#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::fs::OpenOptions;
use std::path::Path;
use std::sync::Arc;
use std::{fs::File, ops::DerefMut};

use anyhow::{Context, Result};
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

        let mut records = Vec::new();
        for result in serde_json::Deserializer::from_reader(&file).into_iter() {
            let record = result?;
            records.push(record);
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
        let mut guard = self.file.lock();
        serde_json::to_writer(guard.deref_mut(), &record)
            .context("Failed to write manifest record")?;
        Ok(())
    }
}
