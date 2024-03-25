#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::io::{BufWriter, Seek};
use std::path::Path;
use std::sync::Arc;
use std::{fs::File, io::Write};

use anyhow::Result;
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::create(path.as_ref())?;
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let content = std::fs::read(path.as_ref())?;
        let mut buf = &content[..];
        while buf.remaining() > 0 {
            let key_len = buf.get_u32();
            let key = buf.copy_to_bytes(key_len as usize);
            let value_len = buf.get_u32();
            let value = buf.copy_to_bytes(value_len as usize);
            skiplist.insert(key, value);
        }

        let mut file = File::open(path.as_ref())?;
        file.seek(std::io::SeekFrom::End(0))?;
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut buf = Vec::with_capacity(4 + key.len() + 4 + value.len());
        buf.put_u32(key.len() as u32);
        buf.put_slice(key);
        buf.put_u32(value.len() as u32);
        buf.put_slice(value);

        let mut guard = self.file.lock();
        guard.write_all(&buf)?;

        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut guard = self.file.lock();
        guard.flush()?;
        Ok(())
    }
}
