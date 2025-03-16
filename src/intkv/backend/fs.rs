use super::super::{Bytes, IntKv};
use memmap::MmapOptions;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};
use tempfile::NamedTempFile;

/// `IntKv` based on filesystem.
///
/// Changes will be write to disk but will not be visible to new `FsIntKv`
/// instances until `flush()`.
///
/// `flush()` ensures changes are atomic by using WAL:
/// 1. fsync files to write using "pending" names (suffix "p").
/// 2. Write WAL about what files to replace or delete.
/// 3. Rename the pending files. Remove deleted files.
/// 4. Remove WAL.
///
/// If the program was killed during `flush()`, the next `FsIntKv` will
/// try to redo WAL to complete partially modified state.
#[derive(Debug)]
pub struct FsIntKv {
    dir: PathBuf,
    overlay: HashMap<usize, State>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
enum State {
    /// Modified. Stored using suffix "p".
    Modified,

    /// Removed.
    Removed,
}

impl FsIntKv {
    pub fn new(path: &Path) -> io::Result<Self> {
        let kv = Self {
            dir: path.to_path_buf(),
            overlay: Default::default(),
        };

        // Redo WAL on previous crash.
        if kv.wal_path().exists() {
            log::info!("Re-committing WAL");
            kv.wal_checkpoint()?;
        }

        Ok(kv)
    }

    fn get_path_for_index(&self, index: usize) -> PathBuf {
        let in_wal = match self.overlay.get(&index) {
            Some(State::Modified) => true,
            _ => false,
        };
        self.get_path_for_index_wal(index, in_wal)
    }

    fn get_path_for_index_wal(&self, index: usize, in_wal: bool) -> PathBuf {
        let name = match in_wal {
            true => format!("{}p", index),
            false => index.to_string(),
        };
        self.dir.join(name)
    }
}

impl IntKv for FsIntKv {
    fn read(&self, index: usize) -> io::Result<Bytes> {
        if let Some(State::Removed) = self.overlay.get(&index) {
            return Err(io::ErrorKind::NotFound.into());
        }
        let path = self.get_path_for_index(index);
        let file = fs::OpenOptions::new().read(true).open(path)?;
        let bytes: Bytes = if file.metadata()?.len() == 0 {
            Bytes::new()
        } else {
            // Use mmap to read files.
            unsafe { MmapOptions::new().map(&file) }?.into()
        };
        // fs::read(self.get_path_for_index(index)).map(|b| b.into())
        Ok(bytes)
    }

    fn write(&mut self, index: usize, data: Bytes) -> io::Result<()> {
        self.overlay.insert(index, State::Modified);
        let path = self.get_path_for_index(index);
        fs::write(path, &data)
    }

    fn remove(&mut self, index: usize) -> io::Result<()> {
        match self.overlay.get(&index).cloned() {
            Some(State::Removed) => {
                return Err(io::ErrorKind::NotFound.into());
            }
            Some(State::Modified) => {
                let path = self.get_path_for_index(index);
                fs::remove_file(&path)?;
                self.overlay.insert(index, State::Removed);
            }
            None => {
                if !self.has(index)? {
                    return Err(io::ErrorKind::NotFound.into());
                }
                self.overlay.insert(index, State::Removed);
            }
        }
        Ok(())
    }

    fn has(&self, index: usize) -> io::Result<bool> {
        match self.overlay.get(&index).cloned() {
            Some(State::Removed) => Ok(false),
            Some(State::Modified) => Ok(true),
            None => {
                let path = self.get_path_for_index(index);
                Ok(path.exists())
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flush_wal()
    }
}

impl FsIntKv {
    fn flush_wal(&mut self) -> io::Result<()> {
        if self.overlay.is_empty() {
            return Ok(());
        }

        // Step 1: Fsync pending files.
        for (&index, &state) in self.overlay.iter() {
            match state {
                State::Modified => {
                    let path = self.get_path_for_index(index);
                    let file = fs::OpenOptions::new().read(true).write(true).open(&path)?;
                    file.sync_all()?;
                }
                State::Removed => {}
            }
        }

        // Step 2: Write WAL.
        log::info!("Writing WAL of {} entries", self.overlay.len());
        let wal_bytes = bincode::serialize(&self.overlay).unwrap();
        let mut wal_file = NamedTempFile::new_in(self.dir.join(""))?;
        wal_file.write_all(&wal_bytes)?;
        wal_file.as_file().sync_data()?;
        wal_file.persist_noclobber(self.wal_path())?;

        // Step 3: Apply WAL. Clear internal state.
        log::info!("Committing WAL");
        self.wal_checkpoint()?;
        self.overlay = Default::default();

        Ok(())
    }

    fn wal_path(&self) -> PathBuf {
        const WAL_NAME: &str = "wal";
        self.dir.join(WAL_NAME)
    }

    /// Persist WAL to disk.
    fn wal_checkpoint(&self) -> io::Result<()> {
        let wal_path = self.wal_path();
        let wal_data = ignore_not_found(fs::read(self.wal_path()))?;
        if wal_data.is_empty() {
            return Ok(());
        }
        let overlay: HashMap<usize, State> = bincode::deserialize(&wal_data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        // Apply WAL: Rename or remove files.
        for (&index, &state) in overlay.iter() {
            match state {
                State::Modified => {
                    log::info!("Committing {}", index);
                    let wal_path = self.get_path_for_index_wal(index, true);
                    if wal_path.exists() {
                        let dest_path = self.get_path_for_index_wal(index, false);
                        fs::rename(wal_path, dest_path)?;
                    }
                }
                State::Removed => {
                    log::info!("Removing {}", index);
                    let dest_path = self.get_path_for_index_wal(index, false);
                    ignore_not_found(fs::remove_file(&dest_path))?;
                }
            }
        }

        ignore_not_found(fs::remove_file(wal_path))?;
        Ok(())
    }
}

fn ignore_not_found<T: Default>(result: io::Result<T>) -> io::Result<T> {
    match result {
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(T::default()),
        _ => result,
    }
}

#[test]
fn test_fsint_kv() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    super::super::test_int_kv(|_| FsIntKv::new(&path).unwrap(), 10);
}
