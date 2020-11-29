use super::super::{Bytes, IntKv};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

/// `IntKv` based on filesystem.
pub struct FsIntKv {
    dir: PathBuf,
}

impl FsIntKv {
    pub fn new(path: &Path) -> io::Result<Self> {
        let result = Self {
            dir: path.to_path_buf(),
        };
        Ok(result)
    }

    fn get_path_for_index(&self, index: usize) -> PathBuf {
        let b = index & 0xff;
        let r = index >> 8;
        self.dir.join(b.to_string()).join(r.to_string())
    }
}

impl IntKv for FsIntKv {
    fn read(&self, index: usize) -> io::Result<Bytes> {
        fs::read(self.get_path_for_index(index)).map(|b| b.into())
    }

    fn write(&mut self, index: usize, data: Bytes) -> io::Result<()> {
        let path = self.get_path_for_index(index);
        fs::create_dir_all(path.parent().unwrap())?;
        fs::write(path, &data)
    }

    fn remove(&mut self, index: usize) -> io::Result<()> {
        let path = self.get_path_for_index(index);
        fs::remove_file(&path)?;
        let _ = fs::remove_dir(path.parent().unwrap());
        Ok(())
    }

    fn has(&self, index: usize) -> io::Result<bool> {
        let path = self.get_path_for_index(index);
        Ok(path.exists())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn test_fsint_kv() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    let mut kv = FsIntKv::new(&path).unwrap();
    super::super::test_int_kv(&mut kv, 10);
}
