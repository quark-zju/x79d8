use super::super::{Bytes, IntKv};
use std::collections::BTreeMap;
use std::io;

pub type MemIntKv = BTreeMap<usize, Bytes>;

impl IntKv for MemIntKv {
    fn read(&self, index: usize) -> io::Result<Bytes> {
        self.get(&index)
            .cloned()
            .ok_or_else(|| io::ErrorKind::NotFound.into())
    }

    fn write(&mut self, index: usize, data: Bytes) -> io::Result<()> {
        self.insert(index, data);
        Ok(())
    }

    fn remove(&mut self, index: usize) -> io::Result<()> {
        match self.remove(&index) {
            Some(_) => Ok(()),
            None => Err(io::ErrorKind::NotFound.into()),
        }
    }

    fn has(&self, index: usize) -> io::Result<bool> {
        Ok(self.contains_key(&index))
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn test_mem_int_kv() {
    super::super::test_int_kv(|kv| kv.unwrap_or_else(MemIntKv::new), 200);
}
