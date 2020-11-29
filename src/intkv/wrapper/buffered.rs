use super::super::{Bytes, IntKv};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::io;

/// Buffered IntKv. Writes are buffered until `flush()`.
pub struct BufferedIntKv {
    /// Cached.
    cache: RwLock<HashMap<usize, State>>,

    /// Changed in this layer.
    changes: HashMap<usize, Option<Bytes>>,

    kv: Box<dyn IntKv>,
}

#[derive(Debug, Clone)]
#[repr(u8)]
enum State {
    Unknown,

    /// Removed in this layer. When flush, call kv.remove().
    Data(Bytes),

    /// Not exist in the original kv. When flush, do nothing.
    Has(bool),
}

impl BufferedIntKv {
    pub fn new(kv: Box<dyn IntKv>) -> Self {
        Self {
            cache: Default::default(),
            changes: Default::default(),
            kv,
        }
    }

    fn get_changed(&self, index: usize) -> io::Result<Option<Bytes>> {
        match self.changes.get(&index) {
            None => Ok(None),
            // Removed.
            Some(None) => Err(io::ErrorKind::NotFound.into()),
            Some(Some(b)) => Ok(Some(b.clone())),
        }
    }

    fn get_cache(&self, index: usize) -> State {
        self.cache
            .read()
            .get(&index)
            .cloned()
            .unwrap_or(State::Unknown)
    }
}

impl IntKv for BufferedIntKv {
    fn read(&self, index: usize) -> io::Result<Bytes> {
        if let Some(b) = self.get_changed(index)? {
            return Ok(b);
        }
        match self.get_cache(index) {
            State::Has(false) => Err(io::ErrorKind::NotFound.into()),
            State::Unknown => {
                // Load content from kv.
                let b = match self.kv.read(index) {
                    Err(e) => {
                        if e.kind() == io::ErrorKind::NotFound {
                            self.cache.write().insert(index, State::Has(false));
                        }
                        return Err(e);
                    }
                    Ok(b) => b,
                };
                self.cache.write().insert(index, State::Data(b.clone()));
                Ok(b)
            }
            State::Has(true) => {
                let b = self.kv.read(index)?;
                self.cache.write().insert(index, State::Data(b.clone()));
                Ok(b)
            }
            State::Data(b) => Ok(b),
        }
    }

    fn write(&mut self, index: usize, data: Bytes) -> io::Result<()> {
        self.changes.insert(index, Some(data));
        Ok(())
    }

    fn remove(&mut self, index: usize) -> io::Result<()> {
        if self.has(index)? {
            self.changes.insert(index, None);
            Ok(())
        } else {
            Err(io::ErrorKind::NotFound.into())
        }
    }

    fn has(&self, index: usize) -> io::Result<bool> {
        match self.changes.get(&index) {
            Some(Some(_)) => return Ok(true),
            Some(None) => return Ok(false),
            None => {}
        }
        match self.get_cache(index) {
            State::Unknown => {
                let b = self.kv.has(index)?;
                self.cache.write().insert(index, State::Has(b));
                Ok(b)
            }
            State::Has(b) => Ok(b),
            State::Data(_) => Ok(true),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut cache = self.cache.write();
        for (id, v) in self.changes.drain() {
            match v {
                None => {
                    // Need remove.
                    if self.kv.has(id)? {
                        self.kv.remove(id)?;
                        cache.insert(id, State::Has(false));
                    }
                }
                Some(d) => {
                    // Need write.
                    self.kv.write(id, d.clone())?;
                    cache.insert(id, State::Data(d));
                }
            }
        }
        self.kv.flush()
    }
}

#[test]
fn test_buffered() {
    let kv = super::super::backend::MemIntKv::new();
    let mut kv = BufferedIntKv::new(Box::new(kv));
    kv.flush().unwrap();
    super::super::test_int_kv(&mut kv, 100);
    kv.flush().unwrap();
    super::super::test_int_kv(&mut kv, 200);
    kv.flush().unwrap();
    super::super::test_int_kv(&mut kv, 100);
}
