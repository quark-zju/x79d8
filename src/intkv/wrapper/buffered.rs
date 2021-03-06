use super::super::{Bytes, IntKv};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::{io, sync::atomic::AtomicUsize, sync::atomic::Ordering};

/// Buffered IntKv. Writes are buffered until `flush()`.
#[derive(Debug)]
pub struct BufferedIntKv {
    /// Cached.
    cache: RwLock<HashMap<usize, State>>,

    /// Cache size limit.
    cache_size_limit: usize,
    cache_size: AtomicUsize,

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
            cache_size_limit: 0,
            cache_size: Default::default(),
            kv,
        }
    }

    pub fn with_cache_size_limit(mut self, limit: usize) -> Self {
        self.cache_size_limit = limit;
        self
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
                let size = self.cache_size.fetch_add(b.len(), Ordering::AcqRel);
                let mut cache = self.cache.write();
                if self.cache_size_limit > 0 && size > self.cache_size_limit {
                    // Remove cache to keep size bounded.
                    log::debug!(
                        "Dropping cache (size {} > limit {})",
                        size,
                        self.cache_size_limit
                    );
                    self.cache_size.fetch_sub(size, Ordering::AcqRel);
                    cache.clear();
                }
                cache.insert(index, State::Data(b.clone()));
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
    super::super::test_int_kv(
        |kv| {
            kv.unwrap_or_else(|| {
                BufferedIntKv::new(Box::new(super::super::backend::MemIntKv::new()))
            })
        },
        100,
    );
}
