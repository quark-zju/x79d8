pub mod backend;
pub mod wrapper;

use std::fmt;
use std::io;
use std::ops::Deref;
use std::ops::DerefMut;

pub use minibytes::Bytes;

/// `IntKv` supports reading, writing, or deleting data keyed by integers.
pub trait IntKv: fmt::Debug + Send + Sync + 'static {
    /// Read an entry.
    fn read(&self, index: usize) -> io::Result<Bytes>;

    /// Overwrite an entry.
    fn write(&mut self, index: usize, data: Bytes) -> io::Result<()>;

    /// Delete an entry.
    fn remove(&mut self, index: usize) -> io::Result<()>;

    /// Test if an entry exists.
    fn has(&self, index: usize) -> io::Result<bool>;

    /// Persist pending changes.
    fn flush(&mut self) -> io::Result<()>;
}

impl IntKv for Box<dyn IntKv> {
    fn read(&self, index: usize) -> io::Result<Bytes> {
        self.deref().read(index)
    }

    fn write(&mut self, index: usize, data: Bytes) -> io::Result<()> {
        self.deref_mut().write(index, data)
    }

    fn remove(&mut self, index: usize) -> io::Result<()> {
        self.deref_mut().remove(index)
    }

    fn has(&self, index: usize) -> io::Result<bool> {
        self.deref().has(index)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.deref_mut().flush()
    }
}

#[cfg(test)]
pub(crate) fn test_int_kv<F, K>(mut reload_kv: F, n: usize) -> K
where
    F: FnMut(Option<K>) -> K,
    K: IntKv,
{
    let mut kv = reload_kv(None);
    for i in 0..n {
        let data = vec![i as u8; i * 541];
        kv.write(i, data.into()).unwrap();
    }
    for _ in 0..=1 {
        for i in 0..n {
            let data = vec![i as u8; i * 541];
            assert_eq!(kv.read(i).unwrap(), Bytes::from(data));
            assert!(kv.has(i).unwrap());
        }
        kv.flush().unwrap();
        kv = reload_kv(Some(kv));
    }

    for i in 0..n {
        kv.remove(i).unwrap();
    }
    for _ in 0..=1 {
        for i in 0..n {
            assert!(!kv.has(i).unwrap());
        }
        kv.flush().unwrap();
        kv = reload_kv(Some(kv));
    }

    // Test random operations.
    use rand::*;
    use rand_chacha::ChaChaRng;
    let mut rng = ChaChaRng::from_seed(Default::default());
    use std::collections::BTreeMap;
    let mut m = BTreeMap::new();

    fn rand_id(rng: &mut ChaChaRng, m: &BTreeMap<usize, Bytes>) -> usize {
        let n = (rng.next_u32() as usize) % m.len();
        m.keys().nth(n).cloned().unwrap()
    }

    fn rand_data(rng: &mut ChaChaRng) -> Bytes {
        let len = (1 << (rng.next_u32() % 18)) as u32;
        let len = (rng.next_u32() % len) as usize;
        let b: u8 = (rng.next_u32() & 255) as u8;
        let data = vec![b; len];
        data.into()
    }

    for _ in 0..(n * 10) {
        match rng.next_u32() % 3 {
            0 => {
                // Remove.
                if !m.is_empty() {
                    let id = rand_id(&mut rng, &m);
                    kv.remove(id).unwrap();
                    assert!(!kv.has(id).unwrap());
                    m.remove(&id);
                }
            }
            1 => {
                // Write.
                let id = rng.next_u32() as usize;
                let data = rand_data(&mut rng);
                kv.write(id, data.clone()).unwrap();
                assert!(kv.has(id).unwrap());
                m.insert(id, data);
            }
            2 => {
                // Rewrite.
                if !m.is_empty() {
                    let id = rand_id(&mut rng, &m);
                    let data = rand_data(&mut rng);
                    kv.write(id, data.clone()).unwrap();
                    assert!(kv.has(id).unwrap());
                    m.insert(id, data);
                }
            }
            _ => {}
        }
    }
    for _ in 0..=1 {
        for (k, v) in &m {
            let l = kv.read(*k).ok();
            let r = Some(v.clone());
            assert_eq!(
                l.as_ref().map(|v| v.len()),
                r.as_ref().map(|v| v.len()),
                "read key {}\n----\n{:#?}\n----\n{:#?}\n",
                k,
                &kv,
                &m.iter()
                    .map(|(k, v)| (k, v.len()))
                    .collect::<BTreeMap<_, _>>(),
            );
            assert_eq!(l, r);
        }
        kv.flush().unwrap();
        kv = reload_kv(Some(kv));
    }

    kv
}
