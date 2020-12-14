use super::super::{Bytes, IntKv};
use aes::Aes256;
use blake2::{Blake2s, Digest};
use cfb_mode::cipher::{NewStreamCipher, StreamCipher};
use cfb_mode::Cfb;
use rand::RngCore;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::io;

type Bits256 = [u8; 32];
type Bits128 = [u8; 16];
type AesCfb = Cfb<Aes256>;

/// Wrap an `IntKv` with encryption.
///
/// Each entry will be encrypted by AES256-CFB, with IV derived from 3 values:
/// the master key, the integer index, and a 63-bit `Count` stored in the first
/// 8 bytes of the block. The `Count` is preserved upon deletion to avoid
/// reusing IVs.
pub struct EncInKv {
    /// The master key.
    key: Bits256,

    /// Random number generator.
    rng: Box<dyn RngCore>,

    /// The inner `IntKv` backend.
    kv: Box<dyn IntKv>,
}

impl EncInKv {
    pub fn new(key: Bits256, rng: Box<dyn RngCore>, kv: Box<dyn IntKv>) -> Self {
        Self { key, rng, kv }
    }

    /// Get iv from blake2s(key, index, count).
    fn iv(&self, index: usize, count: Count) -> Bits128 {
        debug_assert!(!count.is_deleted());
        let mut b = Blake2s::new();
        b.update(&self.key);
        b.update(&count.to_bytes());
        b.update(&(index as u64).to_be_bytes());
        b.finalize().as_slice()[0..16].try_into().unwrap()
    }

    fn cipher(&self, index: usize, count: Count) -> AesCfb {
        let iv = self.iv(index, count);
        AesCfb::new(&self.key.into(), &iv.into())
    }
}

impl IntKv for EncInKv {
    fn read(&self, index: usize) -> io::Result<Bytes> {
        let data = self.kv.read(index)?;
        let count = Count::read_from(&data)?;
        if count.is_deleted() {
            return Err(io::ErrorKind::NotFound.into());
        }
        let mut cipher = self.cipher(index, count);
        let mut data = data[8..].to_vec();
        cipher.decrypt(&mut data);
        Ok(data.into())
    }

    fn write(&mut self, index: usize, data: Bytes) -> io::Result<()> {
        let count = if self.kv.has(index)? {
            let old_data = self.kv.read(index)?;
            Count::read_from(&old_data)?.bump().with_deleted(false)
        } else {
            Count::new_random(self.rng.as_mut()).with_deleted(false)
        };
        let mut new_data = Vec::with_capacity(data.len() + 8);
        new_data.extend_from_slice(&count.to_bytes());
        new_data.extend_from_slice(&data);
        let mut cipher = self.cipher(index, count);
        cipher.encrypt(&mut new_data[8..]);
        self.kv.write(index, new_data.into())
    }

    fn remove(&mut self, index: usize) -> io::Result<()> {
        let old_data = self.kv.read(index)?;
        let count = Count::read_from(&old_data)?.bump().with_deleted(true);
        let new_data = count.to_bytes().to_vec();
        self.kv.write(index, new_data.into())
    }

    fn has(&self, index: usize) -> io::Result<bool> {
        if !self.kv.has(index)? {
            Ok(false)
        } else {
            let old_data = self.kv.read(index)?;
            let count = Count::read_from(&old_data)?;
            Ok(!count.is_deleted())
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.kv.flush()
    }
}

/// The "count" as the header of blocks to help avoid IV reuse.
/// The highest bit is used to indicate "deletion".
#[derive(Debug, Copy, Clone)]
struct Count(u64);

impl Count {
    fn new_random(rng: &mut dyn RngCore) -> Self {
        Self(rng.next_u64())
    }

    fn read_from(data: &[u8]) -> io::Result<Self> {
        match data.get(0..8) {
            None => Err(io::ErrorKind::UnexpectedEof.into()),
            Some(v) => Ok(Self(u64::from_be_bytes(<[u8; 8]>::try_from(v).unwrap()))),
        }
    }

    fn is_deleted(&self) -> bool {
        (self.0 & 0x8000000000000000) != 0
    }

    fn with_deleted(self, deleted: bool) -> Self {
        if deleted {
            Self(self.0 | 0x8000000000000000u64)
        } else {
            Self(self.0 & 0x7fffffffffffffffu64)
        }
    }

    fn bump(self) -> Self {
        Self(self.0.wrapping_add(1))
    }

    fn to_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
}

#[test]
fn test_enc_kv() {
    super::super::test_int_kv(
        |opt_kv| {
            opt_kv.unwrap_or_else(|| {
                let kv = super::super::backend::MemIntKv::new();
                let key = [0; 32];
                let rng: rand_chacha::ChaChaRng = rand::SeedableRng::from_seed(Default::default());
                EncIntKv::from_key_rng_kv(key, Box::new(rng), Box::new(kv))
            })
        },
        50,
    );
}
