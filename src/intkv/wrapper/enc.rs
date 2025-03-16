use super::super::{Bytes, IntKv};
use aes::cipher::AsyncStreamCipher as _;
use aes::cipher::KeyIvInit as _;
use aes::Aes256;
use blake2::{Blake2s256 as Blake2s, Digest};
use rand::RngCore;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::fmt;
use std::io;

type Bits256 = [u8; 32];
type Bits128 = [u8; 16];

type AesCfbEnc = cfb_mode::Encryptor<Aes256>;
type AesCfbDec = cfb_mode::Decryptor<Aes256>;

pub const IV_HEADER_SIZE: usize = 16;

/// Wrap an `IntKv` with encryption.
///
/// Each entry will be encrypted by AES256-CFB, with IV derived from 3 values:
/// the master key, the integer index, and a 63-bit `Count` stored in the first
/// 8 bytes of the block. The `Count` is preserved upon deletion to avoid
/// reusing IVs.
pub struct EncIntKv {
    /// The master key.
    key: Bits256,

    /// Random number generator.
    rng: Box<dyn RngCore + Send + Sync>,

    /// The inner `IntKv` backend.
    kv: Box<dyn IntKv>,
}

impl fmt::Debug for EncIntKv {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EncIntKv")
            .field("key", &self.key)
            .field("kv", &self.kv)
            .finish()
    }
}

impl EncIntKv {
    pub const fn iv_header_size() -> usize {
        IV_HEADER_SIZE
    }

    pub fn from_key_rng_kv(
        key: Bits256,
        rng: Box<dyn RngCore + Send + Sync>,
        kv: Box<dyn IntKv>,
    ) -> Self {
        Self { key, rng, kv }
    }

    pub fn from_key_kv(key: Bits256, kv: Box<dyn IntKv>) -> Self {
        let rng: rand_chacha::ChaChaRng = rand::SeedableRng::from_seed(Default::default());
        Self::from_key_rng_kv(key, Box::new(rng), kv)
    }

    /// Get iv from blake2s(key, count, index).
    fn iv(&self, index: usize, count: Count) -> Bits128 {
        let mut b = Blake2s::new();
        b.update(self.key);
        b.update(count.to_bytes());
        b.update((index as u64).to_be_bytes());
        b.finalize().as_slice()[0..16].try_into().unwrap()
    }

    fn cipher_enc(&self, index: usize, count: Count) -> AesCfbEnc {
        let iv = self.iv(index, count);
        AesCfbEnc::new(&self.key.into(), &iv.into())
    }

    fn cipher_dec(&self, index: usize, count: Count) -> AesCfbDec {
        let iv = self.iv(index, count);
        AesCfbDec::new(&self.key.into(), &iv.into())
    }
}

impl IntKv for EncIntKv {
    fn read(&self, index: usize) -> io::Result<Bytes> {
        let data = self.kv.read(index)?;
        let count = Count::read_from(&data)?;
        let cipher = self.cipher_dec(index, count);
        let mut data = data[IV_HEADER_SIZE..].to_vec();
        log::info!("Decrypt {} ({} bytes)", index, data.len());
        cipher.decrypt(&mut data);
        log::debug!("Decrypt {} complete", index);
        Ok(data.into())
    }

    fn write(&mut self, index: usize, data: Bytes) -> io::Result<()> {
        let count = if self.kv.has(index)? {
            let old_data = self.kv.read(index)?;
            Count::read_from(&old_data)?.bump(&mut self.rng)
        } else {
            Count::new_random(self.rng.as_mut())
        };
        let mut new_data = Vec::with_capacity(data.len() + IV_HEADER_SIZE);
        new_data.extend_from_slice(&count.to_bytes());
        new_data.extend_from_slice(&data);
        let cipher = self.cipher_enc(index, count);
        log::info!("Encrypt {} ({} bytes)", index, data.len());
        cipher.encrypt(&mut new_data[IV_HEADER_SIZE..]);
        log::debug!("Encrypt {} complete", index);
        self.kv.write(index, new_data.into())
    }

    fn remove(&mut self, index: usize) -> io::Result<()> {
        // This frees space and forgets about the IV header.
        // It relies on self.rng to avoid IV reuse.
        self.kv.remove(index)
    }

    fn has(&self, index: usize) -> io::Result<bool> {
        self.kv.has(index)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.kv.flush()
    }
}

/// The "count" as the header of blocks to help avoid IV reuse.
/// The highest bit is used to indicate "deletion".
#[derive(Debug, Copy, Clone)]
struct Count(u64, u64);

impl Count {
    fn new_random(rng: &mut dyn RngCore) -> Self {
        Self(rng.next_u64(), rng.next_u64())
    }

    fn read_from(data: &[u8]) -> io::Result<Self> {
        match data.get(0..16) {
            None => Err(io::ErrorKind::UnexpectedEof.into()),
            Some(v) => {
                let v1 = u64::from_be_bytes(<[u8; 8]>::try_from(&v[0..8]).unwrap());
                let v2 = u64::from_be_bytes(<[u8; 8]>::try_from(&v[8..16]).unwrap());
                Ok(Self(v1, v2))
            }
        }
    }

    fn bump(self, rng: &mut dyn RngCore) -> Self {
        let v = rng.next_u64() | 1;
        Self(self.0.wrapping_add(v), self.1.wrapping_add(1))
    }

    fn to_bytes(self) -> [u8; IV_HEADER_SIZE] {
        let mut result = [0u8; 16];
        result[0..8].copy_from_slice(&self.0.to_be_bytes());
        result[8..16].copy_from_slice(&self.1.to_be_bytes());
        result
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
