use bincode::Options;
use serde::{Deserialize, Serialize};
use std::io;

fn bincode_opts() -> impl bincode::Options {
    bincode::options()
        .with_big_endian()
        .with_fixint_encoding()
        .allow_trailing_bytes()
}

/// Bincode deserialize using options preferred by the crate.
pub fn bincode_deserialize<T: for<'a> Deserialize<'a>>(data: &[u8]) -> io::Result<T> {
    bincode_opts()
        .deserialize(data)
        .map_err(|_| io::ErrorKind::InvalidData.into())
}

/// Bincode serialize size using options preferred by the crate.
pub fn bincode_size<T: Serialize>(value: &T) -> u64 {
    bincode_opts().serialized_size(value).unwrap()
}

/// Bincode serialize using options preferred by the crate.
/// If `page_size` is not 0, add padding to `page_size`.
pub fn bincode_serialize_pad<T: Serialize>(value: &T, mut page_size: u64) -> Vec<u8> {
    let opts = bincode_opts();
    if page_size == 0 {
        page_size = bincode_size(value);
    }
    let mut buf = Vec::with_capacity(page_size as _);
    opts.serialize_into(&mut buf, value).unwrap();
    debug_assert_eq!(buf.len() as u64, bincode_size(value));
    assert!(buf.len() <= page_size as _);
    // Padding
    buf.resize(page_size as _, 0);
    buf
}
