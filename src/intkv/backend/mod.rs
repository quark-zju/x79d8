mod fs;
mod mem;

pub use fs::FsIntKv;
#[cfg(test)]
pub use mem::MemIntKv;
