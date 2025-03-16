use crate::intkv::Bytes;
use crate::intkv::IntKv;
use crate::util;
use libunftp::storage;
use libunftp::storage::Error;
use libunftp::storage::ErrorKind;
use libunftp::storage::Fileinfo;
use libunftp::storage::Metadata;
use libunftp::storage::Result;
use libunftp::storage::StorageBackend;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::io;
use std::sync::{atomic::AtomicU64, atomic::Ordering, Arc};
use std::time::SystemTime;
use std::{
    collections::BTreeMap,
    ffi::OsStr,
    path::{Component, Path},
};
use tokio::io::AsyncReadExt;
use tokio::time::Duration;

macro_rules! denied {
    ($($t:tt)*) => {
        return Err(Error::new(
            ErrorKind::PermissionDenied,
            format!($($t)*),
        ));
    };
}

const WRITE_DELAY_SECS: u64 = 5;

/// Expose `IntKv` as a libunftp filesystem.
#[derive(Debug, Clone)]
pub struct IntKvFtpFs {
    kv: Arc<RwLock<Box<dyn IntKv>>>,
    flush_timer_id: Arc<AtomicU64>,
}

impl IntKvFtpFs {
    pub fn new(kv: Box<dyn IntKv>) -> Self {
        Self {
            kv: Arc::new(RwLock::new(kv)),
            flush_timer_id: Default::default(),
        }
    }

    fn schedule_flush(&self) {
        let kv = self.kv.clone();
        let timer_id1 = self.flush_timer_id.clone();
        let timer_id2 = self
            .flush_timer_id
            .fetch_add(1, Ordering::AcqRel)
            .wrapping_add(1);
        tokio::task::spawn(async move {
            tokio::time::sleep(Duration::from_secs(WRITE_DELAY_SECS)).await;
            if timer_id1.load(Ordering::Acquire) == timer_id2 {
                maybe_flush(&kv)
            }
        });
    }

    pub(crate) fn flush(&mut self) -> io::Result<()> {
        self.kv.write().flush()
    }
}

fn maybe_flush(kv: &Arc<RwLock<Box<dyn IntKv>>>) {
    log::info!("Writing changes to disk");
    if let Err(e) = kv.write().flush() {
        log::error!("Cannot flush: {:?}", e)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
struct Tree {
    items: BTreeMap<String, (u64, Meta)>,

    #[serde(skip)]
    index: u64,
}

impl Tree {
    fn find(&self, name: &str) -> Result<&(u64, Meta)> {
        match self.items.get(name) {
            Some(v) => Ok(v),
            None => Err(Error::new(
                ErrorKind::PermanentFileNotAvailable,
                format!("{} does not exist in tree {}", name, self.index),
            )),
        }
    }

    fn has(&self, name: &str) -> bool {
        self.items.contains_key(name)
    }
}

const ROOT_ID: u64 = 0;

trait IntKvFsExt: IntKv {
    fn read_tree_by_id(&self, index: u64) -> Result<Tree> {
        log::debug!("read_tree_by_id {} {:p}", index, self);
        // PERF: Caching?
        let kv = self;
        if index == ROOT_ID && !kv.has(index as _)? {
            return Ok(Tree::default());
        }
        let bytes = kv.read(index as _)?;
        let mut tree: Tree = util::bincode_deserialize(&bytes).map_err(|_| local_error())?;
        tree.index = index;
        Ok(tree)
    }

    fn find_free_index(&self) -> Result<usize> {
        // PERF: This can be improved.
        loop {
            let i: u32 = rand::random();
            if !self.has(i as _)? {
                log::debug!("find_free_index => {}", i);
                return Ok(i as _);
            }
        }
    }

    fn create_blob(&mut self, data: Bytes) -> Result<usize> {
        let index = self.find_free_index()?;
        self.write(index, data)?;
        Ok(index)
    }

    fn create_tree(&mut self) -> Result<Tree> {
        let kv = self;
        let tree = Tree {
            index: kv.find_free_index()? as _,
            ..Tree::default()
        };
        kv.write_tree(&tree)?;
        Ok(tree)
    }

    fn write_tree(&mut self, tree: &Tree) -> Result<()> {
        log::debug!("write_tree {:#?}", tree);
        let index = tree.index;
        let bytes = util::bincode_serialize_pad(&tree, 0);
        self.write(index as _, bytes.into())?;
        debug_assert_eq!(
            self.read_tree_by_id(index as _)?.items.len(),
            tree.items.len()
        );
        Ok(())
    }

    fn read_blob_by_index(&self, index: u64) -> Result<Bytes> {
        Ok(self.read(index as _)?)
    }

    fn read_blob_by_path(&self, path: &Path) -> Result<Bytes> {
        let (id, meta) = self.read_id_meta_by_path(path)?;
        if !meta.is_file() {
            denied!("{} is not a file", path.display());
        }
        self.read_blob_by_index(id)
    }

    fn write_blob(&mut self, index: u64, data: Bytes) -> Result<()> {
        Ok(self.write(index as _, data)?)
    }

    fn remove_blob(&mut self, index: u64) -> Result<()> {
        log::debug!("Remove blob {}", index);
        Ok(self.remove(index as _)?)
    }

    fn root_tree(&self) -> Result<Tree> {
        self.read_tree_by_id(ROOT_ID)
    }

    fn read_tree_by_path(&self, path: &Path) -> Result<Tree> {
        log::debug!("read_tree_by_path {}", path.display());
        let mut tree = self.root_tree()?;
        for name in path.components() {
            match name {
                Component::RootDir => {}
                Component::Prefix(_) | Component::CurDir | Component::ParentDir => {
                    return Err(ErrorKind::FileNameNotAllowedError.into())
                }
                Component::Normal(s) => {
                    let s = to_str(s)?;
                    let (index, meta) = tree.find(s)?;
                    if meta.is_dir() {
                        tree = self.read_tree_by_id(*index)?;
                        continue;
                    } else {
                        return Err(Error::new(
                            ErrorKind::PermanentFileNotAvailable,
                            format!("{} is not a dir in tree {:?}", s, &tree),
                        ));
                    }
                }
            }
        }
        Ok(tree)
    }

    fn read_tree_name_from_path<'a>(&self, path: &'a Path) -> Result<(Tree, &'a str)> {
        let tree = match path.parent() {
            None => self.root_tree()?,
            Some(p) => self.read_tree_by_path(p)?,
        };
        match path.file_name() {
            None => Err(Error::new(
                ErrorKind::PermanentFileNotAvailable,
                format!("{} does not have a filename", path.display()),
            )),
            Some(f) => Ok((tree, to_str(f)?)),
        }
    }

    fn read_id_meta_by_path(&self, path: &Path) -> Result<(u64, Meta)> {
        let (tree, name) = self.read_tree_name_from_path(path)?;
        match tree.items.get(name).cloned() {
            None => Err(Error::new(
                ErrorKind::PermanentFileNotAvailable,
                format!("{} does not exist in tree {}", name, tree.index),
            )),
            Some(p) => Ok(p),
        }
    }
}

impl<T: IntKv> IntKvFsExt for T {}

#[async_trait::async_trait]
impl<U: Send + Sync + Debug> StorageBackend<U> for IntKvFtpFs {
    /// The concrete type of the _metadata_ used by this storage backend.
    type Metadata = Meta;

    /// Tells which optional features are supported by the storage back-end
    /// Return a value with bits set according to the FEATURE_* constants.
    fn supported_features(&self) -> u32 {
        storage::FEATURE_RESTART
    }

    /// Returns the `Metadata` for the given file.
    ///
    /// [`Metadata`]: ./trait.Metadata.html
    async fn metadata<P: AsRef<Path> + Send + Debug>(
        &self,
        _user: &Option<U>,
        path: P,
    ) -> Result<Self::Metadata> {
        let path = path.as_ref();
        let kv = self.kv.read();
        kv.read_id_meta_by_path(path).map(|(_i, m)| m)
    }

    /// Returns the list of files in the given directory.
    async fn list<P: AsRef<Path> + Send + Debug>(
        &self,
        _user: &Option<U>,
        path: P,
    ) -> Result<Vec<Fileinfo<std::path::PathBuf, Self::Metadata>>>
    where
        <Self as StorageBackend<U>>::Metadata: Metadata,
    {
        let kv = self.kv.read();
        let path = path.as_ref();
        let tree = kv.read_tree_by_path(path)?;
        let files = tree
            .items
            .iter()
            .map(|(name, (_id, meta))| Fileinfo {
                path: path.join(name),
                metadata: meta.clone(),
            })
            .collect();
        Ok(files)
    }

    /// Returns the content of the given file from offset start_pos.
    /// The starting position will only be greater than zero if the storage back-end implementation
    /// advertises to support partial reads through the supported_features method i.e. the result
    /// from supported_features yield 1 if a logical and operation is applied with FEATURE_RESTART.
    async fn get<P: AsRef<Path> + Send + Debug>(
        &self,
        _user: &Option<U>,
        path: P,
        start_pos: u64,
    ) -> Result<Box<dyn tokio::io::AsyncRead + Send + Sync + Unpin>> {
        let path = path.as_ref();
        let blob = self.kv.read().read_blob_by_path(path)?;
        if blob.len() as u64 <= start_pos {
            static EMPTY: &[u8] = b"";
            Ok(Box::new(EMPTY))
        } else {
            let blob = blob.slice((start_pos as usize)..);
            Ok(Box::new(io::Cursor::new(blob)))
        }
    }

    /// Writes bytes from the given reader to the specified path starting at offset start_pos in the file
    async fn put<
        P: AsRef<Path> + Send + Debug,
        R: tokio::io::AsyncRead + Send + Sync + Unpin + 'static,
    >(
        &self,
        _user: &Option<U>,
        mut input: R,
        path: P,
        start_pos: u64,
    ) -> Result<u64> {
        let path = path.as_ref();
        let mut buf = Vec::new();
        if start_pos > 0 {
            // Read existing parts.
            let kv = self.kv.read();
            let blob = kv.read_blob_by_path(path)?;
            if (blob.len() as u64) < start_pos {
                denied!(
                    "put: {} is shorter ({}) than start_pos ({})",
                    path.display(),
                    blob.len(),
                    start_pos
                );
            }
            buf.extend_from_slice(&blob.slice(0..(start_pos as usize)));
        }

        input.read_to_end(&mut buf).await?;
        let written = (buf.len() as u64) - start_pos;
        let data: Bytes = buf.into();
        let mut kv = self.kv.write();
        let (mut tree, name) = kv.read_tree_name_from_path(path)?;
        let (index, meta) = if let Some((index, mut meta)) = tree.items.get(name).cloned() {
            if !meta.is_file() {
                denied!("put: {} is not a file", path.display());
            }
            meta.len = data.len() as _;
            meta.mtime = SystemTime::now();
            kv.write_blob(index, data)?;
            (index, meta)
        } else {
            // Create a new file.
            let meta = Meta::new_file(data.len() as _);
            let index = kv.create_blob(data)? as u64;
            (index, meta)
        };
        tree.items.insert(name.to_string(), (index as _, meta));
        kv.write_tree(&tree)?;
        self.schedule_flush();
        Ok(written)
    }

    /// Deletes the file at the given path.
    async fn del<P: AsRef<Path> + Send + Debug>(&self, _user: &Option<U>, path: P) -> Result<()> {
        let path = path.as_ref();
        let mut kv = self.kv.write();
        let (mut tree, name) = kv.read_tree_name_from_path(path)?;
        let (id, meta) = tree.find(name)?;
        let id = *id;
        // Must be a file to delete.
        if !meta.is_file() {
            denied!("del: {} is not a file", path.display());
        }
        tree.items.remove(name);
        kv.write_tree(&tree)?;
        kv.remove_blob(id)?;
        self.schedule_flush();
        Ok(())
    }

    /// Creates the given directory.
    async fn mkd<P: AsRef<Path> + Send + Debug>(&self, _user: &Option<U>, path: P) -> Result<()> {
        let path = path.as_ref();
        let mut kv = self.kv.write();
        let (mut tree, name) = kv.read_tree_name_from_path(path.as_ref())?;
        if tree.has(name) {
            denied!("mkd: {} exists", path.display());
        }
        let new_tree = kv.create_tree()?;
        let meta = Meta::new_folder();
        tree.items.insert(name.to_string(), (new_tree.index, meta));
        kv.write_tree(&tree)?;
        self.schedule_flush();
        Ok(())
    }

    /// Renames the given file to the given new filename.
    async fn rename<P: AsRef<Path> + Send + Debug>(
        &self,
        _user: &Option<U>,
        from: P,
        to: P,
    ) -> Result<()> {
        // TODO: Detect cycles.
        let to = to.as_ref();
        let mut kv = self.kv.write();
        let (mut from_tree, from_name) = kv.read_tree_name_from_path(from.as_ref())?;
        let (mut to_tree, to_name) = kv.read_tree_name_from_path(to)?;
        if to_tree.has(to_name) {
            denied!("rename: destination {} exists", to.display());
        }
        let from_item = from_tree.find(from_name)?;
        to_tree.items.insert(to_name.to_string(), from_item.clone());
        if to_tree.index == from_tree.index {
            to_tree.items.remove(from_name);
            kv.write_tree(&to_tree)?;
        } else {
            kv.write_tree(&to_tree)?;
            from_tree.items.remove(from_name);
            kv.write_tree(&from_tree)?;
        }
        self.schedule_flush();
        Ok(())
    }

    /// Deletes the given directory.
    async fn rmd<P: AsRef<Path> + Send + Debug>(&self, _user: &Option<U>, path: P) -> Result<()> {
        let path = path.as_ref();
        let mut kv = self.kv.write();
        let (mut tree, name) = kv.read_tree_name_from_path(path)?;
        let (index, meta) = tree.find(name)?;
        // Must be a dir.
        if !meta.is_dir() {
            denied!("rmd: {} is not a dir", path.display());
        }
        // Must be an empty dir.
        if !kv.read_tree_by_id(*index)?.items.is_empty() {
            denied!("rmd: {} is not empty", path.display());
        }
        tree.items.remove(name);
        kv.write_tree(&tree)?;
        self.schedule_flush();
        Ok(())
    }

    /// Changes the working directory to the given path.
    async fn cwd<P: AsRef<Path> + Send + Debug>(&self, _user: &Option<U>, path: P) -> Result<()> {
        let path = path.as_ref();
        let kv = self.kv.read();
        kv.read_tree_by_path(path)?;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Meta {
    len: u64,
    mode: u64,
    mtime: SystemTime,
}

impl Meta {
    fn new_folder() -> Self {
        Self {
            len: 0,
            mode: 0o040000,
            mtime: SystemTime::now(),
        }
    }

    fn new_file(len: u64) -> Self {
        Self {
            len,
            mode: 0o100644,
            mtime: SystemTime::now(),
        }
    }
}

impl Metadata for Meta {
    fn len(&self) -> u64 {
        self.len
    }

    fn is_dir(&self) -> bool {
        self.mode == 0o040000
    }

    fn is_file(&self) -> bool {
        self.mode == 0o100644
    }

    fn is_symlink(&self) -> bool {
        self.mode == 0o120000
    }

    fn modified(&self) -> storage::Result<SystemTime> {
        Ok(self.mtime)
    }

    fn gid(&self) -> u32 {
        0
    }

    fn uid(&self) -> u32 {
        0
    }
}

fn local_error() -> Error {
    ErrorKind::LocalError.into()
}

fn to_str(path: &OsStr) -> Result<&str> {
    match path.to_str() {
        Some(s) => Ok(s),
        None => Err(ErrorKind::FileNameNotAllowedError.into()),
    }
}

impl Drop for IntKvFtpFs {
    fn drop(&mut self) {
        log::debug!("Flushing on drop");
        maybe_flush(&self.kv);
    }
}
