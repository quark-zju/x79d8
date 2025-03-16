use super::super::{Bytes, IntKv};
use crate::util::bincode_deserialize;
use crate::util::bincode_serialize_pad;
use crate::util::bincode_size;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::io;

/// Normalize requests so only fixed-sized sized pages are
/// written.
///
/// There are 2 kinds of fixed sized pages: meta, and data.
///
/// A meta page consists of:
/// - logical index -> physical data page index mapping
/// - physical data page -> logical
///
/// A data page consists of:
/// - logical index -> (data chunk, Option<next page index>)
///
/// To reconstruct data, first lookup from the meta page,
/// then follow the linked list in data pages and concat
/// all data chunks.
///
/// Modifications are buffered. Meta pages are eagerly
/// loaded into memory on construction.
#[derive(Debug)]
pub struct PageIntKv {
    // Desired page size.
    page_size: u64,

    // Physical page indexes.
    // Together with data_page_sizes for finding free pages.
    meta_pages: Vec<u64>,

    // physical data page index -> physical data page size.
    // Also serves as a way to get all data pages.
    data_page_sizes: BTreeMap<u64, u64>,

    // Data pages that are changed, not flushed.
    // Empty pages will be deleted on flush.
    dirty_data_pages: BTreeMap<u64, DataPage>,

    // logical -> first physical page index.
    map_index: BTreeMap<u64, u64>,

    // Underlying kv.
    kv: Box<dyn IntKv>,
}

#[derive(Serialize, Deserialize, Default)]
struct MetaPage {
    // physical page index for the next meta page (0: end)
    next_page_index: u64,

    // logical -> first physical page index
    map_index: BTreeMap<u64, u64>,

    // physical data page index -> physical data page size
    data_size_indexes: BTreeMap<u64, u64>,

    #[serde(skip)]
    page_index: u64,
}

#[derive(Serialize, Deserialize, Default, Clone, Debug)]
struct DataPage {
    // logical index, chunk of data
    chunks: BTreeMap<u64, Chunk>,

    #[serde(skip)]
    page_index: u64,
}
#[derive(Serialize, Deserialize, Default, Clone)]
struct Chunk {
    // Physical page index for the next data page containing the next
    // part of the data belonging to a single logical index (0: end).
    next_page_index: u64,
    data: Bytes,
}

impl fmt::Debug for Chunk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Chunk")
            .field("next_page", &self.next_page_index)
            .field("data", &self.data.len())
            .finish()
    }
}

impl PageIntKv {
    /// Create a new `PageIntKv` with specified page size.
    pub fn new(page_size: u64, kv: Box<dyn IntKv>) -> io::Result<Self> {
        let (meta_pages, map_index, data_page_sizes) = load_metadata(kv.as_ref())?;
        let result = Self {
            page_size,
            kv,
            meta_pages,
            map_index,
            data_page_sizes,
            dirty_data_pages: Default::default(),
        };
        #[cfg(debug_assertions)]
        result.verify()?;
        Ok(result)
    }

    /// Check integrity: page sizes are correct, all pages are referred,
    /// no page exceeds the limited size.
    #[cfg(debug_assertions)]
    pub fn verify(&self) -> io::Result<()> {
        fn error(message: impl ToString) -> io::Result<()> {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                message.to_string(),
            ))
        }

        // Check page sizes.
        for (&index, &size) in &self.data_page_sizes {
            let data = self.read_data_page(index as _)?;
            let actual_size = bincode_size(&data);
            if actual_size != size {
                return error(format!(
                    "data page {} has mismatched size: actual {} vs recorded {}",
                    index, actual_size, size
                ));
            }
        }

        if !self.has(0)? {
            return Ok(());
        }

        // Check referred data pages.
        let mut data_referred: BTreeSet<u64> = Default::default();
        let mut meta_index = 0;
        loop {
            let meta = self.read_meta_page(meta_index)?;
            // Check logical -> data mapping.
            for (&logical_index, &data_index) in &meta.map_index {
                let page = self.read_data_page(data_index as _)?;
                if !page.chunks.contains_key(&logical_index) {
                    return error(format!(
                        "data page {} does not contain expected entry {}",
                        data_index, logical_index,
                    ));
                }
            }
            // Collect referred data pages from this meta page.
            for &data_index in meta.map_index.values() {
                let mut to_visit = vec![data_index];
                while let Some(data_index) = to_visit.pop() {
                    if data_referred.insert(data_index) {
                        let data = self.read_data_page(data_index as _)?;
                        let indexes = data
                            .chunks
                            .values()
                            .map(|c| c.next_page_index)
                            .filter(|&i| i != 0);
                        to_visit.extend(indexes);
                    }
                }
            }
            meta_index = meta.next_page_index as _;
            if meta_index == 0 {
                break;
            }
        }
        let data_recorded = self.data_page_sizes.keys().cloned().collect();
        if data_referred != data_recorded {
            return error(format!(
                "data pages mismatch: actual{:?} recorded {:?}",
                data_referred, &data_recorded,
            ));
        }

        // Check page sizes
        for &i in self.meta_pages.iter().chain(data_referred.iter()) {
            let data = self.kv.read(i as _)?;
            let len = data.len();
            if len != self.page_size as usize {
                return error(format!(
                    "page {} size mismatch: actual {:?} expected {:?}",
                    i, len, self.page_size,
                ));
            }
        }

        Ok(())
    }

    #[cfg(debug_assertions)]
    fn read_meta_page(&self, index: usize) -> io::Result<MetaPage> {
        let data = self.kv.read(index)?;
        bincode_deserialize(&data)
    }

    fn read_data_page(&self, index: usize) -> io::Result<DataPage> {
        match self.dirty_data_pages.get(&(index as _)) {
            Some(page) => Ok(page.clone()),
            None => {
                let data = self.kv.read(index)?;
                let mut page: DataPage = bincode_deserialize(&data)?;
                page.page_index = index as _;
                Ok(page)
            }
        }
    }

    fn create_data_page(&mut self) -> io::Result<DataPage> {
        let page_index = self.find_free_page_index()?;
        let page = DataPage {
            page_index,
            ..Default::default()
        };
        self.write_data_page(page.clone());
        Ok(page)
    }

    /// Update chunk in a data page.
    ///
    /// Attempt to write part (or rewrite) of the data associated with
    /// the given logical index. With the maximum size limit.
    /// If data is None, remove the data entry.
    ///
    /// Return the next page and the remaining data for writing.
    fn update_chunk(
        &mut self,
        mut page: DataPage,
        logical_index: u64,
        data: Option<Bytes>,
    ) -> io::Result<Option<(DataPage, Option<Bytes>)>> {
        log::debug!(
            "UpdateChunk {} (len {:?}) to DataPage {}",
            logical_index,
            data.as_ref().map(|d| d.len()),
            page.page_index,
        );

        // Remove old data.
        let orig_chunk = page.chunks.remove(&logical_index);

        // Find the next page by following the existing data.
        let mut next_page = {
            let index = orig_chunk.map(|c| c.next_page_index).unwrap_or(0);
            debug_assert_ne!(index, page.page_index);
            if index == 0 {
                None
            } else {
                Some(self.read_data_page(index as _)?)
            }
        };
        let mut next_data = None;

        // Rewrite chunk and find the next page.
        if let Some(data) = data {
            let max_page_size = self.page_size;
            let overhead = 8 * 3;
            let current_page_size = bincode_size(&page) + overhead;
            if current_page_size > max_page_size {
                // Cannot satisfy the max_page_size limit.
                return Err(io::ErrorKind::WriteZero.into());
            }
            let size = ((max_page_size - current_page_size) as usize).min(data.len());
            let part = data.slice(0..size);
            if part.len() < data.len() {
                // Both next_data and next_page are needed.
                next_data = Some(data.slice(part.len()..));
                // Allocate next_page on demand.
                if next_page.is_none() {
                    let new_page = self.create_data_page()?;
                    debug_assert_ne!(new_page.page_index, page.page_index);
                    next_page = Some(new_page);
                }
            }
            let chunk = Chunk {
                data: part,
                next_page_index: match &next_page {
                    Some(p) => match &next_data {
                        None => 0,
                        Some(_) => p.page_index,
                    },
                    None => 0,
                },
            };
            page.chunks.insert(logical_index, chunk);
            if next_data.is_some() {
                // Should fill up the current page if there are remaining data.
                debug_assert_eq!(bincode_size(&page), max_page_size);
            }
        }
        self.write_data_page(page);

        if next_data.is_some() {
            // Next page must be allocated if there are remaining data.
            debug_assert!(next_page.is_some())
        }

        Ok(next_page.map(|p| (p, next_data)))
    }

    /// Update logical data. Rewrite the linked data pages.
    /// If data is None, remove the data from all linked lists.
    fn update_logical_data(&mut self, index: usize, mut data: Option<Bytes>) -> io::Result<()> {
        let mut data_page = match self.map_index.get(&(index as _)) {
            // Find a suitable page from existing pages.
            None => match &data {
                // Cannot remove if the data does not exist.
                None => return Err(not_found()),
                Some(data) => {
                    let page = self.find_first_page_for_size(data.len() as _)?;
                    self.map_index.insert(index as _, page.page_index);
                    page
                }
            },
            // Using the existing data page via mapping.
            Some(&id) => self.read_data_page(id as _)?,
        };
        if data.is_none() {
            self.map_index.remove(&(index as _));
        }
        while let Some((next_page, next_data)) = self.update_chunk(data_page, index as _, data)? {
            data_page = next_page;
            data = next_data;
        }
        Ok(())
    }

    /// Find a page index that can store the given sized data as the first
    /// page.
    fn find_first_page_for_size(&mut self, size: u64) -> io::Result<DataPage> {
        let overhead = 8 * 3;
        let needed_size = size + overhead;
        if needed_size > self.page_size {
            // Pick a page with maximum free space.
            if let Some((&page_index, &page_size)) = self
                .data_page_sizes
                .iter()
                .min_by_key(|(_, page_size)| *page_size)
            {
                if page_size + overhead < self.page_size {
                    return self.read_data_page(page_index as _);
                }
            }
        }
        // PERF: This can probably be improved.
        for (&page_index, &page_size) in &self.data_page_sizes {
            if page_size + needed_size <= self.page_size {
                return self.read_data_page(page_index as _);
            }
        }
        // Allocate a new page.
        self.create_data_page()
    }

    /// Find an unused page index.
    fn find_free_page_index(&self) -> io::Result<u64> {
        Ok(self
            .find_free_index_in_batch(1)?
            .iter()
            .next()
            .cloned()
            .unwrap())
    }

    /// Find free pages.
    fn find_free_index_in_batch(&self, n: usize) -> io::Result<BTreeSet<u64>> {
        // PERF: This can be improved.
        let mut result: BTreeSet<u64> = Default::default();
        while result.len() < n {
            let i: u32 = rand::random();
            if !self.has(i as _)? {
                result.insert(i as _);
            }
        }
        Ok(result)
    }

    /// Mark a page for writing on flush.
    fn write_data_page(&mut self, page: DataPage) {
        let index = page.page_index;
        // Keep empty pages in data_page_sizes cache. They can be mutable.
        // They will be deleted on flush.
        let page_size = bincode_size(&page);
        self.data_page_sizes.insert(index, page_size);
        self.dirty_data_pages.insert(index, page);
    }

    /// Write a meta page to the underlying IntKv.
    fn write_meta_page(&mut self, page: &MetaPage) -> io::Result<()> {
        let index = page.page_index;
        let bytes = bincode_serialize_pad(page, self.page_size);
        self.kv.write(index as _, bytes.into())?;
        Ok(())
    }
}

impl IntKv for PageIntKv {
    fn read(&self, index: usize) -> io::Result<Bytes> {
        let mut mapped_index = match self.map_index.get(&(index as _)) {
            None => return Err(not_found()),
            Some(&mapped_index) => mapped_index,
        };
        let mut result = Vec::new();
        while mapped_index != 0 {
            let page: DataPage = self.read_data_page(mapped_index as _)?;
            match page.chunks.get(&(index as _)) {
                Some(chunk) => {
                    mapped_index = chunk.next_page_index as _;
                    if mapped_index == 0 && result.is_empty() {
                        // Fast path: single chunk data.
                        return Ok(chunk.data.clone());
                    } else {
                        // Multiple chunks. Concat them.
                        result.extend_from_slice(&chunk.data)
                    }
                }
                None => return Err(not_found()),
            }
        }
        Ok(result.into())
    }

    fn write(&mut self, index: usize, data: Bytes) -> io::Result<()> {
        self.update_logical_data(index, Some(data))
    }

    fn remove(&mut self, index: usize) -> io::Result<()> {
        self.update_logical_data(index, None)
    }

    fn has(&self, index: usize) -> io::Result<bool> {
        Ok(self.map_index.contains_key(&(index as _)))
    }

    fn flush(&mut self) -> io::Result<()> {
        // Nothing changed?
        if self.dirty_data_pages.is_empty() {
            return Ok(());
        }

        // Write out data pages.
        for (&index, page) in &self.dirty_data_pages {
            log::debug!(
                "Flushing DataPage {} with chunks {:?}",
                index,
                page.chunks.keys().collect::<Vec<_>>()
            );
            if page.chunks.is_empty() {
                // Delete empty pages.
                debug_assert!(!self.map_index.values().any(|&p| p == index));
                if self.kv.has(index as _)? {
                    self.kv.remove(index as _)?;
                }
                self.data_page_sizes.remove(&index);
            } else {
                let bytes = bincode_serialize_pad(page, self.page_size);
                self.kv.write(index as _, bytes.into())?;
            }
        }
        self.dirty_data_pages.clear();

        // Prepare meta pages.
        let mut to_insert = self.map_index.len() + self.data_page_sizes.len();
        let mut new_meta_pages: Vec<MetaPage> = vec![MetaPage::default()];
        let mut map_iter = self.map_index.iter();
        let mut data_size_iter = self.data_page_sizes.iter();
        while to_insert > 0 {
            let page = new_meta_pages.last_mut().unwrap();
            let size = bincode_size(page);

            // 16: bincode size for (key, value) pair.
            let n = ((self.page_size - size) as usize) / 16;
            for _ in 0..n {
                if let Some((&k, &v)) = map_iter.next() {
                    page.map_index.insert(k, v);
                    to_insert -= 1;
                }
            }
            let orig_size = size;
            let size = bincode_size(page);
            assert!(
                size <= self.page_size,
                "{} <= {}, n={}, orig={}",
                size,
                self.page_size,
                n,
                orig_size
            );

            let m = ((self.page_size - size) as usize) / 16;
            for _ in 0..m {
                if let Some((&k, &v)) = data_size_iter.next() {
                    page.data_size_indexes.insert(k, v);
                    to_insert -= 1;
                }
            }
            let orig_size = size;
            let size = bincode_size(page);
            assert!(
                size <= self.page_size,
                "{} <= {}, m={}, orig={}",
                size,
                self.page_size,
                m,
                orig_size
            );

            if n + m == 0 && to_insert > 0 {
                // Need a new page.
                new_meta_pages.push(MetaPage::default());
            }
        }

        // Fix meta page indexes.
        let mut next_free_index = {
            let free_indexes = self.find_free_index_in_batch(new_meta_pages.len())?;
            let mut iter = free_indexes.into_iter();
            move || iter.next().unwrap()
        };
        for (i, new_meta_page) in new_meta_pages.iter_mut().enumerate().skip(1) {
            new_meta_page.page_index = match self.meta_pages.get(i) {
                None => {
                    let id = next_free_index();
                    if self.has(id as _)? {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!(
                                "page {} should not be taken (bug in find_free_index_in_batch)",
                                id
                            ),
                        ));
                    }
                    id
                }
                Some(&id) => id,
            };
        }

        // Fix linked list.
        for i in 0..(new_meta_pages.len() - 1) {
            new_meta_pages[i].next_page_index = new_meta_pages[i + 1].page_index;
        }

        // Write out new meta pages.
        for page in &new_meta_pages {
            self.write_meta_page(page)?;
        }

        // Remove unused pages.
        if let Some(indexes) = self.meta_pages.get(new_meta_pages.len()..) {
            for &i in indexes {
                self.kv.remove(i as _)?;
            }
        }

        self.kv.flush()?;

        // Update internal state.
        self.meta_pages = new_meta_pages.into_iter().map(|p| p.page_index).collect();
        self.dirty_data_pages.clear();

        #[cfg(debug_assertions)]
        self.verify()?;
        Ok(())
    }
}

#[allow(clippy::type_complexity)]
fn load_metadata(kv: &dyn IntKv) -> io::Result<(Vec<u64>, BTreeMap<u64, u64>, BTreeMap<u64, u64>)> {
    let mut meta_pages: Vec<u64> = Default::default();
    let mut map_index: BTreeMap<u64, u64> = Default::default();
    let mut data_page_sizes: BTreeMap<u64, u64> = Default::default();
    // Page 0 is reserved as an index page.
    if kv.has(0)? {
        let mut index = 0;
        loop {
            if meta_pages.contains(&(index as _)) {
                // Meta pages must not form a cycle.
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("meta pages form a cycle ({})", index),
                ));
            }
            meta_pages.push(index as _);
            let data = kv.read(index)?;
            let mut page: MetaPage = bincode_deserialize(&data)?;
            // Merge the index map into the global index map.
            map_index.append(&mut page.map_index);
            // Merge the data page size map.
            data_page_sizes.append(&mut page.data_size_indexes);
            index = page.next_page_index as usize;
            if index == 0 {
                // No more meta page to load.
                break;
            }
        }
    }
    Ok((meta_pages, map_index, data_page_sizes))
}

fn not_found() -> io::Error {
    io::ErrorKind::NotFound.into()
}

#[cfg(test)]
fn test_page_kv_size(size: u64, n: usize) {
    let kv = super::super::test_int_kv(
        |kv| {
            kv.unwrap_or_else(|| {
                let kv = super::super::backend::MemIntKv::new();
                PageIntKv::new(size, Box::new(kv)).unwrap()
            })
        },
        n,
    );
    kv.verify().unwrap();

    // Reconstruct from the underlying kv.
    let mut orig_kv = Some(kv.kv);
    let kv = super::super::test_int_kv(
        |kv| {
            kv.unwrap_or_else(|| {
                let kv = orig_kv.take().unwrap();
                PageIntKv::new(size, kv).unwrap()
            })
        },
        n,
    );
    kv.verify().unwrap();
}

#[test]
fn test_page_kv_64() {
    test_page_kv_size(64, 10);
}

#[test]
fn test_page_kv_1024() {
    test_page_kv_size(1024, 100);
}

#[test]
fn test_page_kv_16384() {
    test_page_kv_size(16384, 100);
}
