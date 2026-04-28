//! Pluggable storage backend for shards that don't live on the local
//! filesystem (S3, GCS, HTTP, etc.).
//!
//! v0.3 design: a minimal `ObjectStore` trait (length + range_read) and
//! a helper that downloads the whole shard into a tempfile + opens it
//! with the existing `Reader`. This keeps the format primitives mmap-
//! backed (the assumption shard locality + content_hash makes
//! defensible) while letting users source shards from anywhere.
//!
//! True random-access S3 reading without a full download awaits a
//! refactor of `Reader` to take a generic `RangedReader` instead of an
//! `mmap::Mmap`. That's deliberately scoped out of this commit — it
//! changes every line of `reader.rs`. The trait below is the seam that
//! refactor will plug into.

use std::io::Write;
use std::path::Path;

use crate::error::{TsetError, TsetResult};

pub trait ObjectStore: Send + Sync {
    /// Total length of the object in bytes.
    fn len(&self) -> TsetResult<u64>;

    /// `len() == 0`. Default impl is provided so callers can write
    /// idiomatic empty-checks without each backend re-implementing it.
    fn is_empty(&self) -> TsetResult<bool> {
        Ok(self.len()? == 0)
    }

    /// Range read: `start..end`. Implementations should return exactly
    /// `end - start` bytes; a short read is a `TsetError::BadManifest`.
    fn range_read(&self, start: u64, end: u64) -> TsetResult<Vec<u8>>;

    /// Convenience: read the entire object.
    fn read_all(&self) -> TsetResult<Vec<u8>> {
        let n = self.len()?;
        self.range_read(0, n)
    }
}

/// Backend backed by a local file. Same as opening the file directly
/// but useful for trait-driven code that wants a single API.
pub struct LocalFile {
    path: std::path::PathBuf,
}

impl LocalFile {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }
}

impl ObjectStore for LocalFile {
    fn len(&self) -> TsetResult<u64> {
        Ok(std::fs::metadata(&self.path)?.len())
    }

    fn range_read(&self, start: u64, end: u64) -> TsetResult<Vec<u8>> {
        use std::io::{Read, Seek, SeekFrom};
        if end < start {
            return Err(TsetError::BadManifest("range_read: end < start"));
        }
        let n = (end - start) as usize;
        let mut f = std::fs::File::open(&self.path)?;
        f.seek(SeekFrom::Start(start))?;
        let mut out = vec![0u8; n];
        f.read_exact(&mut out)?;
        Ok(out)
    }
}

/// In-memory backend. Useful for tests + for downloading-then-opening
/// shards from networked storage.
pub struct InMemory {
    bytes: Vec<u8>,
}

impl InMemory {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }
}

impl ObjectStore for InMemory {
    fn len(&self) -> TsetResult<u64> {
        Ok(self.bytes.len() as u64)
    }

    fn range_read(&self, start: u64, end: u64) -> TsetResult<Vec<u8>> {
        let s = start as usize;
        let e = end as usize;
        if e < s || e > self.bytes.len() {
            return Err(TsetError::BadManifest("range_read out of bounds"));
        }
        Ok(self.bytes[s..e].to_vec())
    }
}

/// Download the entire object into a tempfile and return its path. Use
/// this with `Reader::open(path)` when the shard isn't local.
///
/// The cost: full download up front. The benefit: every existing
/// `Reader` invariant (mmap, content hashes, fast seeking) just works.
/// Future work — random-access streaming over S3 — will let the Reader
/// avoid the full download for cases where only a fraction of the shard
/// is read.
pub fn download_to_tempfile(store: &dyn ObjectStore) -> TsetResult<tempfile::NamedTempFile> {
    let mut tmp = tempfile::NamedTempFile::new()?;
    let bytes = store.read_all()?;
    tmp.write_all(&bytes)?;
    tmp.flush()?;
    Ok(tmp)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_file_round_trip() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.write_all(b"hello world").unwrap();
        tmp.flush().unwrap();
        let store = LocalFile::new(tmp.path());
        assert_eq!(store.len().unwrap(), 11);
        assert_eq!(store.range_read(0, 5).unwrap(), b"hello");
        assert_eq!(store.range_read(6, 11).unwrap(), b"world");
    }

    #[test]
    fn in_memory_bounds_checks() {
        let store = InMemory::new(b"abcdef".to_vec());
        assert_eq!(store.len().unwrap(), 6);
        assert_eq!(store.range_read(1, 4).unwrap(), b"bcd");
        // Out-of-bounds returns Err, never panics
        assert!(store.range_read(0, 99).is_err());
        assert!(store.range_read(5, 2).is_err());
    }

    #[test]
    fn download_to_tempfile_preserves_bytes() {
        let store = InMemory::new(b"shard payload".to_vec());
        let tmp = download_to_tempfile(&store).unwrap();
        let read = std::fs::read(tmp.path()).unwrap();
        assert_eq!(read, b"shard payload");
    }
}
