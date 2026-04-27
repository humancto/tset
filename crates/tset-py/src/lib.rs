//! PyO3 bindings exposing `tset-core` to Python as the `tset_rs` module.
//!
//! Imported as: `from tset import _rs` (the Python shim re-exports it).

use std::path::PathBuf;

use pyo3::exceptions::{PyKeyError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};

use tset_core::reader::Reader as CoreReader;
use tset_core::tokenizers::{ByteLevelTokenizer, Tokenizer, WhitespaceTokenizer};
use tset_core::writer::Writer as CoreWriter;
use tset_core::TsetError;

fn map_err(e: TsetError) -> PyErr {
    match e {
        TsetError::DocumentNotFound(s) => PyKeyError::new_err(s),
        TsetError::ViewNotFound(s) => PyKeyError::new_err(s),
        other => PyValueError::new_err(other.to_string()),
    }
}

#[pyclass(name = "Reader", module = "tset._rs")]
pub struct PyReader {
    inner: CoreReader,
}

#[pymethods]
impl PyReader {
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let inner = CoreReader::open(PathBuf::from(path)).map_err(map_err)?;
        Ok(Self { inner })
    }

    #[getter]
    fn version_major(&self) -> u8 {
        self.inner.header.version_major
    }

    #[getter]
    fn version_minor(&self) -> u8 {
        self.inner.header.version_minor
    }

    #[getter]
    fn shard_id(&self) -> Option<String> {
        self.inner.shard_id().map(|s| s.to_string())
    }

    #[getter]
    fn shard_merkle_root<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new_bound(py, &self.inner.header.shard_merkle_root)
    }

    fn tokenizer_ids(&self) -> PyResult<Vec<String>> {
        self.inner.tokenizer_ids().map_err(map_err)
    }

    fn view_total_tokens(&self, tokenizer_id: &str) -> PyResult<u64> {
        self.inner.view_total_tokens(tokenizer_id).map_err(map_err)
    }

    fn has_document(&self, doc_hash: &[u8]) -> PyResult<bool> {
        if doc_hash.len() != 32 {
            return Err(PyValueError::new_err("doc_hash must be 32 bytes"));
        }
        let mut h = [0u8; 32];
        h.copy_from_slice(doc_hash);
        Ok(self.inner.has_document(&h))
    }

    fn get_document<'py>(&self, py: Python<'py>, doc_hash: &[u8]) -> PyResult<Bound<'py, PyBytes>> {
        if doc_hash.len() != 32 {
            return Err(PyValueError::new_err("doc_hash must be 32 bytes"));
        }
        let mut h = [0u8; 32];
        h.copy_from_slice(doc_hash);
        let bytes = self.inner.get_document(&h).map_err(map_err)?;
        Ok(PyBytes::new_bound(py, &bytes))
    }

    /// Returns a list of `(tokens_bytes_le_u32, doc_hash_bytes)` tuples.
    /// Tokens are returned as raw little-endian u32 bytes — Python wraps
    /// them in `numpy.frombuffer(tokens, dtype=np.uint32)`.
    fn stream_tokens<'py>(
        &self,
        py: Python<'py>,
        tokenizer_id: &str,
    ) -> PyResult<Bound<'py, PyList>> {
        let view = self.inner.open_view(tokenizer_id).map_err(map_err)?;
        let pieces = view.iter_per_doc().map_err(map_err)?;
        let list = PyList::empty_bound(py);
        for (tokens, doc_hash) in pieces {
            let mut buf = Vec::with_capacity(tokens.len() * 4);
            for t in tokens {
                buf.extend_from_slice(&t.to_le_bytes());
            }
            let tup = (
                PyBytes::new_bound(py, &buf),
                PyBytes::new_bound(py, &doc_hash),
            );
            list.append(tup)?;
        }
        Ok(list)
    }

    fn doc_hashes_hex(&self) -> Vec<String> {
        self.inner.doc_hashes().map(|h| hex::encode(h)).collect()
    }
}

#[pyclass(name = "Writer", module = "tset._rs")]
pub struct PyWriter {
    // Option so we can take it on close()
    inner: Option<CoreWriter>,
}

#[pymethods]
impl PyWriter {
    #[new]
    #[pyo3(signature = (path, shard_id=None))]
    fn new(path: &str, shard_id: Option<String>) -> Self {
        Self {
            inner: Some(CoreWriter::create(path, shard_id)),
        }
    }

    fn add_document<'py>(
        &mut self,
        py: Python<'py>,
        content: &[u8],
    ) -> PyResult<Bound<'py, PyBytes>> {
        let w = self
            .inner
            .as_mut()
            .ok_or_else(|| PyValueError::new_err("writer already closed"))?;
        let h = w.add_document(content).map_err(map_err)?;
        Ok(PyBytes::new_bound(py, &h))
    }

    /// `tokenizer_spec` is a (id, vocab_size) tuple. v0.2 supports
    /// "byte-level-v1" and "whitespace-hashed-v1".
    fn add_tokenizer_view(&mut self, tokenizer_id: &str, vocab_size: u32) -> PyResult<()> {
        let w = self
            .inner
            .as_mut()
            .ok_or_else(|| PyValueError::new_err("writer already closed"))?;
        let tok: Box<dyn Tokenizer> = match tokenizer_id {
            ByteLevelTokenizer::ID => Box::new(ByteLevelTokenizer),
            WhitespaceTokenizer::ID => Box::new(WhitespaceTokenizer::new(vocab_size).map_err(map_err)?),
            other => {
                return Err(PyValueError::new_err(format!(
                    "unknown tokenizer_id: {other}"
                )))
            }
        };
        w.add_tokenizer_view(tok).map_err(map_err)
    }

    fn close(&mut self) -> PyResult<()> {
        let w = self
            .inner
            .take()
            .ok_or_else(|| PyValueError::new_err("writer already closed"))?;
        w.close().map_err(map_err)
    }
}

#[pymodule]
fn tset_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyReader>()?;
    m.add_class::<PyWriter>()?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
