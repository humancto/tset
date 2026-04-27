//! PyO3 bindings exposing `tset-core` to Python as the `tset_rs` module.
//!
//! Imported as: `from tset import _rs` (the Python shim re-exports it).

use std::path::PathBuf;

use pyo3::exceptions::{PyKeyError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};

use tset_core::dataset::{Dataset as CoreDataset, DatasetWriter as CoreDatasetWriter};
use tset_core::reader::Reader as CoreReader;
use tset_core::signing::{AuditSigner, verify_signature};
use tset_core::tokenizers::{ByteLevelTokenizer, Tokenizer, WhitespaceTokenizer};
use tset_core::writer::{append_tokenizer_view as core_append_tokenizer_view, Writer as CoreWriter};
use tset_core::TsetError;

fn map_err(e: TsetError) -> PyErr {
    match e {
        TsetError::DocumentNotFound(s) => PyKeyError::new_err(s),
        TsetError::ViewNotFound(s) => PyKeyError::new_err(s),
        other => PyValueError::new_err(other.to_string()),
    }
}

/// Convert a u32 slice to little-endian bytes for the wire/Python boundary.
///
/// Little-endian is the only supported endianness in the spec, so on LE
/// platforms (essentially all modern x86 + ARM) we can do a zero-copy
/// reinterpret of the buffer. On big-endian platforms we fall back to a
/// per-element byteswap. The fallback path is what makes this fn safe in
/// the general case; the LE path is the hot one.
#[inline]
fn tokens_to_le_bytes(tokens: &[u32]) -> std::borrow::Cow<'_, [u8]> {
    #[cfg(target_endian = "little")]
    {
        // SAFETY: u32 has alignment 4 ≥ u8 alignment 1; lifetimes tied to
        // the input slice. The reinterpretation is well-defined on LE
        // because the spec requires little-endian on disk and the in-memory
        // u32 representation matches that byte-for-byte.
        let bytes: &[u8] = unsafe {
            std::slice::from_raw_parts(
                tokens.as_ptr() as *const u8,
                std::mem::size_of_val(tokens),
            )
        };
        std::borrow::Cow::Borrowed(bytes)
    }
    #[cfg(not(target_endian = "little"))]
    {
        let mut buf = Vec::with_capacity(tokens.len() * 4);
        for t in tokens {
            buf.extend_from_slice(&t.to_le_bytes());
        }
        std::borrow::Cow::Owned(buf)
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
    /// Tokens are little-endian u32 bytes — Python wraps them in
    /// `numpy.frombuffer(tokens, dtype=np.uint32)`.
    fn stream_tokens<'py>(
        &self,
        py: Python<'py>,
        tokenizer_id: &str,
    ) -> PyResult<Bound<'py, PyList>> {
        let view = self.inner.open_view(tokenizer_id).map_err(map_err)?;
        let pieces = view.iter_per_doc().map_err(map_err)?;
        let list = PyList::empty_bound(py);
        for (tokens, doc_hash) in pieces {
            let tokens_bytes = tokens_to_le_bytes(&tokens);
            let tup = (
                PyBytes::new_bound(py, &tokens_bytes),
                PyBytes::new_bound(py, &doc_hash),
            );
            list.append(tup)?;
        }
        Ok(list)
    }

    fn doc_hashes_hex(&self) -> Vec<String> {
        self.inner.doc_hashes().map(|h| hex::encode(h)).collect()
    }

    fn smt_root<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new_bound(py, &self.inner.smt_root())
    }

    /// Inclusion proof for a doc that is in this shard. Returns
    /// `(doc_hash_hex, [sibling_hex; 256])` — siblings are listed
    /// top-down (root → leaf) per spec.
    fn prove_inclusion(&self, doc_hash: &[u8]) -> PyResult<(String, Vec<String>)> {
        let h = parse_doc_hash(doc_hash)?;
        if !self.inner.has_document(&h) {
            return Err(PyKeyError::new_err(format!(
                "document {} not in this shard",
                hex::encode(h)
            )));
        }
        let smt = build_reader_smt(&self.inner);
        match smt.prove(&h) {
            tset_core::smt::Proof::Inclusion(p) => Ok((
                hex::encode(p.key),
                p.siblings.iter().map(hex::encode).collect(),
            )),
            tset_core::smt::Proof::NonInclusion(_) => Err(PyValueError::new_err(
                "shard claims doc but SMT says absent",
            )),
        }
    }

    fn prove_non_inclusion(&self, doc_hash: &[u8]) -> PyResult<(String, Vec<String>)> {
        let h = parse_doc_hash(doc_hash)?;
        if self.inner.has_document(&h) {
            return Err(PyValueError::new_err(format!(
                "document {} IS in this shard; use prove_inclusion",
                hex::encode(h)
            )));
        }
        let smt = build_reader_smt(&self.inner);
        match smt.prove(&h) {
            tset_core::smt::Proof::NonInclusion(p) => Ok((
                hex::encode(p.key),
                p.siblings.iter().map(hex::encode).collect(),
            )),
            tset_core::smt::Proof::Inclusion(_) => Err(PyValueError::new_err(
                "SMT says present but reader says absent",
            )),
        }
    }
}

fn parse_doc_hash(b: &[u8]) -> PyResult<[u8; 32]> {
    if b.len() != 32 {
        return Err(PyValueError::new_err("doc_hash must be 32 bytes"));
    }
    let mut h = [0u8; 32];
    h.copy_from_slice(b);
    Ok(h)
}

fn build_reader_smt(r: &CoreReader) -> tset_core::smt::SparseMerkleTree {
    let mut tree = tset_core::smt::SparseMerkleTree::new();
    if let Some(arr) = r
        .manifest()
        .raw()
        .get("smt_present_keys")
        .and_then(serde_json::Value::as_array)
    {
        for v in arr {
            if let Some(s) = v.as_str() {
                if let Ok(bytes) = hex::decode(s) {
                    if bytes.len() == 32 {
                        let mut h = [0u8; 32];
                        h.copy_from_slice(&bytes);
                        tree.insert(h);
                    }
                }
            }
        }
    }
    tree
}

/// Verify a SMT proof against a root. Top-level helper so callers can
/// verify a serialized proof without holding a Reader.
#[pyfunction]
fn verify_inclusion_proof(
    doc_hash: &[u8],
    siblings_hex: Vec<String>,
    expected_root: &[u8],
) -> PyResult<bool> {
    let h = parse_doc_hash(doc_hash)?;
    if expected_root.len() != 32 {
        return Err(PyValueError::new_err("expected_root must be 32 bytes"));
    }
    let mut root = [0u8; 32];
    root.copy_from_slice(expected_root);
    let mut sibs = Vec::with_capacity(siblings_hex.len());
    for s in &siblings_hex {
        let bytes = hex::decode(s).map_err(|e| PyValueError::new_err(e.to_string()))?;
        if bytes.len() != 32 {
            return Err(PyValueError::new_err("sibling must decode to 32 bytes"));
        }
        let mut sh = [0u8; 32];
        sh.copy_from_slice(&bytes);
        sibs.push(sh);
    }
    Ok(tset_core::smt::InclusionProof { key: h, siblings: sibs }.verify(&root))
}

#[pyfunction]
fn verify_non_inclusion_proof(
    doc_hash: &[u8],
    siblings_hex: Vec<String>,
    expected_root: &[u8],
) -> PyResult<bool> {
    let h = parse_doc_hash(doc_hash)?;
    if expected_root.len() != 32 {
        return Err(PyValueError::new_err("expected_root must be 32 bytes"));
    }
    let mut root = [0u8; 32];
    root.copy_from_slice(expected_root);
    let mut sibs = Vec::with_capacity(siblings_hex.len());
    for s in &siblings_hex {
        let bytes = hex::decode(s).map_err(|e| PyValueError::new_err(e.to_string()))?;
        if bytes.len() != 32 {
            return Err(PyValueError::new_err("sibling must decode to 32 bytes"));
        }
        let mut sh = [0u8; 32];
        sh.copy_from_slice(&bytes);
        sibs.push(sh);
    }
    Ok(tset_core::smt::NonInclusionProof { key: h, siblings: sibs }.verify(&root))
}

#[pyclass(name = "Writer", module = "tset._rs")]
pub struct PyWriter {
    // Option so we can take it on close()
    inner: Option<CoreWriter>,
}

#[pymethods]
impl PyWriter {
    #[new]
    #[pyo3(signature = (path, shard_id=None, signing_key=None))]
    fn new(
        path: &str,
        shard_id: Option<String>,
        signing_key: Option<&[u8]>,
    ) -> PyResult<Self> {
        let signer = match signing_key {
            None => None,
            Some(b) => Some(
                AuditSigner::from_secret_bytes(b)
                    .map_err(|e| PyValueError::new_err(e.to_string()))?,
            ),
        };
        Ok(Self {
            inner: Some(CoreWriter::create_with_options(path, shard_id, signer)),
        })
    }

    #[pyo3(signature = (content, metadata=None))]
    fn add_document<'py>(
        &mut self,
        py: Python<'py>,
        content: &[u8],
        metadata: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let w = self
            .inner
            .as_mut()
            .ok_or_else(|| PyValueError::new_err("writer already closed"))?;
        let h = match metadata {
            None => w.add_document(content).map_err(map_err)?,
            Some(obj) => {
                let json_str: String = py
                    .import_bound("json")?
                    .call_method1("dumps", (obj,))?
                    .extract()?;
                let value: serde_json::Value = serde_json::from_str(&json_str)
                    .map_err(|e| PyValueError::new_err(e.to_string()))?;
                let map = value
                    .as_object()
                    .ok_or_else(|| PyValueError::new_err("metadata must be a dict"))?;
                w.add_document_with_metadata(content, Some(map))
                    .map_err(map_err)?
            }
        };
        Ok(PyBytes::new_bound(py, &h))
    }

    fn add_subset(
        &mut self,
        name: &str,
        predicate: &str,
        default_weight: f64,
    ) -> PyResult<()> {
        let w = self
            .inner
            .as_mut()
            .ok_or_else(|| PyValueError::new_err("writer already closed"))?;
        w.add_subset(name, predicate, default_weight);
        Ok(())
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

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Context-manager exit: close on success, drop pending state on
    /// exception (don't write a half-finished file).
    fn __exit__(
        &mut self,
        exc_type: &Bound<'_, PyAny>,
        _exc_value: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        if exc_type.is_none() {
            if self.inner.is_some() {
                self.close()?;
            }
        } else {
            // Drop the writer without writing — file at `path` will not exist.
            self.inner = None;
        }
        Ok(false)
    }
}

#[pyclass(name = "DatasetWriter", module = "tset._rs")]
pub struct PyDatasetWriter {
    inner: Option<CoreDatasetWriter>,
    root: std::path::PathBuf,
}

#[pymethods]
impl PyDatasetWriter {
    #[new]
    fn new(root: &str) -> PyResult<Self> {
        let inner = CoreDatasetWriter::create(root).map_err(map_err)?;
        Ok(Self {
            inner: Some(inner),
            root: std::path::PathBuf::from(root),
        })
    }

    /// Return the path where a shard with the given name will be written.
    /// Caller writes the shard via `tset_rs.Writer(path)` then calls
    /// `register_shard(name)`.
    fn shard_path(&self, name: &str) -> PyResult<String> {
        let w = self
            .inner
            .as_ref()
            .ok_or_else(|| PyValueError::new_err("dataset writer already closed"))?;
        Ok(w.shard_path(name).to_string_lossy().into_owned())
    }

    fn register_shard(&mut self, name: &str) -> PyResult<()> {
        let w = self
            .inner
            .as_mut()
            .ok_or_else(|| PyValueError::new_err("dataset writer already closed"))?;
        w.register_shard(name).map_err(map_err)?;
        Ok(())
    }

    #[pyo3(signature = (doc_hash, reason=""))]
    fn add_exclusion(&mut self, doc_hash: &[u8], reason: &str) -> PyResult<()> {
        if doc_hash.len() != 32 {
            return Err(PyValueError::new_err("doc_hash must be 32 bytes"));
        }
        let w = self
            .inner
            .as_mut()
            .ok_or_else(|| PyValueError::new_err("dataset writer already closed"))?;
        let mut h = [0u8; 32];
        h.copy_from_slice(doc_hash);
        w.add_exclusion(&h, reason);
        Ok(())
    }

    fn close(&mut self) -> PyResult<()> {
        let w = self
            .inner
            .take()
            .ok_or_else(|| PyValueError::new_err("dataset writer already closed"))?;
        w.close().map_err(map_err)
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &mut self,
        exc_type: &Bound<'_, PyAny>,
        _exc_value: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        if exc_type.is_none() && self.inner.is_some() {
            self.close()?;
        } else {
            self.inner = None;
        }
        Ok(false)
    }

    #[getter]
    fn root(&self) -> String {
        self.root.to_string_lossy().into_owned()
    }
}

#[pyclass(name = "Dataset", module = "tset._rs")]
pub struct PyDataset {
    inner: CoreDataset,
}

#[pymethods]
impl PyDataset {
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        Ok(Self {
            inner: CoreDataset::open(path).map_err(map_err)?,
        })
    }

    fn shard_paths(&self) -> Vec<String> {
        self.inner
            .shard_paths()
            .iter()
            .map(|p| p.to_string_lossy().into_owned())
            .collect()
    }

    fn exclusions(&self) -> Vec<String> {
        self.inner.exclusions().iter().cloned().collect()
    }

    fn is_excluded(&self, doc_hash_hex: &str) -> bool {
        self.inner.is_excluded(doc_hash_hex)
    }

    fn dataset_merkle_root<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let r = self.inner.dataset_merkle_root().map_err(map_err)?;
        Ok(PyBytes::new_bound(py, &r))
    }
}

/// Generate a fresh Ed25519 signing key. Returns (secret_bytes, public_bytes).
/// Use the returned secret to construct a `Writer(..., signing_key=secret)`.
#[pyfunction]
fn generate_signing_key(py: Python<'_>) -> (Bound<'_, PyBytes>, Bound<'_, PyBytes>) {
    let signer = AuditSigner::generate();
    let secret = signer.secret_bytes();
    let public = signer.public_key_bytes();
    (
        PyBytes::new_bound(py, &secret),
        PyBytes::new_bound(py, &public),
    )
}

/// Compute the public key for an existing 32-byte secret.
#[pyfunction]
fn signing_public_key<'py>(
    py: Python<'py>,
    secret_bytes: &[u8],
) -> PyResult<Bound<'py, PyBytes>> {
    let s = AuditSigner::from_secret_bytes(secret_bytes)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    Ok(PyBytes::new_bound(py, &s.public_key_bytes()))
}

/// Verify an Ed25519 signature against a public key + message.
#[pyfunction]
fn verify_audit_signature(
    public_key: &[u8],
    message: &[u8],
    signature: &[u8],
) -> bool {
    verify_signature(public_key, message, signature)
}

/// Append a new tokenization view to an existing TSET shard, in-place.
/// Mirrors `tset.writer.append_tokenizer_view` (Python).
#[pyfunction]
fn append_tokenizer_view(path: &str, tokenizer_id: &str, vocab_size: u32) -> PyResult<()> {
    let tok: Box<dyn Tokenizer> = match tokenizer_id {
        ByteLevelTokenizer::ID => Box::new(ByteLevelTokenizer),
        WhitespaceTokenizer::ID => Box::new(
            WhitespaceTokenizer::new(vocab_size).map_err(map_err)?,
        ),
        other => {
            return Err(PyValueError::new_err(format!(
                "unknown tokenizer_id: {other}"
            )))
        }
    };
    core_append_tokenizer_view(path, tok).map_err(map_err)
}

#[pymodule]
fn tset_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyReader>()?;
    m.add_class::<PyWriter>()?;
    m.add_class::<PyDataset>()?;
    m.add_class::<PyDatasetWriter>()?;
    m.add_function(wrap_pyfunction!(verify_inclusion_proof, m)?)?;
    m.add_function(wrap_pyfunction!(verify_non_inclusion_proof, m)?)?;
    m.add_function(wrap_pyfunction!(generate_signing_key, m)?)?;
    m.add_function(wrap_pyfunction!(signing_public_key, m)?)?;
    m.add_function(wrap_pyfunction!(verify_audit_signature, m)?)?;
    m.add_function(wrap_pyfunction!(append_tokenizer_view, m)?)?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
