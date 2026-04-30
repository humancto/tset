//! Multi-shard dataset layout per RFC §5.8.
//!
//! On disk:
//!
//! ```text
//! my-dataset.tset/
//!   manifest.tset.json   <- root manifest (JSON in v0.1; pretty-printed,
//!                          sort_keys=True, indent=2 to match Python)
//!   shards/
//!     part-00001.tset
//!     part-00002.tset
//!   exclusions.json      <- dataset-wide exclusion overlay
//! ```
//!
//! A single `.tset` file is a valid degenerate dataset of size 1.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{json, Value};

use crate::audit_log::AuditLog;
use crate::error::{TsetError, TsetResult};
use crate::hashing::{hash_bytes, Hash};
use crate::reader::Reader;
use crate::smt::{empty_root, InclusionProof, NonInclusionProof};
use crate::tokenizers::canonical_json;

pub const DATASET_MANIFEST_NAME: &str = "manifest.tset.json";
pub const EXCLUSIONS_NAME: &str = "exclusions.json";
pub const SHARDS_DIRNAME: &str = "shards";

/// Domain-separated hash of (manifest_hash || shard_merkle_root) for the
/// dataset-level Merkle commitment. We deliberately don't hash the entire
/// shard bytes — that would defeat single-shard updates, and the manifest
/// hash already binds the entire shard contents.
fn shard_hash_for_dataset(shard: &Reader) -> Hash {
    let mut buf = Vec::with_capacity(1 + 32 + 32);
    buf.push(0x20);
    buf.extend_from_slice(&shard.header.manifest_hash);
    buf.extend_from_slice(&shard.header.shard_merkle_root);
    hash_bytes(&buf)
}

/// Dataset overlay version, versioned independently of the per-shard
/// binary wire format.
///
/// Legacy variants — root committed only to shard entries:
/// - `"0.1.0"` (original Python writer)
/// - `"0.2.0"` (original Rust writer; same computation, different
///   string — pre-existing minor inconsistency)
///
/// Current — composite root that ALSO commits the exclusion overlay
/// (the spec fix for issue #4):
/// - `"0.3.0"`
pub const OVERLAY_VERSION_CURRENT: &str = "0.3.0";

/// Treat the listed strings as legacy and route their root computation
/// to the shards-only path. Anything else (including unknown future
/// versions) yields the composite root.
fn is_legacy_overlay(version: &str) -> bool {
    matches!(version, "0.1.0" | "0.2.0")
}

/// Decode a hex doc-hash string to a 32-byte hash, surfacing both
/// "not hex" and "wrong length" as `TsetError::BadManifest`. Using
/// `unwrap_or_default()` here would silently collapse arbitrary
/// invalid strings to `[0u8; 32]`, which lets a tampered or corrupted
/// `exclusions.json` produce a valid-looking root and weakens the
/// integrity guarantee this whole subtree exists to provide.
fn parse_doc_hash_hex(s: &str) -> TsetResult<[u8; 32]> {
    let bytes = hex::decode(s)
        .map_err(|_| TsetError::BadManifest("dataset overlay hash is not valid hex"))?;
    if bytes.len() != 32 {
        return Err(TsetError::BadManifest(
            "dataset overlay hash is not 32 bytes",
        ));
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Ok(out)
}

/// Sort shards by `shard_id` and Merkle-hash the per-shard leaves
/// `(0x21 || shard_id_bytes || shard_hash || shard_smt_root)`. Used as
/// the v1 dataset root and as the shards subtree of the v2 composite.
fn shards_subroot(entries: &[ShardEntry]) -> TsetResult<Hash> {
    if entries.is_empty() {
        return Ok(empty_root());
    }
    let mut sorted: Vec<&ShardEntry> = entries.iter().collect();
    sorted.sort_by(|a, b| a.shard_id.cmp(&b.shard_id));
    let mut leaves: Vec<Hash> = Vec::with_capacity(sorted.len());
    for e in sorted {
        let shard_hash = parse_doc_hash_hex(&e.shard_hash)?;
        let shard_smt_root = parse_doc_hash_hex(&e.shard_smt_root)?;
        let mut buf = Vec::with_capacity(1 + e.shard_id.len() + 32 + 32);
        buf.push(0x21);
        buf.extend_from_slice(e.shard_id.as_bytes());
        buf.extend_from_slice(&shard_hash);
        buf.extend_from_slice(&shard_smt_root);
        leaves.push(hash_bytes(&buf));
    }
    Ok(crate::hashing::merkle_root_unsorted(&leaves))
}

/// Domain-tagged Merkle root over the sorted exclusion set. Each leaf
/// is `BLAKE3(0x22 || raw_doc_hash_bytes)`. `0x22` is a distinct domain
/// tag from `0x21` (shard leaves) so a hash collision between a shard
/// and an exclusion can't pun across the two subtrees.
fn exclusions_subroot(exclusions: &BTreeSet<String>) -> TsetResult<Hash> {
    if exclusions.is_empty() {
        return Ok(empty_root());
    }
    // BTreeSet iterates in sorted order, matching the Python impl.
    let mut leaves: Vec<Hash> = Vec::with_capacity(exclusions.len());
    for hex_h in exclusions {
        let raw = parse_doc_hash_hex(hex_h)?;
        let mut buf = Vec::with_capacity(1 + 32);
        buf.push(0x22);
        buf.extend_from_slice(&raw);
        leaves.push(hash_bytes(&buf));
    }
    Ok(crate::hashing::merkle_root_unsorted(&leaves))
}

/// Compute the dataset Merkle root for the given overlay version.
///
/// Known-legacy versions return the shards-only root, matching pre-fix
/// behavior so existing datasets verify with the same root they were
/// written with. Current and future versions return the composite
/// `BLAKE3(0x42 || shards_subroot || exclusions_subroot)`. Adding or
/// revoking an exclusion therefore changes the root — the spec fix
/// for issue #4.
///
/// Returns `TsetError::BadManifest` if any shard hash, SMT root, or
/// excluded doc-hash string fails to decode as 32-byte hex. Python's
/// `bytes.fromhex` raises on the same input class, so cross-impl
/// verification stays in agreement on tampered overlays.
fn dataset_merkle_root(
    entries: &[ShardEntry],
    exclusions: &BTreeSet<String>,
    overlay_version: &str,
) -> TsetResult<Hash> {
    let shards_root = shards_subroot(entries)?;
    if is_legacy_overlay(overlay_version) {
        return Ok(shards_root);
    }
    let excl_root = exclusions_subroot(exclusions)?;
    let mut buf = Vec::with_capacity(1 + 32 + 32);
    buf.push(0x42);
    buf.extend_from_slice(&shards_root);
    buf.extend_from_slice(&excl_root);
    Ok(hash_bytes(&buf))
}

#[derive(Debug, Clone)]
pub struct ShardEntry {
    pub shard_id: String,
    pub relpath: String,
    pub shard_hash: String,
    pub shard_smt_root: String,
    pub doc_count: u64,
    pub total_tokens_per_view: serde_json::Map<String, Value>,
}

impl ShardEntry {
    pub fn to_json(&self) -> Value {
        json!({
            "shard_id": self.shard_id,
            "relpath": self.relpath,
            "shard_hash": self.shard_hash,
            "shard_smt_root": self.shard_smt_root,
            "doc_count": self.doc_count,
            "total_tokens_per_view": Value::Object(self.total_tokens_per_view.clone()),
        })
    }

    pub fn from_json(v: &Value) -> Self {
        Self {
            shard_id: v
                .get("shard_id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            relpath: v
                .get("relpath")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            shard_hash: v
                .get("shard_hash")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            shard_smt_root: v
                .get("shard_smt_root")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            doc_count: v.get("doc_count").and_then(Value::as_u64).unwrap_or(0),
            total_tokens_per_view: v
                .get("total_tokens_per_view")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default(),
        }
    }
}

pub struct Dataset {
    root: PathBuf,
    single_file: bool,
    shard_paths: Vec<PathBuf>,
    exclusions: BTreeSet<String>,
    dataset_manifest: Option<Value>,
}

impl Dataset {
    pub fn open<P: AsRef<Path>>(path: P) -> TsetResult<Self> {
        let root: PathBuf = path.as_ref().to_path_buf();
        let single_file =
            root.is_file() && root.extension().and_then(|s| s.to_str()) == Some("tset");
        if single_file {
            return Ok(Self {
                shard_paths: vec![root.clone()],
                root,
                single_file: true,
                exclusions: BTreeSet::new(),
                dataset_manifest: None,
            });
        }
        let manifest_path = root.join(DATASET_MANIFEST_NAME);
        let bytes = fs::read(&manifest_path)?;
        let dataset_manifest: Value = serde_json::from_slice(&bytes)?;
        let shards_arr = dataset_manifest
            .get("shards")
            .and_then(Value::as_array)
            .ok_or(TsetError::BadManifest("dataset manifest missing shards"))?;
        let mut shard_paths = Vec::with_capacity(shards_arr.len());
        for s in shards_arr {
            let relpath = s
                .get("relpath")
                .and_then(Value::as_str)
                .ok_or(TsetError::BadManifest("shard.relpath"))?;
            shard_paths.push(root.join(relpath));
        }
        let mut exclusions = BTreeSet::new();
        let excl_path = root.join(EXCLUSIONS_NAME);
        if excl_path.exists() {
            let raw = fs::read(&excl_path)?;
            let excl: Value = serde_json::from_slice(&raw)?;
            if let Some(arr) = excl.get("excluded_doc_hashes").and_then(Value::as_array) {
                for h in arr {
                    if let Some(s) = h.as_str() {
                        exclusions.insert(s.to_string());
                    }
                }
            }
        }
        Ok(Self {
            root,
            single_file: false,
            shard_paths,
            exclusions,
            dataset_manifest: Some(dataset_manifest),
        })
    }

    pub fn shard_paths(&self) -> &[PathBuf] {
        &self.shard_paths
    }

    pub fn exclusions(&self) -> &BTreeSet<String> {
        &self.exclusions
    }

    pub fn is_excluded(&self, doc_hash_hex: &str) -> bool {
        self.exclusions.contains(doc_hash_hex)
    }

    pub fn dataset_merkle_root(&self) -> TsetResult<Hash> {
        if self.single_file {
            let r = Reader::open(&self.root)?;
            return Ok(r.header.shard_merkle_root);
        }
        let manifest = self
            .dataset_manifest
            .as_ref()
            .ok_or(TsetError::BadManifest("missing dataset manifest"))?;
        let arr = manifest
            .get("shards")
            .and_then(Value::as_array)
            .ok_or(TsetError::BadManifest("dataset manifest.shards"))?;
        let mut entries = Vec::new();
        for s in arr {
            entries.push(ShardEntry::from_json(s));
        }
        // Pick the computation that matches the overlay version this
        // dataset was written with. Legacy datasets (v0.1.0) keep the
        // shards-only root they had on disk; current datasets (v0.2.0+)
        // bind the exclusion overlay too, per issue #4.
        // Default to "0.1.0" if the version field is missing — the
        // only state that ever existed without one.
        let overlay_version = manifest
            .get("version")
            .and_then(Value::as_str)
            .unwrap_or("0.1.0");
        dataset_merkle_root(&entries, &self.exclusions, overlay_version)
    }

    /// Locate a document across all shards (skipping excluded ones for
    /// inclusion proofs). Returns the shard path + InclusionProof.
    pub fn prove_inclusion(&self, doc_hash: &Hash) -> TsetResult<(PathBuf, InclusionProof)> {
        let hex = hex::encode(doc_hash);
        for p in &self.shard_paths {
            let r = Reader::open(p)?;
            if r.has_document(doc_hash) {
                if self.exclusions.contains(&hex) {
                    return Err(TsetError::BadManifest("document is dataset-level excluded"));
                }
                let smt = build_smt_from_reader(&r);
                let proof = match smt.prove(doc_hash) {
                    crate::smt::Proof::Inclusion(p) => p,
                    crate::smt::Proof::NonInclusion(_) => {
                        return Err(TsetError::BadManifest(
                            "shard claims doc but SMT says absent",
                        ))
                    }
                };
                return Ok((p.clone(), proof));
            }
        }
        Err(TsetError::DocumentNotFound(hex))
    }

    /// Compose a dataset-level non-inclusion proof. For each shard,
    /// either an absent SMT proof or — if the doc is present — an
    /// inclusion proof bound to a non-empty exclusion overlay.
    pub fn prove_non_inclusion(&self, doc_hash: &Hash) -> TsetResult<Value> {
        let hex_h = hex::encode(doc_hash);
        let excluded = self.exclusions.contains(&hex_h);
        let mut shards_out: Vec<Value> = Vec::with_capacity(self.shard_paths.len());
        for p in &self.shard_paths {
            let r = Reader::open(p)?;
            let smt_root = r.smt_root();
            let smt = build_smt_from_reader(&r);
            if r.has_document(doc_hash) {
                if !excluded {
                    return Err(TsetError::BadManifest(
                        "document present in shard and not excluded",
                    ));
                }
                let proof = match smt.prove(doc_hash) {
                    crate::smt::Proof::Inclusion(p) => p,
                    _ => {
                        return Err(TsetError::BadManifest(
                            "shard claims doc but SMT says absent",
                        ))
                    }
                };
                shards_out.push(json!({
                    "shard": p.to_string_lossy(),
                    "smt_root": hex::encode(smt_root),
                    "claim": "present_but_excluded",
                    "inclusion_proof": {
                        "siblings": proof.siblings.iter().map(hex::encode).collect::<Vec<_>>(),
                    },
                }));
            } else {
                let proof = match smt.prove(doc_hash) {
                    crate::smt::Proof::NonInclusion(p) => p,
                    _ => {
                        return Err(TsetError::BadManifest(
                            "shard SMT says present but reader says absent",
                        ))
                    }
                };
                shards_out.push(json!({
                    "shard": p.to_string_lossy(),
                    "smt_root": hex::encode(smt_root),
                    "claim": "absent",
                    "non_inclusion_proof": {
                        "siblings": proof.siblings.iter().map(hex::encode).collect::<Vec<_>>(),
                    },
                }));
            }
        }
        let dataset_root = self.dataset_merkle_root()?;
        Ok(json!({
            "doc_hash": hex_h,
            "dataset_merkle_root": hex::encode(dataset_root),
            "shards": shards_out,
            "exclusion_overlay_includes": excluded,
        }))
    }

    pub fn verify_non_inclusion_proof(proof: &Value) -> bool {
        let Some(hex_h) = proof.get("doc_hash").and_then(Value::as_str) else {
            return false;
        };
        let bytes = match hex::decode(hex_h) {
            Ok(b) if b.len() == 32 => b,
            _ => return false,
        };
        let mut doc_hash = [0u8; 32];
        doc_hash.copy_from_slice(&bytes);
        let Some(shards) = proof.get("shards").and_then(Value::as_array) else {
            return false;
        };
        let overlay_includes = proof
            .get("exclusion_overlay_includes")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        for s in shards {
            let Some(smt_root_hex) = s.get("smt_root").and_then(Value::as_str) else {
                return false;
            };
            let mut smt_root = [0u8; 32];
            match hex::decode(smt_root_hex) {
                Ok(b) if b.len() == 32 => smt_root.copy_from_slice(&b),
                _ => return false,
            }
            let claim = s.get("claim").and_then(Value::as_str).unwrap_or("");
            match claim {
                "absent" => {
                    let Some(siblings_arr) = s
                        .pointer("/non_inclusion_proof/siblings")
                        .and_then(Value::as_array)
                    else {
                        return false;
                    };
                    let siblings = match decode_siblings(siblings_arr) {
                        Some(v) => v,
                        None => return false,
                    };
                    let proof = NonInclusionProof {
                        key: doc_hash,
                        siblings,
                    };
                    if !proof.verify(&smt_root) {
                        return false;
                    }
                }
                "present_but_excluded" => {
                    if !overlay_includes {
                        return false;
                    }
                    let Some(siblings_arr) = s
                        .pointer("/inclusion_proof/siblings")
                        .and_then(Value::as_array)
                    else {
                        return false;
                    };
                    let siblings = match decode_siblings(siblings_arr) {
                        Some(v) => v,
                        None => return false,
                    };
                    let proof = InclusionProof {
                        key: doc_hash,
                        siblings,
                    };
                    if !proof.verify(&smt_root) {
                        return false;
                    }
                }
                _ => return false,
            }
        }
        true
    }
}

fn decode_siblings(arr: &[Value]) -> Option<Vec<Hash>> {
    // The SMT is fixed-depth at SMT_DEPTH=256. A proof must have exactly
    // that many siblings; anything else is malformed and we reject up front
    // rather than letting verify_path return a meaningless `None`.
    if arr.len() != crate::smt::SMT_DEPTH {
        return None;
    }
    let mut out = Vec::with_capacity(arr.len());
    for v in arr {
        let s = v.as_str()?;
        let bytes = hex::decode(s).ok()?;
        if bytes.len() != 32 {
            return None;
        }
        let mut h = [0u8; 32];
        h.copy_from_slice(&bytes);
        out.push(h);
    }
    Some(out)
}

fn build_smt_from_reader(r: &Reader) -> crate::smt::SparseMerkleTree {
    // Reconstruct the SMT from the manifest's smt_present_keys list.
    let mut tree = crate::smt::SparseMerkleTree::new();
    if let Some(arr) = r
        .manifest()
        .raw()
        .get("smt_present_keys")
        .and_then(Value::as_array)
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

pub struct DatasetWriter {
    root: PathBuf,
    shards: Vec<ShardEntry>,
    exclusions: BTreeSet<String>,
    audit: AuditLog,
    closed: bool,
}

impl DatasetWriter {
    pub fn create<P: AsRef<Path>>(root: P) -> TsetResult<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(root.join(SHARDS_DIRNAME))?;
        Ok(Self {
            root,
            shards: Vec::new(),
            exclusions: BTreeSet::new(),
            audit: AuditLog::new(),
            closed: false,
        })
    }

    /// Re-open an existing dataset for extension. Mirrors the Python
    /// `DatasetWriter(root, load_existing=True)` contract: prior shard
    /// registrations, prior exclusions, and the existing audit log are
    /// restored so a subsequent `add_exclusion` / `register_shard` /
    /// `close` produces a well-formed v0.3.0 dataset that's a
    /// continuation, not a fresh start.
    ///
    /// Use this when adding an exclusion to a dataset that's already
    /// been closed (the standard `tset add-exclusion` CLI flow).
    pub fn open_existing<P: AsRef<Path>>(root: P) -> TsetResult<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(root.join(SHARDS_DIRNAME))?;
        let manifest_path = root.join(DATASET_MANIFEST_NAME);
        let mut shards: Vec<ShardEntry> = Vec::new();
        let mut audit = AuditLog::new();
        if manifest_path.exists() {
            let bytes = fs::read(&manifest_path)?;
            let manifest: Value = serde_json::from_slice(&bytes)?;
            if let Some(arr) = manifest.get("shards").and_then(Value::as_array) {
                for s in arr {
                    shards.push(ShardEntry::from_json(s));
                }
            }
            if let Some(audit_json) = manifest.get("audit_log") {
                audit = AuditLog::from_json(audit_json);
            }
        }
        if audit.was_loaded_signed_without_key() {
            // The existing audit log carries an Ed25519 signing
            // public key. Adding new (necessarily unsigned) entries
            // would silently downgrade the integrity contract — the
            // resulting log would have a mix of signed + unsigned
            // entries, which `verify_audit_log` rightly rejects, OR
            // we'd have to drop the public key entirely (also a
            // downgrade). Refuse the open until a key-injection API
            // exists. (Codex P1 on PR #16.)
            return Err(TsetError::BadManifest(
                "dataset audit log is Ed25519-signed; \
                 DatasetWriter::open_existing cannot extend it without \
                 the signing key. Use the Python API with an \
                 explicit signer, or perform the operation on a \
                 fresh unsigned dataset",
            ));
        }
        let mut exclusions = BTreeSet::new();
        let excl_path = root.join(EXCLUSIONS_NAME);
        if excl_path.exists() {
            let raw = fs::read(&excl_path)?;
            let excl: Value = serde_json::from_slice(&raw)?;
            if let Some(arr) = excl.get("excluded_doc_hashes").and_then(Value::as_array) {
                for h in arr {
                    if let Some(s) = h.as_str() {
                        exclusions.insert(s.to_string());
                    }
                }
            }
        }
        Ok(Self {
            root,
            shards,
            exclusions,
            audit,
            closed: false,
        })
    }

    /// Path where a shard with the given name will be written. Caller
    /// uses the regular `Writer` to populate it.
    pub fn shard_path(&self, name: &str) -> PathBuf {
        self.root.join(SHARDS_DIRNAME).join(format!("{name}.tset"))
    }

    pub fn register_shard(&mut self, name: &str) -> TsetResult<&ShardEntry> {
        let relpath = format!("{}/{}.tset", SHARDS_DIRNAME, name);
        let shard_path = self.root.join(&relpath);
        let r = Reader::open(&shard_path)?;
        let shard_id = r
            .shard_id()
            .ok_or(TsetError::BadManifest("shard missing shard_id"))?
            .to_string();
        let smt_root = r.smt_root();
        let doc_count = r.doc_hashes().count() as u64;
        let mut totals = serde_json::Map::new();
        for tid in r.tokenizer_ids()? {
            let total = r.view_total_tokens(&tid)?;
            totals.insert(tid, json!(total));
        }
        let shard_hash = shard_hash_for_dataset(&r);
        let entry = ShardEntry {
            shard_id: shard_id.clone(),
            relpath,
            shard_hash: hex::encode(shard_hash),
            shard_smt_root: hex::encode(smt_root),
            doc_count,
            total_tokens_per_view: totals,
        };
        self.audit.append(
            "ingestion",
            json!({
                "shard_id": shard_id,
                "doc_count": doc_count,
                "shard_hash": hex::encode(shard_hash),
            }),
            current_timestamp(),
        );
        self.shards.push(entry);
        Ok(self.shards.last().unwrap())
    }

    /// Read-only view of the current exclusion set. Used by callers
    /// that need to distinguish "newly excluded" from "already
    /// excluded" (e.g. the `tset add-exclusion` CLI's no-op message).
    pub fn exclusions(&self) -> &BTreeSet<String> {
        &self.exclusions
    }

    /// Returns `true` if the hash was newly excluded, `false` if it
    /// was already in the overlay (no-op, no audit entry appended).
    pub fn add_exclusion(&mut self, doc_hash: &Hash, reason: &str) -> bool {
        let h = hex::encode(doc_hash);
        if self.exclusions.insert(h.clone()) {
            self.audit.append(
                "exclusion",
                json!({"doc_hash": h, "reason": reason}),
                current_timestamp(),
            );
            true
        } else {
            false
        }
    }

    pub fn close(mut self) -> TsetResult<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        let snapshot_id = format_snapshot_id(current_timestamp());
        // New datasets always use the current overlay version, which
        // commits the exclusion overlay into the root (issue #4).
        // The writer's exclusions are inserted via `add_exclusion(&[u8])`
        // and hex-encoded by us, so this call is only fallible on a
        // logic bug (corrupt internal state) — `?` is enough.
        let ds_root = dataset_merkle_root(&self.shards, &self.exclusions, OVERLAY_VERSION_CURRENT)?;
        self.audit.append(
            "version_snapshot",
            json!({
                "snapshot_id": snapshot_id,
                "dataset_merkle_root": hex::encode(ds_root),
                "shard_count": self.shards.len(),
                "exclusion_count": self.exclusions.len(),
            }),
            current_timestamp(),
        );
        let manifest = json!({
            "version": OVERLAY_VERSION_CURRENT,
            "created_at": current_timestamp(),
            "shards": self.shards.iter().map(ShardEntry::to_json).collect::<Vec<_>>(),
            "dataset_merkle_root": hex::encode(ds_root),
            "exclusion_count": self.exclusions.len(),
            "audit_log": self.audit.to_json(),
            "snapshot_id": snapshot_id.clone(),
        });
        // Pretty-print with sort_keys+indent=2 to match Python's
        // `json.dump(manifest, indent=2, sort_keys=True)`. canonical_json
        // is compact (no indent); use a custom path here.
        let mut sorted_pretty = String::new();
        write_pretty_sorted(&manifest, 0, &mut sorted_pretty);
        sorted_pretty.push('\n');
        fs::write(self.root.join(DATASET_MANIFEST_NAME), &sorted_pretty)?;

        let exclusions_doc = json!({
            "snapshot_id": snapshot_id,
            "excluded_doc_hashes": self.exclusions.iter().cloned().collect::<Vec<_>>(),
        });
        let mut excl_pretty = String::new();
        write_pretty_sorted(&exclusions_doc, 0, &mut excl_pretty);
        excl_pretty.push('\n');
        fs::write(self.root.join(EXCLUSIONS_NAME), excl_pretty)?;
        Ok(())
    }
}

fn current_timestamp() -> f64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// Format `secs_since_epoch` as `snapshot-YYYYMMDD-HHMMSS` UTC, matching
/// Python's `datetime.now(timezone.utc).strftime("snapshot-%Y%m%d-%H%M%S")`.
/// Implemented inline (no chrono dep) using the standard civil-from-days
/// algorithm.
pub(crate) fn format_snapshot_id(secs_since_epoch: f64) -> String {
    let secs = secs_since_epoch as i64;
    let days = secs.div_euclid(86_400);
    let day_secs = secs.rem_euclid(86_400);
    let h = day_secs / 3600;
    let m = (day_secs % 3600) / 60;
    let s = day_secs % 60;
    let (year, month, day) = civil_from_days(days);
    format!("snapshot-{year:04}{month:02}{day:02}-{h:02}{m:02}{s:02}")
}

/// Hinnant's "days_from_civil" inverse — returns (year, month, day).
/// Days are unix epoch days (1970-01-01 = 0).
fn civil_from_days(z: i64) -> (i32, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u32; // [0, 146_096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = (yoe as i64 + era * 400) as i32;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let year = if m <= 2 { y + 1 } else { y };
    (year, m, d)
}

fn write_pretty_sorted(v: &Value, indent: usize, out: &mut String) {
    use std::fmt::Write;
    match v {
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            out.push_str(&canonical_json(v));
        }
        Value::Array(arr) => {
            if arr.is_empty() {
                out.push_str("[]");
                return;
            }
            out.push('[');
            for (i, item) in arr.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push('\n');
                for _ in 0..(indent + 2) {
                    out.push(' ');
                }
                write_pretty_sorted(item, indent + 2, out);
            }
            out.push('\n');
            for _ in 0..indent {
                out.push(' ');
            }
            out.push(']');
        }
        Value::Object(map) => {
            if map.is_empty() {
                out.push_str("{}");
                return;
            }
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push('{');
            for (i, k) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push('\n');
                for _ in 0..(indent + 2) {
                    out.push(' ');
                }
                let _ = write!(out, "\"{}\": ", k);
                write_pretty_sorted(map.get(*k).unwrap(), indent + 2, out);
            }
            out.push('\n');
            for _ in 0..indent {
                out.push(' ');
            }
            out.push('}');
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_id_matches_python_strftime() {
        // From Python (Apr 2026):
        //   import calendar
        //   calendar.timegm((2026, 4, 27, 15, 34, 22, 0, 0, 0))  -> 1777304062
        //   datetime.fromtimestamp(1777304062, timezone.utc)
        //     .strftime("snapshot-%Y%m%d-%H%M%S")
        //     -> "snapshot-20260427-153422"
        let t = 1_777_304_062.0;
        assert_eq!(format_snapshot_id(t), "snapshot-20260427-153422");
    }

    #[test]
    fn snapshot_id_zero_is_unix_epoch() {
        assert_eq!(format_snapshot_id(0.0), "snapshot-19700101-000000");
    }
}
