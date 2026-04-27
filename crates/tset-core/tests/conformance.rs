//! Run the Rust reader against the language-agnostic conformance corpus
//! built by `tests/conformance/build_corpus.py`.
//!
//! Each `<name>.tset` shard is paired with `<name>.expected.json`. The
//! reader must report invariants that match the sidecar exactly.
//!
//! Skipped (with a printed warning) if the fixtures aren't present —
//! that case means the developer hasn't run the corpus generator yet.

use std::path::PathBuf;

use serde_json::Value;
use tset_core::Reader;

fn fixtures_dir() -> PathBuf {
    let manifest_dir: PathBuf = env!("CARGO_MANIFEST_DIR").into();
    // crates/tset-core/Cargo.toml → ../../tests/conformance/fixtures
    manifest_dir
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("tests")
        .join("conformance")
        .join("fixtures")
}

fn cases() -> Vec<String> {
    let dir = fixtures_dir();
    if !dir.exists() {
        return Vec::new();
    }
    let mut out = Vec::new();
    for entry in std::fs::read_dir(&dir).unwrap().flatten() {
        let p = entry.path();
        if p.extension().and_then(|s| s.to_str()) == Some("tset") {
            if let Some(stem) = p.file_stem().and_then(|s| s.to_str()) {
                let sidecar = dir.join(format!("{stem}.expected.json"));
                if sidecar.exists() {
                    out.push(stem.to_string());
                }
            }
        }
    }
    out.sort();
    out
}

#[test]
fn rust_reader_matches_conformance_invariants() {
    let cases = cases();
    if cases.is_empty() {
        eprintln!(
            "skipping conformance: no fixtures in {}; run \
             `python tests/conformance/build_corpus.py`",
            fixtures_dir().display()
        );
        return;
    }
    for name in &cases {
        let shard = fixtures_dir().join(format!("{name}.tset"));
        let sidecar = fixtures_dir().join(format!("{name}.expected.json"));
        let expected: Value = serde_json::from_slice(&std::fs::read(&sidecar).unwrap()).unwrap();

        let r = Reader::open(&shard).unwrap_or_else(|e| {
            panic!("conformance: failed to open {}: {}", shard.display(), e)
        });

        let exp_minor = expected["version_minor"].as_u64().unwrap() as u8;
        assert_eq!(r.header.version_minor, exp_minor, "version_minor for {name}");

        assert_eq!(
            hex::encode(r.header.shard_merkle_root),
            expected["shard_merkle_root"].as_str().unwrap(),
            "shard_merkle_root for {name}"
        );
        assert_eq!(
            hex::encode(r.header.manifest_hash),
            expected["manifest_hash"].as_str().unwrap(),
            "manifest_hash for {name}"
        );
        assert_eq!(
            r.header.manifest_size,
            expected["manifest_size"].as_u64().unwrap(),
            "manifest_size for {name}"
        );

        let doc_count = r
            .manifest()
            .raw()
            .pointer("/document_store/document_index")
            .and_then(Value::as_object)
            .map(|m| m.len())
            .unwrap_or(0) as u64;
        assert_eq!(
            doc_count,
            expected["document_count"].as_u64().unwrap(),
            "document_count for {name}"
        );

        // Tokenization views must match per-view invariants
        let exp_views = expected["tokenization_views"].as_object().unwrap();
        for (tid, exp) in exp_views {
            let total = r.view_total_tokens(tid).unwrap();
            assert_eq!(total, exp["total_tokens"].as_u64().unwrap(),
                "total_tokens {name}/{tid}");
            let vocab = exp["vocab_size"].as_u64().unwrap();
            let view = r.manifest().view(tid).unwrap();
            assert_eq!(view["vocab_size"].as_u64().unwrap(), vocab);
            assert_eq!(
                view["config_hash"].as_str().unwrap(),
                exp["config_hash"].as_str().unwrap(),
                "config_hash {name}/{tid}"
            );
            assert_eq!(
                view["chunks"].as_array().unwrap().len() as u64,
                exp["num_chunks"].as_u64().unwrap(),
                "num_chunks {name}/{tid}"
            );
        }
    }
    eprintln!("conformance: {} fixtures verified", cases.len());
}
