//! `tset` — command-line tool for TSET shards.
//!
//! Subcommands:
//!   tset inspect <path>                       summarize header, manifest, views
//!   tset verify  <path>                       full open + integrity check; exit 0 on pass
//!   tset stats   <path>                       size breakdown + per-doc/per-view distributions
//!   tset diff    <a> <b>                      compare two shards: roots, docs, views, sections
//!   tset convert jsonl <src> <dst>            build a TSET shard from a JSONL corpus
//!   tset add-exclusion <dataset> <hex_hash>   record a GDPR-Art-17 exclusion in a dataset overlay
//!   tset conformance <shard> <expected>       run the language-agnostic conformance suite
//!
//! Intentionally argparse-free (no clap) so the CLI has the same minimal
//! footprint as `tset-core`. Add clap when option surface grows.

use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use tset_core::dataset::DatasetWriter;
use tset_core::tokenizers::{ByteLevelTokenizer, Tokenizer, WhitespaceTokenizer};
use tset_core::{Reader, Writer};

fn main() -> ExitCode {
    let args: Vec<String> = env::args().collect();
    let cmd = args.get(1).map(String::as_str).unwrap_or("");
    let rest: Vec<&str> = args[2.min(args.len())..]
        .iter()
        .map(String::as_str)
        .collect();

    let result = match cmd {
        "inspect" => cmd_inspect(&rest),
        "verify" => cmd_verify(&rest),
        "stats" => cmd_stats(&rest),
        "diff" => cmd_diff(&rest),
        "convert" => cmd_convert(&rest),
        "add-exclusion" => cmd_add_exclusion(&rest),
        "conformance" => cmd_conformance(&rest),
        "version" | "--version" | "-V" => {
            println!("tset {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        "" | "help" | "--help" | "-h" => {
            print_usage();
            Ok(())
        }
        unknown => Err(format!("unknown subcommand: {unknown}\n\nrun `tset help`")),
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(msg) => {
            eprintln!("error: {msg}");
            ExitCode::FAILURE
        }
    }
}

fn print_usage() {
    println!(
        "tset {} — open standard for LLM training data\n",
        env!("CARGO_PKG_VERSION")
    );
    println!("USAGE:");
    println!("    tset inspect <path>");
    println!("    tset verify  <path>");
    println!("    tset stats   <path>");
    println!("    tset diff    <a.tset> <b.tset>");
    println!("    tset convert jsonl <src.jsonl> <dst.tset> [--text-field FIELD] [--tokenizer ID] [--vocab N] [--binary-sections]");
    println!("    tset add-exclusion <dataset-dir> <hex-doc-hash> [--reason \"...\"]");
    println!("    tset conformance <shard.tset> <expected.json> [--json]");
    println!("    tset version");
}

fn cmd_inspect(args: &[&str]) -> Result<(), String> {
    let path = args.first().ok_or("inspect: missing <path>")?;
    let r = Reader::open(Path::new(path)).map_err(|e| e.to_string())?;
    println!("path:                {path}");
    println!(
        "version:             {}.{}",
        r.header.version_major, r.header.version_minor
    );
    if let Some(id) = r.shard_id() {
        println!("shard_id:            {id}");
    }
    println!(
        "shard_merkle_root:   {}",
        hex::encode(r.header.shard_merkle_root)
    );
    println!("manifest_offset:     {}", r.header.manifest_offset);
    println!("manifest_size:       {} bytes", r.header.manifest_size);
    let docs: Vec<_> = r.doc_hashes().collect();
    println!("documents:           {}", docs.len());
    let views = r.tokenizer_ids().map_err(|e| e.to_string())?;
    println!("tokenization_views:  {}", views.len());
    for tid in &views {
        let total = r.view_total_tokens(tid).map_err(|e| e.to_string())?;
        println!("  - {tid}: {total} tokens");
    }

    // v0.3.2 on-disk binary sections (TSMT/TLOG/TCOL). Show pointers
    // when present.
    let mut section_lines: Vec<(&str, &serde_json::Value)> = Vec::new();
    for key in [
        "smt_section",
        "audit_log_section",
        "metadata_columns_section",
    ] {
        if let Some(v) = r.manifest().raw().get(key) {
            section_lines.push((key, v));
        }
    }
    if !section_lines.is_empty() {
        println!("on_disk_sections:    {}", section_lines.len());
        for (key, v) in &section_lines {
            let off = v.get("offset").and_then(|x| x.as_u64()).unwrap_or(0);
            let size = v.get("size").and_then(|x| x.as_u64()).unwrap_or(0);
            println!("  - {key}: offset={off} size={size}");
        }
    }

    // Audit-log signing pubkey (PR 10) when present.
    if let Some(audit) = r.manifest().raw().get("audit_log") {
        if let Some(pk) = audit.get("writer_public_key").and_then(|v| v.as_str()) {
            println!("writer_public_key:   {pk}");
        }
    }
    Ok(())
}

fn cmd_verify(args: &[&str]) -> Result<(), String> {
    let path = args.first().ok_or("verify: missing <path>")?;
    Reader::open(Path::new(path)).map_err(|e| e.to_string())?;
    println!("OK: {path}");
    Ok(())
}

fn cmd_convert(args: &[&str]) -> Result<(), String> {
    let kind = args.first().ok_or("convert: missing <format>")?;
    match *kind {
        "jsonl" => convert_jsonl(&args[1..]),
        other => Err(format!(
            "convert: unsupported format {other:?}; supported: jsonl"
        )),
    }
}

fn convert_jsonl(args: &[&str]) -> Result<(), String> {
    if args.len() < 2 {
        return Err("convert jsonl: usage: <src.jsonl> <dst.tset>".into());
    }
    let src = args[0];
    let dst = args[1];

    // Parse optional flags. Supports both `--flag value` (two args) and
    // `--flag=value` (single arg with `=`); `--bool-flag` is a boolean
    // toggle with no value.
    let mut text_field = "text".to_string();
    let mut tokenizer_id = ByteLevelTokenizer::ID.to_string();
    let mut vocab_size: u32 = 0;
    let mut binary_sections = false;
    let mut i = 2;
    while i < args.len() {
        let raw = args[i];
        let (flag, inline_value): (&str, Option<&str>) = match raw.find('=') {
            Some(eq) => (&raw[..eq], Some(&raw[eq + 1..])),
            None => (raw, None),
        };
        let take_value = |name: &str| -> Result<String, String> {
            if let Some(v) = inline_value {
                Ok(v.to_string())
            } else {
                args.get(i + 1)
                    .map(|s| s.to_string())
                    .ok_or_else(|| format!("{name} needs a value"))
            }
        };
        let advance = if inline_value.is_some() { 1 } else { 2 };
        match flag {
            "--text-field" => {
                text_field = take_value("--text-field")?;
                i += advance;
            }
            "--tokenizer" => {
                tokenizer_id = take_value("--tokenizer")?;
                i += advance;
            }
            "--vocab" => {
                vocab_size = take_value("--vocab")?
                    .parse()
                    .map_err(|_| "invalid --vocab".to_string())?;
                i += advance;
            }
            "--binary-sections" => {
                // Boolean toggle — no value to consume
                if inline_value.is_some() {
                    return Err("--binary-sections is a boolean flag; don't pass `=value`".into());
                }
                binary_sections = true;
                i += 1;
            }
            other => return Err(format!("convert jsonl: unknown flag {other:?}")),
        }
    }

    let f = File::open(src).map_err(|e| format!("open {src}: {e}"))?;
    let reader = BufReader::new(f);
    let mut w = Writer::create(dst, None);
    if binary_sections {
        w.enable_binary_sections();
    }
    let mut count: u64 = 0;
    for (lineno, line) in reader.lines().enumerate() {
        let line = line.map_err(|e| format!("read line {}: {}", lineno + 1, e))?;
        if line.trim().is_empty() {
            continue;
        }
        let v: serde_json::Value = serde_json::from_str(&line)
            .map_err(|e| format!("line {}: invalid JSON: {}", lineno + 1, e))?;
        let text = v
            .get(&text_field)
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| format!("line {}: missing field {text_field:?}", lineno + 1))?;
        w.add_document(text.as_bytes()).map_err(|e| e.to_string())?;
        count += 1;
    }

    let tok: Box<dyn Tokenizer> = match tokenizer_id.as_str() {
        ByteLevelTokenizer::ID => Box::new(ByteLevelTokenizer),
        WhitespaceTokenizer::ID => {
            let v = if vocab_size == 0 { 65536 } else { vocab_size };
            Box::new(WhitespaceTokenizer::new(v).map_err(|e| e.to_string())?)
        }
        other => return Err(format!("unknown tokenizer {other:?}")),
    };
    w.add_tokenizer_view(tok).map_err(|e| e.to_string())?;
    w.close().map_err(|e| e.to_string())?;
    println!("converted {count} documents from {src} → {dst}");
    Ok(())
}

// ── stats ───────────────────────────────────────────────────────────────

/// Region-by-region byte breakdown of a shard, plus per-view token totals
/// and per-doc length distribution. Mirrors what
/// `examples/datasets/_lib/profile_size.py` does in Python so the same
/// answers are reachable from the CLI.
fn cmd_stats(args: &[&str]) -> Result<(), String> {
    let path = args.first().ok_or("stats: missing <path>")?;
    let r = Reader::open(Path::new(path)).map_err(|e| e.to_string())?;
    let total_size = std::fs::metadata(path)
        .map_err(|e| format!("stat {path}: {e}"))?
        .len();
    let manifest = r.manifest().raw();

    println!("path:                {path}");
    println!("total_size:          {}", fmt_bytes(total_size));
    println!(
        "version:             {}.{}",
        r.header.version_major, r.header.version_minor
    );

    // Sourced from `tset_core::constants` so the breakdown stays
    // truthful if the format ever bumps these.
    let header_size: u64 = tset_core::constants::HEADER_SIZE as u64;
    let footer_size: u64 = tset_core::constants::FOOTER_SIZE as u64;

    // Doc store: sum compressed_size of all blocks
    let doc_store_bytes: u64 = manifest
        .pointer("/document_store/blocks")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|b| b.get("compressed_size").and_then(|v| v.as_u64()))
                .sum()
        })
        .unwrap_or(0);

    // Tokenizer views: each carries view_size in the manifest.
    let mut view_bytes: Vec<(String, u64, u64)> = Vec::new(); // (id, view_size, total_tokens)
    if let Some(views) = manifest
        .pointer("/tokenization_views")
        .and_then(|v| v.as_object())
    {
        for (id, info) in views {
            let view_size = info
                .get("view_size")
                .and_then(|v| v.as_u64())
                .or_else(|| {
                    info.get("chunks").and_then(|v| v.as_array()).map(|a| {
                        a.iter()
                            .filter_map(|c| c.get("compressed_size").and_then(|v| v.as_u64()))
                            .sum()
                    })
                })
                .unwrap_or(0);
            let total_tokens = info
                .get("total_tokens")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            view_bytes.push((id.clone(), view_size, total_tokens));
        }
    }
    view_bytes.sort_by(|a, b| a.0.cmp(&b.0));

    let mut section_bytes: Vec<(&'static str, u64)> = Vec::new();
    for (key, label) in [
        ("smt_section", "TSMT"),
        ("audit_log_section", "TLOG"),
        ("metadata_columns_section", "TCOL"),
    ] {
        if let Some(v) = manifest.get(key) {
            if let Some(sz) = v.get("size").and_then(|x| x.as_u64()) {
                section_bytes.push((label, sz));
            }
        }
    }

    let known: u64 = header_size
        + doc_store_bytes
        + view_bytes.iter().map(|(_, s, _)| s).sum::<u64>()
        + section_bytes.iter().map(|(_, s)| s).sum::<u64>()
        + footer_size;
    let manifest_bytes = total_size.saturating_sub(known);

    println!("\nregion breakdown:");
    print_region("header", header_size, total_size);
    print_region("doc store", doc_store_bytes, total_size);
    for (id, sz, _) in &view_bytes {
        print_region(&format!("view: {id}"), *sz, total_size);
    }
    for (label, sz) in &section_bytes {
        print_region(&format!("section: {label}"), *sz, total_size);
    }
    print_region("manifest (tail)", manifest_bytes, total_size);
    print_region("footer", footer_size, total_size);

    let docs: Vec<_> = r.doc_hashes().collect();
    println!("\ndocuments:           {}", docs.len());
    if let Some(blocks) = manifest
        .pointer("/document_store/blocks")
        .and_then(|v| v.as_array())
    {
        let uncompressed: u64 = blocks
            .iter()
            .filter_map(|b| b.get("uncompressed_size").and_then(|v| v.as_u64()))
            .sum();
        if uncompressed > 0 && !docs.is_empty() {
            let avg = uncompressed / docs.len() as u64;
            println!("avg_doc_bytes:       {avg}");
            println!("total_doc_bytes:     {}", fmt_bytes(uncompressed));
        }
    }

    if !view_bytes.is_empty() {
        println!("\ntoken totals:");
        for (id, sz, total_tokens) in &view_bytes {
            let bytes_per_token = if *total_tokens > 0 {
                format!("{:.2}", *sz as f64 / *total_tokens as f64)
            } else {
                "n/a".into()
            };
            println!(
                "  - {id}: {total_tokens} tokens · view_size {} · {bytes_per_token} bytes/token",
                fmt_bytes(*sz)
            );
        }
    }

    if let Some(audit) = manifest.get("audit_log") {
        if let Some(entries) = audit.get("entries").and_then(|v| v.as_array()) {
            println!("\naudit_log_entries:   {}", entries.len());
        }
    }

    Ok(())
}

fn print_region(label: &str, size: u64, total: u64) {
    let pct = if total > 0 {
        size as f64 / total as f64 * 100.0
    } else {
        0.0
    };
    println!("  {:<28}  {:>14}   {:>5.1}%", label, fmt_bytes(size), pct);
}

fn fmt_bytes(n: u64) -> String {
    if n < 1024 {
        return format!("{n} B");
    }
    let units = ["KB", "MB", "GB", "TB"];
    let mut f = n as f64 / 1024.0;
    for (i, u) in units.iter().enumerate() {
        if f < 1024.0 || i == units.len() - 1 {
            return format!("{f:.1} {u}");
        }
        f /= 1024.0;
    }
    format!("{n} B")
}

// ── diff ────────────────────────────────────────────────────────────────

/// Diff two shards: report version, root, and document/view set
/// differences. Exit code is 0 if the shards are identical along every
/// axis we check; non-zero otherwise. Useful for "did re-running my
/// pipeline change anything?" CI checks.
fn cmd_diff(args: &[&str]) -> Result<(), String> {
    let a_path = args.first().ok_or("diff: missing <a.tset>")?;
    let b_path = args.get(1).ok_or("diff: missing <b.tset>")?;
    let a = Reader::open(Path::new(a_path)).map_err(|e| format!("open {a_path}: {e}"))?;
    let b = Reader::open(Path::new(b_path)).map_err(|e| format!("open {b_path}: {e}"))?;

    let mut differences: u32 = 0;

    println!("a: {a_path}");
    println!("b: {b_path}");
    println!();

    // Format version
    let av = (a.header.version_major, a.header.version_minor);
    let bv = (b.header.version_major, b.header.version_minor);
    if av != bv {
        println!("version:    a={}.{}  b={}.{}", av.0, av.1, bv.0, bv.1);
        differences += 1;
    } else {
        println!("version:    {}.{}  (same)", av.0, av.1);
    }

    // shard_merkle_root
    let ar = a.header.shard_merkle_root;
    let br = b.header.shard_merkle_root;
    if ar != br {
        println!("shard_merkle_root differs:");
        println!("  a: {}", hex::encode(ar));
        println!("  b: {}", hex::encode(br));
        differences += 1;
    } else {
        println!("shard_merkle_root:  {}  (same)", hex::encode(ar));
    }

    // smt_root
    let ar = a.smt_root();
    let br = b.smt_root();
    if ar != br {
        println!("smt_root differs:");
        println!("  a: {}", hex::encode(ar));
        println!("  b: {}", hex::encode(br));
        differences += 1;
    } else {
        println!("smt_root:           {}  (same)", hex::encode(ar));
    }

    // Document set
    use std::collections::HashSet;
    let a_docs: HashSet<_> = a.doc_hashes().copied().collect();
    let b_docs: HashSet<_> = b.doc_hashes().copied().collect();
    let only_a: Vec<_> = a_docs.difference(&b_docs).collect();
    let only_b: Vec<_> = b_docs.difference(&a_docs).collect();
    let shared = a_docs.intersection(&b_docs).count();
    println!(
        "\ndocuments:  {} shared, {} only-in-a, {} only-in-b",
        shared,
        only_a.len(),
        only_b.len(),
    );
    if !only_a.is_empty() || !only_b.is_empty() {
        differences += 1;
        let max_show = 10usize;
        for (label, set) in [("only-in-a", &only_a), ("only-in-b", &only_b)] {
            if set.is_empty() {
                continue;
            }
            println!(
                "  {label} ({} docs, showing first {}):",
                set.len(),
                max_show
            );
            for h in set.iter().take(max_show) {
                println!("    {}", hex::encode(h));
            }
        }
    }

    // Tokenizer views
    let a_views = a.tokenizer_ids().map_err(|e| e.to_string())?;
    let b_views = b.tokenizer_ids().map_err(|e| e.to_string())?;
    let a_set: HashSet<&String> = a_views.iter().collect();
    let b_set: HashSet<&String> = b_views.iter().collect();
    let only_a_v: Vec<&String> = a_set.difference(&b_set).copied().collect();
    let only_b_v: Vec<&String> = b_set.difference(&a_set).copied().collect();
    if !only_a_v.is_empty() || !only_b_v.is_empty() {
        println!("\ntokenizer views:");
        for v in only_a_v {
            println!("  only-in-a: {v}");
        }
        for v in only_b_v {
            println!("  only-in-b: {v}");
        }
        differences += 1;
    } else {
        println!("\ntokenizer views: {} (same on both)", a_views.len());
    }

    println!();
    if differences == 0 {
        println!("OK: shards are identical along every checked axis");
        Ok(())
    } else {
        Err(format!(
            "{} difference(s) detected — see report above",
            differences
        ))
    }
}

// ── add-exclusion ───────────────────────────────────────────────────────
//
// Records a GDPR-Article-17-style exclusion in a dataset's overlay.
// Operates on a dataset directory (containing manifest.tset.json +
// shards/ + optionally exclusions.json) — single-shard files have an
// immutable SMT and cannot be retroactively excluded.
//
// The new exclusion is appended to the audit log as a signed
// `exclusion` event (or unsigned, if the original log was unsigned),
// the dataset_merkle_root is recomputed under overlay version 0.3.0
// (which binds the exclusion overlay into the root — issue #4), and
// both manifest.tset.json and exclusions.json are rewritten.

fn cmd_add_exclusion(args: &[&str]) -> Result<(), String> {
    let dataset = args.first().ok_or("add-exclusion: missing <dataset-dir>")?;
    let hex_hash = args.get(1).ok_or("add-exclusion: missing <hex-doc-hash>")?;

    // Optional --reason "..." flag.
    let mut reason = String::new();
    let mut i = 2;
    while i < args.len() {
        let raw = args[i];
        let (flag, inline_value): (&str, Option<&str>) = match raw.find('=') {
            Some(eq) => (&raw[..eq], Some(&raw[eq + 1..])),
            None => (raw, None),
        };
        match flag {
            "--reason" => {
                let value = if let Some(v) = inline_value {
                    v.to_string()
                } else {
                    let v = args.get(i + 1).ok_or("--reason needs a value")?.to_string();
                    i += 1;
                    v
                };
                reason = value;
            }
            other => return Err(format!("unknown flag: {other}")),
        }
        i += 1;
    }

    let path = PathBuf::from(dataset);
    if path.is_file() {
        return Err(
            "add-exclusion: target is a single .tset file; exclusions only apply \
             to dataset directories. Wrap the shard in a dataset directory first."
                .into(),
        );
    }
    if !path.is_dir() {
        return Err(format!("add-exclusion: {dataset} is not a directory"));
    }

    // Decode + validate the hash early so we don't half-write state on a typo.
    let hash_bytes = hex::decode(hex_hash)
        .map_err(|e| format!("add-exclusion: <hex-doc-hash> is not valid hex: {e}"))?;
    if hash_bytes.len() != 32 {
        return Err(format!(
            "add-exclusion: <hex-doc-hash> must decode to 32 bytes, got {}",
            hash_bytes.len()
        ));
    }
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&hash_bytes);

    let mut writer = DatasetWriter::open_existing(&path)
        .map_err(|e| format!("add-exclusion: open dataset: {e}"))?;
    let added = writer.add_exclusion(&hash, &reason);
    writer
        .close()
        .map_err(|e| format!("add-exclusion: close: {e}"))?;

    if added {
        println!("recorded exclusion of {hex_hash}");
        if !reason.is_empty() {
            println!("  reason: {reason}");
        }
    } else {
        println!("no-op: {hex_hash} was already excluded — manifest snapshot regenerated");
    }
    println!("  dataset_merkle_root + exclusions.json + audit log refreshed");
    Ok(())
}

// ── conformance ─────────────────────────────────────────────────────────
//
// Language-agnostic conformance harness. Given a shard + an
// `expected.json` sidecar (the format produced by
// `tests/conformance/build_corpus.py`), opens the shard with the Rust
// reader and asserts every invariant in the sidecar matches.
//
// Use case: third-party TSET implementations (Go, JVM, Swift, …) ship
// their conformance results by pointing this binary at the shards
// they produced + the canonical sidecars. Any drift between
// implementations shows up as a structured diff.
//
// Sidecar shape (subset; see fixture-small.expected.json for full):
//   {
//     "version_minor": 3,
//     "manifest_hash": "<hex>",
//     "manifest_size": <bytes>,
//     "shard_merkle_root": "<hex>",
//     "document_count": <int>,
//     "tokenization_views": {
//       "<id>": {
//         "config_hash": "<hex>",
//         "num_chunks": <int>,
//         "total_tokens": <int>,
//         "vocab_size": <int>,
//       }
//     }
//   }

fn cmd_conformance(args: &[&str]) -> Result<(), String> {
    let shard = args.first().ok_or("conformance: missing <shard.tset>")?;
    let expected_path = args.get(1).ok_or("conformance: missing <expected.json>")?;
    let json_output = args.contains(&"--json");

    let raw =
        std::fs::read(expected_path).map_err(|e| format!("conformance: read expected: {e}"))?;
    let expected: serde_json::Value =
        serde_json::from_slice(&raw).map_err(|e| format!("conformance: parse expected: {e}"))?;

    let r = Reader::open(Path::new(shard)).map_err(|e| e.to_string())?;

    let mut checks: Vec<(String, bool, String, String)> = Vec::new();
    let mut record = |name: &str, ok: bool, got: String, want: String| {
        checks.push((name.to_string(), ok, got, want));
    };

    // version_minor
    if let Some(want) = expected.get("version_minor").and_then(|v| v.as_u64()) {
        let got = r.header.version_minor as u64;
        record(
            "version_minor",
            got == want,
            got.to_string(),
            want.to_string(),
        );
    }
    // manifest_hash
    if let Some(want) = expected.get("manifest_hash").and_then(|v| v.as_str()) {
        let got = hex::encode(r.header.manifest_hash);
        record("manifest_hash", got == want, got, want.to_string());
    }
    // manifest_size
    if let Some(want) = expected.get("manifest_size").and_then(|v| v.as_u64()) {
        let got = r.header.manifest_size;
        record(
            "manifest_size",
            got == want,
            got.to_string(),
            want.to_string(),
        );
    }
    // shard_merkle_root
    if let Some(want) = expected.get("shard_merkle_root").and_then(|v| v.as_str()) {
        let got = hex::encode(r.header.shard_merkle_root);
        record("shard_merkle_root", got == want, got, want.to_string());
    }
    // document_count
    if let Some(want) = expected.get("document_count").and_then(|v| v.as_u64()) {
        let got = r.doc_hashes().count() as u64;
        record(
            "document_count",
            got == want,
            got.to_string(),
            want.to_string(),
        );
    }
    // tokenization_views (per-id)
    if let Some(want_views) = expected
        .get("tokenization_views")
        .and_then(|v| v.as_object())
    {
        let got_ids: std::collections::HashSet<String> = r
            .tokenizer_ids()
            .map_err(|e| e.to_string())?
            .into_iter()
            .collect();
        let want_ids: std::collections::HashSet<String> = want_views.keys().cloned().collect();
        record(
            "tokenization_views.set",
            got_ids == want_ids,
            format!("{:?}", sorted(&got_ids)),
            format!("{:?}", sorted(&want_ids)),
        );
        for (id, want_view) in want_views {
            // total_tokens
            if let Some(want) = want_view.get("total_tokens").and_then(|v| v.as_u64()) {
                match r.view_total_tokens(id) {
                    Ok(got) => record(
                        &format!("views.{id}.total_tokens"),
                        got == want,
                        got.to_string(),
                        want.to_string(),
                    ),
                    Err(e) => record(
                        &format!("views.{id}.total_tokens"),
                        false,
                        format!("error: {e}"),
                        want.to_string(),
                    ),
                }
            }
            // num_chunks + vocab_size + config_hash from manifest JSON
            let view_obj = r
                .manifest()
                .raw()
                .get("tokenization_views")
                .and_then(|v| v.get(id))
                .and_then(|v| v.as_object());
            if let Some(view_obj) = view_obj {
                if let Some(want) = want_view.get("num_chunks").and_then(|v| v.as_u64()) {
                    let got = view_obj
                        .get("chunks")
                        .and_then(|v| v.as_array())
                        .map(|a| a.len() as u64)
                        .unwrap_or(0);
                    record(
                        &format!("views.{id}.num_chunks"),
                        got == want,
                        got.to_string(),
                        want.to_string(),
                    );
                }
                if let Some(want) = want_view.get("vocab_size").and_then(|v| v.as_u64()) {
                    let got = view_obj
                        .get("vocab_size")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
                    record(
                        &format!("views.{id}.vocab_size"),
                        got == want,
                        got.to_string(),
                        want.to_string(),
                    );
                }
                if let Some(want) = want_view.get("config_hash").and_then(|v| v.as_str()) {
                    let got = view_obj
                        .get("config_hash")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    record(
                        &format!("views.{id}.config_hash"),
                        got == want,
                        got,
                        want.to_string(),
                    );
                }
            }
        }
    }

    let total = checks.len();
    let failed: Vec<&(String, bool, String, String)> = checks.iter().filter(|c| !c.1).collect();

    if json_output {
        let out = serde_json::json!({
            "shard": shard,
            "expected": expected_path,
            "total": total,
            "passed": total - failed.len(),
            "failed": failed.len(),
            "checks": checks
                .iter()
                .map(|(name, ok, got, want)| serde_json::json!({
                    "name": name, "ok": ok, "got": got, "want": want,
                }))
                .collect::<Vec<_>>(),
        });
        println!("{}", serde_json::to_string_pretty(&out).unwrap());
    } else {
        println!(
            "conformance: {} checks against {}",
            total,
            std::path::Path::new(expected_path)
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("(?)")
        );
        for (name, ok, got, want) in &checks {
            let mark = if *ok { "PASS" } else { "FAIL" };
            if *ok {
                println!("  [{mark}] {name}");
            } else {
                println!("  [{mark}] {name}: got={got}  want={want}");
            }
        }
        println!();
        println!(
            "{} / {} passed{}",
            total - failed.len(),
            total,
            if failed.is_empty() {
                ""
            } else {
                "  *** FAILED ***"
            }
        );
    }

    if total == 0 {
        // An empty `expected.json` (or one whose only fields are
        // unknown to us) would otherwise pass with "0 / 0 passed".
        // That's a false positive — a third-party impl could ship a
        // blank sidecar and claim conformance. Treat zero checks as
        // a configuration error.
        return Err("expected.json had no recognised fields — \
             nothing was actually checked. Required fields: \
             version_minor, manifest_hash, manifest_size, \
             shard_merkle_root, document_count, tokenization_views"
            .into());
    }

    if failed.is_empty() {
        Ok(())
    } else {
        Err(format!("{} conformance check(s) failed", failed.len()))
    }
}

fn sorted(set: &std::collections::HashSet<String>) -> Vec<&String> {
    let mut v: Vec<&String> = set.iter().collect();
    v.sort();
    v
}
