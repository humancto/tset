//! End-to-end CLI smoke tests.

use std::io::Write;
use std::process::Command;

fn cli() -> Command {
    Command::new(env!("CARGO_BIN_EXE_tset"))
}

#[test]
fn version_succeeds() {
    let out = cli().arg("version").output().unwrap();
    assert!(out.status.success());
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(s.starts_with("tset "));
}

#[test]
fn unknown_subcommand_fails_with_message() {
    let out = cli().arg("nonsense").output().unwrap();
    assert!(!out.status.success());
    let s = String::from_utf8_lossy(&out.stderr);
    assert!(s.contains("unknown subcommand"));
}

#[test]
fn convert_then_verify_then_inspect_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("in.jsonl");
    let dst = dir.path().join("out.tset");

    let mut f = std::fs::File::create(&src).unwrap();
    writeln!(f, "{{\"text\": \"alpha\"}}").unwrap();
    writeln!(f, "{{\"text\": \"beta gamma\"}}").unwrap();
    drop(f);

    let convert = cli()
        .args(["convert", "jsonl"])
        .arg(&src)
        .arg(&dst)
        .output()
        .unwrap();
    assert!(convert.status.success(), "convert failed: {convert:?}");

    let verify = cli().arg("verify").arg(&dst).output().unwrap();
    assert!(verify.status.success(), "verify failed: {verify:?}");

    let inspect = cli().arg("inspect").arg(&dst).output().unwrap();
    assert!(inspect.status.success());
    let s = String::from_utf8_lossy(&inspect.stdout);
    assert!(s.contains("byte-level-v1"));
    assert!(s.contains("documents:           2"));
}

#[test]
fn verify_missing_file_returns_nonzero() {
    let out = cli().args(["verify", "/no/such/path"]).output().unwrap();
    assert!(!out.status.success());
}

#[test]
fn convert_with_binary_sections_emits_on_disk_sections() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("in.jsonl");
    let dst = dir.path().join("sec.tset");
    std::fs::write(&src, "{\"text\": \"alpha\"}\n{\"text\": \"beta\"}\n").unwrap();
    let out = cli()
        .args(["convert", "jsonl"])
        .arg(&src)
        .arg(&dst)
        .arg("--binary-sections")
        .output()
        .unwrap();
    assert!(out.status.success(), "convert failed: {out:?}");

    // Inspect should now report on_disk_sections
    let inspect = cli().arg("inspect").arg(&dst).output().unwrap();
    let s = String::from_utf8_lossy(&inspect.stdout);
    assert!(
        s.contains("on_disk_sections:    3"),
        "inspect did not report 3 on-disk sections; got:\n{s}"
    );
    assert!(s.contains("smt_section"));
    assert!(s.contains("audit_log_section"));
    assert!(s.contains("metadata_columns_section"));
}

#[test]
fn convert_supports_inline_flag_equals_value() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("in.jsonl");
    let dst = dir.path().join("ws.tset");
    std::fs::write(&src, "{\"text\": \"alpha beta gamma\"}\n").unwrap();
    let out = cli()
        .args(["convert", "jsonl"])
        .arg(&src)
        .arg(&dst)
        .args(["--tokenizer=whitespace-hashed-v1", "--vocab=512"])
        .output()
        .unwrap();
    assert!(out.status.success(), "convert failed: {out:?}");
    let inspect = cli().arg("inspect").arg(&dst).output().unwrap();
    let s = String::from_utf8_lossy(&inspect.stdout);
    assert!(s.contains("whitespace-hashed-v1"));
}

// ── stats ───────────────────────────────────────────────────────────────

fn make_corpus(dir: &std::path::Path, name: &str, payload: &str) -> std::path::PathBuf {
    let src = dir.join(format!("{name}.jsonl"));
    let dst = dir.join(format!("{name}.tset"));
    std::fs::write(&src, payload).unwrap();
    let out = cli()
        .args(["convert", "jsonl"])
        .arg(&src)
        .arg(&dst)
        .output()
        .unwrap();
    assert!(out.status.success(), "convert failed: {out:?}");
    dst
}

#[test]
fn stats_reports_region_breakdown_and_token_totals() {
    let dir = tempfile::tempdir().unwrap();
    let dst = make_corpus(
        dir.path(),
        "stats",
        "{\"text\": \"alpha\"}\n{\"text\": \"beta gamma\"}\n",
    );
    let out = cli().arg("stats").arg(&dst).output().unwrap();
    assert!(out.status.success(), "stats failed: {out:?}");
    let s = String::from_utf8_lossy(&out.stdout);
    // The four headline regions every shard always has
    assert!(s.contains("region breakdown:"));
    assert!(s.contains("header"));
    assert!(s.contains("doc store"));
    assert!(s.contains("manifest (tail)"));
    assert!(s.contains("footer"));
    // Plus byte-level view + token totals
    assert!(s.contains("view: byte-level-v1"));
    assert!(s.contains("token totals:"));
    assert!(s.contains("byte-level-v1:"));
    assert!(s.contains("documents:           2"));
}

#[test]
fn stats_missing_file_returns_nonzero() {
    let out = cli().args(["stats", "/no/such"]).output().unwrap();
    assert!(!out.status.success());
}

// ── diff ────────────────────────────────────────────────────────────────

#[test]
fn diff_identical_shards_exits_zero() {
    // Same source bytes deterministically produce the same shard
    // (modulo the audit log timestamps, which are wall-clock by
    // default — so we use deterministic env to make the test stable).
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("same.jsonl");
    std::fs::write(&src, "{\"text\": \"alpha\"}\n").unwrap();

    let mut a = cli();
    a.env("TSET_DETERMINISTIC_CREATED_AT", "2026-01-01T00:00:00+00:00")
        .env("TSET_DETERMINISTIC_SNAPSHOT_ID", "fixed")
        .env("TSET_DETERMINISTIC_TIME", "1735689600.0");
    a.args(["convert", "jsonl"])
        .arg(&src)
        .arg(dir.path().join("a.tset"))
        .output()
        .unwrap();

    let mut b = cli();
    b.env("TSET_DETERMINISTIC_CREATED_AT", "2026-01-01T00:00:00+00:00")
        .env("TSET_DETERMINISTIC_SNAPSHOT_ID", "fixed")
        .env("TSET_DETERMINISTIC_TIME", "1735689600.0");
    b.args(["convert", "jsonl"])
        .arg(&src)
        .arg(dir.path().join("b.tset"))
        .output()
        .unwrap();

    // Both shards should at minimum agree on shard_merkle_root and
    // doc set; the wall-clock fields drive the diff exit code via the
    // smt_root / shard_root checks specifically. Both impls put the
    // same content under the same hash.
    let out = cli()
        .arg("diff")
        .arg(dir.path().join("a.tset"))
        .arg(dir.path().join("b.tset"))
        .output()
        .unwrap();
    // The exit-code contract is the whole point of the test: identical
    // shards MUST produce a zero exit code so CI checks like "did
    // re-running the pipeline change anything?" can rely on it.
    assert!(
        out.status.success(),
        "expected zero exit on identical shards;\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("shard_merkle_root"));
    assert!(stdout.contains("OK: shards are identical along every checked axis"));
}

#[test]
fn diff_detects_extra_view() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("in.jsonl");
    std::fs::write(&src, "{\"text\": \"alpha beta\"}\n").unwrap();
    let a = dir.path().join("a.tset");
    let b = dir.path().join("b.tset");
    cli()
        .args(["convert", "jsonl"])
        .arg(&src)
        .arg(&a)
        .output()
        .unwrap();
    cli()
        .args(["convert", "jsonl"])
        .arg(&src)
        .arg(&b)
        .args(["--tokenizer=whitespace-hashed-v1", "--vocab=512"])
        .output()
        .unwrap();

    let out = cli().arg("diff").arg(&a).arg(&b).output().unwrap();
    // Different views → non-zero exit
    assert!(!out.status.success());
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(
        s.contains("only-in-a: byte-level-v1") || s.contains("only-in-b: whitespace-hashed-v1")
    );
}

#[test]
fn diff_detects_different_doc_set() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.jsonl"), "{\"text\": \"alpha\"}\n").unwrap();
    std::fs::write(dir.path().join("b.jsonl"), "{\"text\": \"beta\"}\n").unwrap();
    let a = make_corpus(dir.path(), "a", "{\"text\": \"alpha\"}\n");
    let b = make_corpus(dir.path(), "b", "{\"text\": \"beta\"}\n");
    let out = cli().arg("diff").arg(&a).arg(&b).output().unwrap();
    assert!(!out.status.success());
    let s = String::from_utf8_lossy(&out.stdout);
    // 0 shared, 1 only-in-a, 1 only-in-b
    assert!(s.contains("0 shared"));
    assert!(s.contains("only-in-a") && s.contains("only-in-b"));
}

// ── add-exclusion ───────────────────────────────────────────────────────

/// Build a minimal dataset directory with a single shard registered
/// via `DatasetWriter`, returning (root_path, doc_hash_hex_of_first_doc).
fn make_dataset(dir: &std::path::Path) -> (std::path::PathBuf, String) {
    use tset_core::dataset::DatasetWriter;
    use tset_core::tokenizers::ByteLevelTokenizer;
    use tset_core::Writer;

    let root = dir.join("ds");
    std::fs::create_dir_all(root.join("shards")).unwrap();
    let mut w = Writer::create(root.join("shards/part-00001.tset"), None);
    let h = w.add_document(b"alpha").unwrap();
    w.add_document(b"beta").unwrap();
    w.add_tokenizer_view(Box::new(ByteLevelTokenizer)).unwrap();
    w.close().unwrap();

    let mut dw = DatasetWriter::create(&root).unwrap();
    dw.register_shard("part-00001").unwrap();
    dw.close().unwrap();

    (root, hex::encode(h))
}

#[test]
fn add_exclusion_records_hash_in_overlay() {
    let dir = tempfile::tempdir().unwrap();
    let (root, hex) = make_dataset(dir.path());
    let out = cli()
        .arg("add-exclusion")
        .arg(&root)
        .arg(&hex)
        .args(["--reason", "GDPR-Art-17 request 2026-04-29"])
        .output()
        .unwrap();
    assert!(out.status.success(), "add-exclusion failed: {out:?}");
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(s.contains(&format!("recorded exclusion of {hex}")));
    assert!(s.contains("reason: GDPR-Art-17"));

    let excl = std::fs::read_to_string(root.join("exclusions.json")).unwrap();
    assert!(excl.contains(&hex));
    let manifest = std::fs::read_to_string(root.join("manifest.tset.json")).unwrap();
    assert!(manifest.contains("\"exclusion_count\": 1"));
}

#[test]
fn add_exclusion_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let (root, hex) = make_dataset(dir.path());
    cli()
        .arg("add-exclusion")
        .arg(&root)
        .arg(&hex)
        .output()
        .unwrap();
    let out = cli()
        .arg("add-exclusion")
        .arg(&root)
        .arg(&hex)
        .output()
        .unwrap();
    assert!(out.status.success());
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(s.contains("no-op"), "expected no-op message, got:\n{s}");

    let excl = std::fs::read_to_string(root.join("exclusions.json")).unwrap();
    // Hash appears exactly once
    assert_eq!(excl.matches(&hex).count(), 1);
}

#[test]
fn add_exclusion_rejects_single_shard_file() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("in.jsonl");
    let dst = dir.path().join("out.tset");
    std::fs::write(&src, "{\"text\": \"alpha\"}\n").unwrap();
    cli()
        .args(["convert", "jsonl"])
        .arg(&src)
        .arg(&dst)
        .output()
        .unwrap();
    let out = cli()
        .arg("add-exclusion")
        .arg(&dst)
        .arg("ab".repeat(32))
        .output()
        .unwrap();
    assert!(!out.status.success());
    let s = String::from_utf8_lossy(&out.stderr);
    assert!(
        s.contains("single .tset file"),
        "expected single-file rejection, got:\n{s}"
    );
}

#[test]
fn add_exclusion_rejects_invalid_hex() {
    let dir = tempfile::tempdir().unwrap();
    let (root, _) = make_dataset(dir.path());
    let out = cli()
        .arg("add-exclusion")
        .arg(&root)
        .arg("not-hex")
        .output()
        .unwrap();
    assert!(!out.status.success());
    let s = String::from_utf8_lossy(&out.stderr);
    assert!(s.contains("not valid hex"));
}

// ── conformance ─────────────────────────────────────────────────────────

#[test]
fn conformance_passes_against_committed_fixture() {
    // Fixture lives at the repo level; tests run from the workspace
    // root (target/debug/deps), so navigate up.
    let fixtures = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("tests")
        .join("conformance")
        .join("fixtures");
    let shard = fixtures.join("fixture-small.tset");
    let expected = fixtures.join("fixture-small.expected.json");
    if !shard.exists() {
        eprintln!("skipping: fixture-small.tset not present");
        return;
    }
    let out = cli()
        .arg("conformance")
        .arg(&shard)
        .arg(&expected)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "conformance failed:\n{:?}\n{}\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(s.contains("14 / 14 passed"));
}

#[test]
fn conformance_fails_on_mismatched_fixture() {
    let fixtures = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("tests")
        .join("conformance")
        .join("fixtures");
    let shard = fixtures.join("fixture-empty.tset");
    let expected = fixtures.join("fixture-small.expected.json");
    if !shard.exists() || !expected.exists() {
        eprintln!("skipping: fixtures not present");
        return;
    }
    let out = cli()
        .arg("conformance")
        .arg(&shard)
        .arg(&expected)
        .output()
        .unwrap();
    // Non-zero exit on any mismatch is the contract third-party
    // implementations rely on to detect drift in CI.
    assert!(!out.status.success());
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(s.contains("FAIL"));
    assert!(s.contains("manifest_hash"));
}

#[test]
fn conformance_json_output_is_parseable() {
    let fixtures = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("tests")
        .join("conformance")
        .join("fixtures");
    let shard = fixtures.join("fixture-small.tset");
    let expected = fixtures.join("fixture-small.expected.json");
    if !shard.exists() {
        eprintln!("skipping: fixture-small.tset not present");
        return;
    }
    let out = cli()
        .arg("conformance")
        .arg(&shard)
        .arg(&expected)
        .arg("--json")
        .output()
        .unwrap();
    assert!(out.status.success());
    let v: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("--json output must be valid JSON");
    assert_eq!(v["passed"], v["total"]);
    assert_eq!(v["failed"], 0);
    assert!(v["checks"].as_array().unwrap().len() >= 14);
}

#[test]
fn conformance_rejects_empty_expected_sidecar() {
    // Codex P2 on PR #16. An empty (or partially-malformed) sidecar
    // would otherwise produce "0 / 0 passed" + exit 0 — a third-party
    // implementation could ship a blank sidecar and claim
    // conformance. Treat zero recognised checks as a config error.
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("in.jsonl");
    let dst = dir.path().join("out.tset");
    std::fs::write(&src, "{\"text\": \"alpha\"}\n").unwrap();
    cli()
        .args(["convert", "jsonl"])
        .arg(&src)
        .arg(&dst)
        .output()
        .unwrap();
    let blank = dir.path().join("blank.json");
    std::fs::write(&blank, "{}").unwrap();
    let out = cli()
        .arg("conformance")
        .arg(&dst)
        .arg(&blank)
        .output()
        .unwrap();
    assert!(
        !out.status.success(),
        "conformance must reject an empty sidecar"
    );
    let s = String::from_utf8_lossy(&out.stderr);
    assert!(
        s.contains("no recognised fields"),
        "stderr should explain the rejection, got:\n{s}"
    );
}
