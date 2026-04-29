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
