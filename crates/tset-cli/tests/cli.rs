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
