//! `tset` — command-line tool for TSET shards.
//!
//! Subcommands:
//!   tset inspect <path>           summarize header, manifest, views
//!   tset verify <path>            full open + integrity check; exit 0 on pass
//!   tset convert jsonl <src> <dst>   build a TSET shard from a JSONL corpus
//!
//! Intentionally argparse-free (no clap) so the CLI has the same minimal
//! footprint as `tset-core`. Add clap when option surface grows.

use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::ExitCode;

use tset_core::tokenizers::{ByteLevelTokenizer, Tokenizer, WhitespaceTokenizer};
use tset_core::{Reader, Writer};

fn main() -> ExitCode {
    let args: Vec<String> = env::args().collect();
    let cmd = args.get(1).map(String::as_str).unwrap_or("");
    let rest: Vec<&str> = args[2.min(args.len())..].iter().map(String::as_str).collect();

    let result = match cmd {
        "inspect" => cmd_inspect(&rest),
        "verify" => cmd_verify(&rest),
        "convert" => cmd_convert(&rest),
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
    println!("tset {} — open standard for LLM training data\n", env!("CARGO_PKG_VERSION"));
    println!("USAGE:");
    println!("    tset inspect <path>");
    println!("    tset verify <path>");
    println!("    tset convert jsonl <src.jsonl> <dst.tset> [--text-field FIELD] [--tokenizer ID] [--vocab N]");
    println!("    tset version");
}

fn cmd_inspect(args: &[&str]) -> Result<(), String> {
    let path = args.first().ok_or("inspect: missing <path>")?;
    let r = Reader::open(Path::new(path)).map_err(|e| e.to_string())?;
    println!("path:                {path}");
    println!("version:             {}.{}", r.header.version_major, r.header.version_minor);
    if let Some(id) = r.shard_id() {
        println!("shard_id:            {id}");
    }
    println!("shard_merkle_root:   {}", hex::encode(r.header.shard_merkle_root));
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
        other => Err(format!("convert: unsupported format {other:?}; supported: jsonl")),
    }
}

fn convert_jsonl(args: &[&str]) -> Result<(), String> {
    if args.len() < 2 {
        return Err("convert jsonl: usage: <src.jsonl> <dst.tset>".into());
    }
    let src = args[0];
    let dst = args[1];

    // Parse optional flags
    let mut text_field = "text".to_string();
    let mut tokenizer_id = ByteLevelTokenizer::ID.to_string();
    let mut vocab_size: u32 = 0;
    let mut i = 2;
    while i < args.len() {
        match args[i] {
            "--text-field" => {
                text_field = args
                    .get(i + 1)
                    .ok_or("--text-field needs a value")?
                    .to_string();
                i += 2;
            }
            "--tokenizer" => {
                tokenizer_id = args
                    .get(i + 1)
                    .ok_or("--tokenizer needs a value")?
                    .to_string();
                i += 2;
            }
            "--vocab" => {
                vocab_size = args
                    .get(i + 1)
                    .ok_or("--vocab needs a value")?
                    .parse()
                    .map_err(|_| "invalid --vocab".to_string())?;
                i += 2;
            }
            other => return Err(format!("convert jsonl: unknown flag {other:?}")),
        }
    }

    let f = File::open(src).map_err(|e| format!("open {src}: {e}"))?;
    let reader = BufReader::new(f);
    let mut w = Writer::create(dst, None);
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
        w.add_document(text.as_bytes())
            .map_err(|e| e.to_string())?;
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
