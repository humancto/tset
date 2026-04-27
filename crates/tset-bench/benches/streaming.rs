//! Benchmark B (streaming throughput) — Rust-only.
//!
//! Measures how fast Reader::open + view.iter_per_doc consume a freshly
//! written shard. Run with:
//!
//!   cargo bench -p tset-bench --bench streaming

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use tset_core::tokenizers::ByteLevelTokenizer;
use tset_core::{Reader, Writer};

fn write_shard(path: &std::path::Path, n_docs: usize, doc_size: usize) -> u64 {
    let mut w = Writer::create(path, None);
    let mut total_bytes = 0u64;
    let body = vec![b'a'; doc_size];
    for i in 0..n_docs {
        let mut content = body.clone();
        // Make each doc unique so dedup doesn't fold them.
        content.extend_from_slice(&i.to_le_bytes());
        total_bytes += content.len() as u64;
        w.add_document(&content).unwrap();
    }
    w.add_tokenizer_view(Box::new(ByteLevelTokenizer)).unwrap();
    w.close().unwrap();
    total_bytes
}

fn bench_streaming(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let mut group = c.benchmark_group("streaming");
    for &(n_docs, doc_size) in &[(100usize, 1024usize), (1000, 1024), (1000, 4096)] {
        let path = dir.path().join(format!("s-{}-{}.tset", n_docs, doc_size));
        let total = write_shard(&path, n_docs, doc_size);
        group.throughput(Throughput::Bytes(total));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{n_docs}x{doc_size}")),
            &path,
            |b, path| {
                b.iter(|| {
                    let r = Reader::open(path).unwrap();
                    let view = r.open_view("byte-level-v1").unwrap();
                    let mut tot = 0u64;
                    for (tokens, _hash) in view.iter_per_doc().unwrap() {
                        tot += tokens.len() as u64;
                    }
                    tot
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_streaming);
criterion_main!(benches);
