//! Writer + tokenizer-swap benchmarks (RFC Benchmark C).

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use tset_core::tokenizers::{ByteLevelTokenizer, WhitespaceTokenizer};
use tset_core::Writer;

fn bench_writer(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let mut group = c.benchmark_group("writer");
    for &(n_docs, doc_size) in &[(100usize, 1024usize), (1000, 1024)] {
        let total_bytes = (n_docs * doc_size) as u64;
        group.throughput(Throughput::Bytes(total_bytes));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{n_docs}x{doc_size}")),
            &(n_docs, doc_size),
            |b, &(n, sz)| {
                b.iter_with_setup(
                    || dir.path().join(format!("w-{}-{}-{:p}.tset", n, sz, &n)),
                    |path| {
                        let mut w = Writer::create(&path, None);
                        let body = vec![b'x'; sz];
                        for i in 0..n {
                            let mut c = body.clone();
                            c.extend_from_slice(&i.to_le_bytes());
                            w.add_document(&c).unwrap();
                        }
                        w.add_tokenizer_view(Box::new(ByteLevelTokenizer)).unwrap();
                        w.close().unwrap();
                    },
                );
            },
        );
    }
    group.finish();
}

fn bench_tokenizer_swap(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let mut group = c.benchmark_group("tokenizer_swap");
    for &(n_docs, doc_size) in &[(500usize, 1024usize)] {
        group.throughput(Throughput::Bytes((n_docs * doc_size) as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{n_docs}x{doc_size}_two_views")),
            &(n_docs, doc_size),
            |b, &(n, sz)| {
                b.iter_with_setup(
                    || dir.path().join(format!("t-{}-{}-{:p}.tset", n, sz, &n)),
                    |path| {
                        let mut w = Writer::create(&path, None);
                        let body = vec![b'x'; sz];
                        for i in 0..n {
                            let mut c = body.clone();
                            c.extend_from_slice(&i.to_le_bytes());
                            w.add_document(&c).unwrap();
                        }
                        // Two views — exercises the format's value-prop:
                        // text stored once, tokenized many ways.
                        w.add_tokenizer_view(Box::new(ByteLevelTokenizer)).unwrap();
                        w.add_tokenizer_view(Box::new(WhitespaceTokenizer::new(1024).unwrap()))
                            .unwrap();
                        w.close().unwrap();
                    },
                );
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_writer, bench_tokenizer_swap);
criterion_main!(benches);
