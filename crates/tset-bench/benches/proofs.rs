//! Benchmark D (compliance queries) + E (exclusion workflow).
//!
//! D — predicate filter cost on a metadata column (proxy for the
//! "find all docs from source X" compliance query).
//! E — end-to-end exclusion workflow: write shard, prove non-inclusion
//! against SMT root, verify the proof.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use tset_core::columns::MetadataColumns;
use tset_core::hashing::hash_bytes;
use tset_core::smt::{Proof, SparseMerkleTree};

fn bench_predicate_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("D_predicate_filter");
    for &n in &[1_000usize, 10_000, 100_000] {
        let mut cols = MetadataColumns::new();
        for i in 0..n {
            let mut row = serde_json::Map::new();
            row.insert("idx".into(), serde_json::json!(i));
            row.insert(
                "lang".into(),
                serde_json::json!(if i % 2 == 0 { "en" } else { "fr" }),
            );
            row.insert("score".into(), serde_json::json!((i % 100) as f64 / 100.0));
            cols.add_row(&row);
        }
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::new("simple_eq", n), &n, |b, _| {
            b.iter(|| cols.filter_sql_like("lang = 'en'").unwrap());
        });
        group.bench_with_input(BenchmarkId::new("compound", n), &n, |b, _| {
            b.iter(|| cols.filter_sql_like("lang = 'en' AND score > 0.5").unwrap());
        });
        group.bench_with_input(BenchmarkId::new("between", n), &n, |b, _| {
            b.iter(|| cols.filter_sql_like("score BETWEEN 0.2 AND 0.8").unwrap());
        });
    }
    group.finish();
}

fn bench_exclusion_proof(c: &mut Criterion) {
    let mut group = c.benchmark_group("E_exclusion_proof");
    for &n in &[1_000usize, 10_000, 100_000] {
        let mut tree = SparseMerkleTree::new();
        for i in 0..n {
            tree.insert(hash_bytes(&i.to_le_bytes()));
        }
        let root = tree.root();
        let absent = hash_bytes(b"never-ingested-doc");
        group.bench_with_input(BenchmarkId::new("prove_then_verify", n), &n, |b, _| {
            b.iter(|| match tree.prove(&absent) {
                Proof::NonInclusion(p) => p.verify(&root),
                _ => false,
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_predicate_filter, bench_exclusion_proof);
criterion_main!(benches);
