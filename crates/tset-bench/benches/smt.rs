//! SMT insert/prove/verify benchmarks (RFC Benchmark E underpinnings).

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use tset_core::hashing::hash_bytes;
use tset_core::smt::{Proof, SparseMerkleTree};

fn bench_smt(c: &mut Criterion) {
    let mut group = c.benchmark_group("smt");
    for &n in &[100usize, 1_000, 10_000] {
        group.bench_with_input(BenchmarkId::new("insert_n", n), &n, |b, &n| {
            b.iter(|| {
                let mut t = SparseMerkleTree::new();
                for i in 0..n {
                    t.insert(hash_bytes(&i.to_le_bytes()));
                }
                t.root()
            });
        });
        // Pre-populated tree, time prove+verify on a present key
        let mut tree = SparseMerkleTree::new();
        for i in 0..n {
            tree.insert(hash_bytes(&i.to_le_bytes()));
        }
        let root = tree.root();
        let key = hash_bytes(&0u64.to_le_bytes());
        group.bench_with_input(BenchmarkId::new("prove_inclusion_n", n), &n, |b, _| {
            b.iter(|| match tree.prove(&key) {
                Proof::Inclusion(p) => p.verify(&root),
                _ => false,
            });
        });
        let absent = hash_bytes(b"not-present-doc");
        group.bench_with_input(BenchmarkId::new("prove_non_inclusion_n", n), &n, |b, _| {
            b.iter(|| match tree.prove(&absent) {
                Proof::NonInclusion(p) => p.verify(&root),
                _ => false,
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_smt);
criterion_main!(benches);
