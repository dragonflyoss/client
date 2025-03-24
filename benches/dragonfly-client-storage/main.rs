use criterion::criterion_group;
use criterion::criterion_main;

mod cache;

criterion_group!(benches, cache::bench_cache_operations);
criterion_main!(benches); 