use bitcask::db::Db;
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::{env::temp_dir, fs};

fn benchmark_get(db: &mut Db, key: Vec<u8>) -> Option<Vec<u8>> {
    db.get(&key).unwrap()
}

fn populate_db(db: &mut Db, num_entries: usize, value_size: usize) -> Vec<Vec<u8>> {
    let mut keys = Vec::with_capacity(num_entries);
    for i in 0..num_entries {
        let key = format!("key_{:04}", i).as_bytes().to_vec();
        let value = vec![0u8; value_size];
        db.put(&key, &value).unwrap();
        keys.push(key);
    }
    keys
}

fn criterion_benchmark(c: &mut Criterion) {
    let db_path = temp_dir().as_path().join("bitcask_benchmark");
    fs::create_dir_all(&db_path).unwrap();

    {
        const HUNDRED_MEGABYTES: usize = 100 * 1024 * 1024;
        let mut db = Db::new(db_path.clone(), HUNDRED_MEGABYTES).unwrap();

        let keys = populate_db(&mut db, 100_000, 100);

        c.bench_function("get", |b| {
            b.iter_batched(
                || {
                    let mut rng = thread_rng();
                    let key = keys.choose(&mut rng).unwrap().clone();
                    key
                },
                |key| benchmark_get(black_box(&mut db), black_box(key)),
                BatchSize::SmallInput,
            )
        });
    }

    fs::remove_dir_all(db_path).unwrap();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
