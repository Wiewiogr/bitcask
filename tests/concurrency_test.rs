use bitcask::db::Db;
use rand::Rng;
use std::env::temp_dir;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn test_concurrent_put() {
    let num_threads = 8;
    let num_entries_per_thread = 10_000;
    let value_size = 100;

    // Set up a temporary directory for the database
    let temp_dir = temp_dir();
    let db_path = temp_dir
        .as_path()
        .join(format!("bitcask_db_{}", current_timestamp()));

    // Ensure the directory exists
    std::fs::create_dir_all(&db_path).unwrap();

    // Initialize the database
    let db = Arc::new(Db::new(db_path.clone(), 1024).unwrap());

    // Barrier to synchronize thread start
    let barrier = Arc::new(Barrier::new(num_threads));

    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);

        let handle = thread::spawn(move || {
            barrier.wait();

            let mut rng = rand::thread_rng();

            for i in 0..num_entries_per_thread {
                let key = format!("thread_{}_key_{:04}", thread_id, i)
                    .as_bytes()
                    .to_vec();
                let value: Vec<u8> = (0..value_size).map(|_| rng.gen()).collect();

                db.put(&key, &value).unwrap();
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // // Verify the correctness of the data
    let db = db;

    for thread_id in 0..num_threads {
        for i in 0..num_entries_per_thread {
            let key = format!("thread_{}_key_{:04}", thread_id, i)
                .as_bytes()
                .to_vec();
            let value = db.get(&key).unwrap().unwrap();

            assert_eq!(value.len(), value_size);
        }
    }

    fn current_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }
}
