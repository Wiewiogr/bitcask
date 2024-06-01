use std::io::Error;
use std::path::PathBuf;

use super::chunks::Chunks;
use super::index::Index;

pub struct Db {
    chunks: Chunks,
    index: Index,
}

impl Db {
    fn new(db_path: PathBuf, max_file_size: usize) -> Result<Self, Error> {
        let mut chunks = Chunks::new(db_path, max_file_size)?;
        let index_map = chunks.recreate_index_from_old_chunks()?;
        let index = Index::new(index_map);

        Ok(Db { chunks, index })
    }

    pub fn put(&mut self, key: &Vec<u8>, value: &Vec<u8>) -> Result<(), Error> {
        let value_details = self.chunks.put(key, value)?;
        self.index.put(key.clone(), value_details);
        Ok(())
    }

    pub fn get(&mut self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, Error> {
        let value_details = self.index.get(key);
        match value_details {
            Some(details) => Ok(Some(self.chunks.get(&details)?)),
            None => Ok(None),
        }
    }

    pub fn delete(&mut self, key: &Vec<u8>) -> Result<(), Error> {
        self.chunks.delete(key)?;
        self.index.delete(key);
        Ok(())
    }

    pub fn merge() {
        println!("merging");
    }
}

#[derive(Debug, PartialEq)]
pub struct ValueDetails {
    pub file_id: u32,
    pub value_size: u32,
    pub value_pos: u32,
    pub timestamp: u64,
}

#[cfg(test)]
mod tests {
    use std::{
        env::temp_dir,
        fs,
        sync::atomic::{AtomicU32, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use once_cell::sync::Lazy;

    use super::*;

    const ONE_KB_IN_BYTES: usize = 1024;

    static DB_ID_COUNTER: AtomicU32 = AtomicU32::new(1);

    static SETUP: Lazy<PathBuf> = Lazy::new(|| {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let mut temp_dir_path = temp_dir();
        temp_dir_path.push(format!("db_test_{}", current_time));
        let _ = fs::create_dir_all(temp_dir_path.clone());
        temp_dir_path
    });

    fn get_unique_db_path() -> PathBuf {
        let id = DB_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
        let mut base_db_path = SETUP.clone();
        base_db_path.push(format!("db_{}", id));
        base_db_path
    }

    #[test]
    fn should_contain_added_element() {
        let db_path = get_unique_db_path();
        let mut db = Db::new(db_path, ONE_KB_IN_BYTES).unwrap();

        let key = b"key".to_vec();
        let _ = db.put(&key, &b"value".to_vec());
        let result = db.get(&key).unwrap();

        match result {
            Some(name) => assert_eq!(name, b"value".to_vec()),
            None => panic!("Expected \"value\" but got None"),
        }
    }

    #[test]
    fn should_not_contain_removed_element() {
        let db_path = get_unique_db_path();
        let mut db = Db::new(db_path, ONE_KB_IN_BYTES).unwrap();

        let key = b"key".to_vec();
        let _ = db.put(&key, &b"value".to_vec());
        let _ = db.delete(&key);
        let result = db.get(&key).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn should_return_null_if_key_do_not_exists() {
        let db_path = get_unique_db_path();
        let mut db = Db::new(db_path, ONE_KB_IN_BYTES).unwrap();

        let key = b"key".to_vec();
        let result = db.get(&key).unwrap();
        assert_eq!(result, None);
    }
}
