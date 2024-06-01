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
    fn test_should_contain_added_element() {
        // Given
        let db_path = get_unique_db_path();
        let mut db = Db::new(db_path, ONE_KB_IN_BYTES).unwrap();

        let key = b"key".to_vec();
        let value = b"value".to_vec();

        // When
        let _ = db.put(&key, &value);
        let result = db.get(&key).unwrap();

        // Then
        match result {
            Some(name) => assert_eq!(name, b"value".to_vec()),
            None => panic!("Expected \"value\" but got None"),
        }
    }

    #[test]
    fn test_should_not_contain_removed_element() {
        // Given
        let db_path = get_unique_db_path();
        let mut db = Db::new(db_path, ONE_KB_IN_BYTES).unwrap();

        let key = b"key".to_vec();
        let value = b"value".to_vec();

        // When
        let _ = db.put(&key, &value);
        let _ = db.delete(&key);
        let result = db.get(&key).unwrap();

        // Then
        assert_eq!(result, None);
    }

    #[test]
    fn test_return_null_if_key_do_not_exists() {
        // Given
        let db_path = get_unique_db_path();
        let mut db = Db::new(db_path, ONE_KB_IN_BYTES).unwrap();

        let key = b"key".to_vec();

        // When
        let result = db.get(&key).unwrap();

        // Then
        assert_eq!(result, None);
    }

    #[test]
    fn test_should_recreate_db_from_existing_files() {
        // Given
        let db_path = get_unique_db_path();
        let mut db_original = Db::new(db_path.clone(), ONE_KB_IN_BYTES).unwrap();

        let key_1 = b"key_1".to_vec();
        let value_1 = b"value_1".to_vec();

        let key_2_deleted = b"key_2_deleted".to_vec();
        let value_2_deleted = b"value_2_deleted".to_vec();

        let key_3_overriden = b"key_3_overriden".to_vec();
        let value_3_old = b"value_3_old".to_vec();
        let value_3_new = b"value_3_new".to_vec();

        let large_key_1 = b"large_key_1".to_vec();
        let large_key_2 = b"large_key_2".to_vec();
        let large_value = vec![b'a'; 1024];

        // Database initialization
        let _ = db_original.put(&key_1, &value_1);
        let _ = db_original.put(&key_2_deleted, &value_2_deleted);
        let _ = db_original.put(&key_3_overriden, &value_3_old);
        let _ = db_original.put(&large_key_1, &large_value);

        let _ = db_original.delete(&key_2_deleted);
        let _ = db_original.put(&key_3_overriden, &value_3_new);
        let _ = db_original.put(&large_key_2, &large_value);

        // Create new db instance
        let mut db_recreated = Db::new(db_path.clone(), ONE_KB_IN_BYTES).unwrap();

        // Query data
        let result_1_regular = db_recreated.get(&key_1).unwrap();
        let result_2_deleted = db_recreated.get(&key_2_deleted).unwrap();
        let result_3_overriden = db_recreated.get(&key_3_overriden).unwrap();

        // Verify if data is recreated correctly
        assert_eq!(result_1_regular.unwrap(), value_1);
        assert_eq!(result_2_deleted.is_none(), true);
        assert_eq!(result_3_overriden.unwrap(), value_3_new);
    }
}
