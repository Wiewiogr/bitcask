use std::cell::{RefCell, RefMut};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::Error;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use super::chunk::Chunk;
use super::db::ValueDetails;

pub struct Chunks {
    chunks: RefCell<BTreeMap<u32, Chunk>>,
    current_file_id: Arc<AtomicU32>,
    data_directory: PathBuf,
    max_file_size: usize,
}

impl Chunks {
    pub fn new(db_path: PathBuf, max_file_size: usize) -> Result<Self, Error> {
        fs::create_dir_all(db_path.clone())?;

        let mut current_file_id = 0;
        let mut chunks = BTreeMap::new();

        let entries = fs::read_dir(&db_path).unwrap();
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                let file_name = path.file_name().unwrap().to_str().unwrap();
                let split_name: Vec<&str> = file_name.split(".").collect();

                if split_name.len() != 2 {
                    println!("Skipping file {:?}, not a db file.", path);
                    continue;
                }

                let file_type = *split_name.get(1).unwrap();
                if file_type.eq("data") {
                    let file_index_str = *split_name.get(0).unwrap();
                    let file_index = file_index_str.parse::<u32>().unwrap();

                    current_file_id = Ord::max(current_file_id, file_index);
                    chunks.insert(
                        file_index,
                        Chunk::from_existing(&path, file_index, max_file_size)?,
                    );
                } else if file_type.eq("index") {
                    todo!()
                } else {
                    println!("Skipping file {:?}, not a db file.", path);
                }
            }
        }

        current_file_id = if chunks.len() == 0 {
            0
        } else {
            current_file_id + 1
        };

        chunks.insert(
            current_file_id,
            Chunk::new(&db_path, current_file_id, max_file_size)?,
        );
        Ok(Chunks {
            chunks: RefCell::new(chunks),
            current_file_id: Arc::new(AtomicU32::new(current_file_id)),
            data_directory: db_path.clone(),
            max_file_size,
        })
    }

    pub fn put(&self, key: &Vec<u8>, value: &Vec<u8>) -> Result<ValueDetails, Error> {
        let mut chunks = self.chunks.borrow_mut();
        let mut current_chunk = chunks.last_key_value().unwrap().1;

        if current_chunk.is_full() {
            self.deprecate_current_chunk(&mut chunks)?;
            current_chunk = chunks.last_key_value().unwrap().1;
        }

        let timestamp = self.current_timestamp();
        return current_chunk.put(key, value, timestamp);
    }

    pub fn get(&self, value_details: &ValueDetails) -> Result<Vec<u8>, Error> {
        let file_id = value_details.file_id;
        let chunks = self.chunks.borrow();
        let chunk = chunks.get(&file_id).unwrap();
        return chunk.get(value_details);
    }

    pub fn delete(&self, key: &Vec<u8>) -> Result<(), Error> {
        let mut chunks = self.chunks.borrow_mut();
        let mut current_chunk = chunks.last_key_value().unwrap().1;

        if current_chunk.is_full() {
            self.deprecate_current_chunk(&mut chunks)?;
            current_chunk = chunks.last_key_value().unwrap().1;
        }

        current_chunk.delete(key)?;
        Ok(())
    }

    pub fn recreate_index_from_old_chunks(
        &mut self,
    ) -> Result<HashMap<Vec<u8>, ValueDetails>, Error> {
        let mut index: HashMap<Vec<u8>, ValueDetails> = HashMap::new();
        for (_, chunk) in self.chunks.borrow_mut().iter_mut() {
            chunk.recreate_index(&mut index)?;
        }
        Ok(index)
    }

    fn deprecate_current_chunk(&self, chunks: &mut RefMut<'_, BTreeMap<u32, Chunk>>) -> Result<(), Error> {
        let previous_file_id = self.current_file_id.fetch_add(1, Ordering::SeqCst);
        let new_file_id = previous_file_id + 1;
        let new_chunk = Chunk::new(&self.data_directory, new_file_id, self.max_file_size)?;
        chunks.insert(new_file_id, new_chunk);
        Ok(())
    }

    fn current_timestamp(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }
}

#[cfg(test)]
mod tests {
    use once_cell::sync::Lazy;

    use super::*;
    use std::env::temp_dir;
    use std::fs;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    const ONE_KB_IN_BYTES: usize = 1024;

    static DB_ID_COUNTER: AtomicU32 = AtomicU32::new(1);

    static SETUP: Lazy<PathBuf> = Lazy::new(|| {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let mut temp_dir_path = temp_dir();
        temp_dir_path.push(format!("chunks_test_{}", current_time));
        let _ = fs::create_dir_all(temp_dir_path.clone());
        temp_dir_path
    });

    fn get_unique_db_path() -> PathBuf {
        let id = DB_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
        let mut base_db_path = SETUP.clone();
        base_db_path.push(format!("db_{}", id));
        base_db_path
    }

    fn count_files_in_directory(path: &PathBuf) -> u32 {
        fs::read_dir(path)
            .map(|entries| {
                entries
                    .filter_map(Result::ok)
                    .filter(|e| e.path().is_file())
                    .count() as u32
            })
            .unwrap_or(0)
    }

    #[test]
    fn test_put_value_to_the_current_chunk() {
        // Given
        let db_path = get_unique_db_path();
        let chunks = Chunks::new(db_path.clone(), ONE_KB_IN_BYTES).unwrap();

        let key = b"example_key".to_vec();
        let value = b"example_value".to_vec();

        // When
        let value_details = chunks.put(&key, &value).unwrap();

        // Then
        assert_eq!(count_files_in_directory(&db_path), 1);
        assert_eq!(value_details.file_id, 0);
    }

    #[test]
    fn test_put_more_than_one_value_in_the_current_chunk() {
        // Given
        let db_path = get_unique_db_path();
        let chunks = Chunks::new(db_path.clone(), ONE_KB_IN_BYTES).unwrap();

        let key1 = b"example_key_1".to_vec();
        let value1 = b"example_value_1".to_vec();
        let key2 = b"example_key_2".to_vec();
        let value2 = b"example_value_2".to_vec();

        // When
        let _ = chunks.put(&key1, &value1);
        let value_details = chunks.put(&key2, &value2).unwrap();

        // Then
        assert_eq!(count_files_in_directory(&db_path), 1);
        assert_eq!(value_details.file_id, 0);
    }

    #[test]
    fn test_put_value_to_the_new_chunk_if_current_is_full() {
        // Given
        let db_path = get_unique_db_path();
        let chunks = Chunks::new(db_path.clone(), ONE_KB_IN_BYTES).unwrap();

        let large_key = b"example_key_large".to_vec();
        let large_value = vec![b'a'; 1024];

        let regular_key = b"example_key_regular".to_vec();
        let regular_value = b"example_value".to_vec();

        // When
        let _ = chunks.put(&large_key, &large_value);
        let value_details = chunks.put(&regular_key, &regular_value).unwrap();

        // Then
        assert_eq!(count_files_in_directory(&db_path), 2);
        assert_eq!(value_details.file_id, 1);
    }

    #[test]
    fn test_put_should_create_more_chunks_as_next_ones_are_becoming_full() {
        // Given
        let db_path = get_unique_db_path();
        let chunks = Chunks::new(db_path.clone(), ONE_KB_IN_BYTES).unwrap();

        let key_1 = b"example_key_1".to_vec();
        let key_2 = b"example_key_2".to_vec();
        let key_3 = b"example_key_3".to_vec();

        let large_value = vec![b'a'; 1024];

        // When
        let _ = chunks.put(&key_1, &large_value);
        let _ = chunks.put(&key_2, &large_value);
        let value_details = chunks.put(&key_3, &large_value).unwrap();

        // Then
        assert_eq!(count_files_in_directory(&db_path), 3);
        assert_eq!(value_details.file_id, 2);
    }

    #[test]
    fn test_delete_value_from_the_current_chunk() {
        // Given
        let db_path = get_unique_db_path();
        let chunks = Chunks::new(db_path.clone(), ONE_KB_IN_BYTES).unwrap();

        let key = b"example_key".to_vec();

        // When
        let _ = chunks.delete(&key);

        // Then
        assert_eq!(count_files_in_directory(&db_path), 1);
    }

    #[test]
    fn test_delete_more_than_one_value_in_the_current_chunk() {
        // Given
        let db_path = get_unique_db_path();
        let chunks = Chunks::new(db_path.clone(), ONE_KB_IN_BYTES).unwrap();

        let key1 = b"example_key_1".to_vec();
        let key2 = b"example_key_2".to_vec();

        // When
        let _ = chunks.delete(&key1);
        let _ = chunks.delete(&key2);

        // Then
        assert_eq!(count_files_in_directory(&db_path), 1);
    }

    #[test]
    fn test_delete_value_in_the_new_chunk_if_current_is_full() {
        // Given
        let db_path = get_unique_db_path();
        let chunks = Chunks::new(db_path.clone(), ONE_KB_IN_BYTES).unwrap();

        let large_key = b"example_key_large".to_vec();
        let large_value = vec![b'a'; 1024];

        let key_to_be_deleted = b"example_key_to_be_deleted".to_vec();

        // When
        let _ = chunks.put(&large_key, &large_value);
        let _ = chunks.delete(&key_to_be_deleted);

        // Then
        assert_eq!(count_files_in_directory(&db_path), 2);
    }

    #[test]
    fn test_get_value_from_the_current_chunk() {
        // Given
        let db_path = get_unique_db_path();
        let chunks = Chunks::new(db_path.clone(), ONE_KB_IN_BYTES).unwrap();

        let key = b"example_key".to_vec();
        let value = b"example_value".to_vec();
        let value_details = chunks.put(&key, &value).unwrap();

        // When
        let result = chunks.get(&value_details).unwrap();

        // Then
        assert_eq!(result, value);
    }

    #[test]
    fn test_get_value_from_the_old_chunk() {
        // Given
        let db_path = get_unique_db_path();
        let chunks = Chunks::new(db_path.clone(), ONE_KB_IN_BYTES).unwrap();

        // Records we are looking for
        let key = b"example_key".to_vec();
        let value = b"example_value".to_vec();

        // Records that should trigger current chunk deprecation
        let any_key_1 = b"any_key_1".to_vec();
        let any_key_2 = b"any_key_2".to_vec();
        let large_value = vec![b'a'; 1024];

        let result_value_details = chunks.put(&key, &value).unwrap();
        let _ = chunks.put(&any_key_1, &large_value);
        let last_message_value_details = chunks.put(&any_key_2, &large_value).unwrap();

        // Verify that our key is in the older file
        assert!(result_value_details.file_id < last_message_value_details.file_id);

        // When
        let result = chunks.get(&result_value_details).unwrap();

        // Then
        assert_eq!(result, value);
    }

    #[test]
    fn test_open_alread_existing_files() {
        // Given
        let db_path = get_unique_db_path();
        let previous_chunks = Chunks::new(db_path.clone(), ONE_KB_IN_BYTES).unwrap();

        // populate
        let key = b"example_key".to_vec();
        let value = b"example_value".to_vec();
        let _ = previous_chunks.put(&key, &value).unwrap();

        // When
        let _ = Chunks::new(db_path.clone(), ONE_KB_IN_BYTES).unwrap();

        // Then
        assert_eq!(count_files_in_directory(&db_path), 2); // should create new chunk
    }

    #[test]
    fn test_read_value_from_existing_files() {
        // Given
        let db_path = get_unique_db_path();
        let previous_chunks = Chunks::new(db_path.clone(), ONE_KB_IN_BYTES).unwrap();

        // populate
        let key = b"example_key".to_vec();
        let value = b"example_value".to_vec();
        let value_details = previous_chunks.put(&key, &value).unwrap();

        // When
        let new_chunks = Chunks::new(db_path.clone(), ONE_KB_IN_BYTES).unwrap();
        let result = new_chunks.get(&value_details).unwrap();

        // Then
        assert_eq!(result, value);
    }

    #[test]
    fn test_put_to_the_new_current_file_when_opened_from_existing_files() {
        // Given
        let db_path = get_unique_db_path();
        let previous_chunks = Chunks::new(db_path.clone(), ONE_KB_IN_BYTES).unwrap();

        // populate
        let key = b"example_key".to_vec();
        let value = b"example_value".to_vec();
        let old_value_details = previous_chunks.put(&key, &value).unwrap();

        let new_key = b"new_key".to_vec();
        let new_value = b"new_value".to_vec();

        // When
        let new_chunks = Chunks::new(db_path.clone(), ONE_KB_IN_BYTES).unwrap();
        let new_value_details = new_chunks.put(&new_key, &new_value).unwrap();

        // Then
        assert_eq!(old_value_details.file_id + 1, new_value_details.file_id);
    }

    #[test]
    fn test_create_index_only_from_the_old_chunks() {
        // Given
        let db_path = get_unique_db_path();
        let mut chunks = Chunks::new(db_path.clone(), ONE_KB_IN_BYTES).unwrap();

        // Records that should trigger current chunk deprecation
        let key_1 = b"key_1".to_vec();
        let large_value_1 = vec![b'a'; 1024];

        // Records we are looking for
        let key_2 = b"key_2".to_vec();
        let value_2 = b"example_value".to_vec();

        let value_details_1 = chunks.put(&key_1, &large_value_1).unwrap();
        let value_details_2 = chunks.put(&key_2, &value_2).unwrap();

        // Verify that keys are placed in different files
        assert!(value_details_1.file_id < value_details_2.file_id);

        // When
        let result = chunks.recreate_index_from_old_chunks().unwrap();

        // Then
        assert_eq!(result.len(), 2);

        assert_eq!(result.contains_key(&key_1), true);
        assert_eq!(*result.get(&key_1).unwrap(), value_details_1);

        assert_eq!(result.contains_key(&key_2), true);
        assert_eq!(*result.get(&key_2).unwrap(), value_details_2);
    }

    #[test]
    fn test_create_index_for_values_in_different_files() {
        // Given
        let db_path = get_unique_db_path();
        let mut chunks = Chunks::new(db_path.clone(), ONE_KB_IN_BYTES).unwrap();

        let key_1 = b"key_1".to_vec();
        let large_value_1 = vec![b'a'; 1024];
        let key_2 = b"key_2".to_vec();
        let large_value_2 = vec![b'a'; 1024];
        let key_3_current_file = b"key_3".to_vec();
        let value_3 = b"example_value".to_vec();

        let value_details_1 = chunks.put(&key_1, &large_value_1).unwrap();
        let value_details_2 = chunks.put(&key_2, &large_value_2).unwrap();
        let value_details_3 = chunks.put(&key_3_current_file, &value_3).unwrap();

        // Verify that keys are placed in different files
        assert!(value_details_1.file_id < value_details_2.file_id);
        assert!(value_details_2.file_id < value_details_3.file_id);

        // When
        let result = chunks.recreate_index_from_old_chunks().unwrap();

        // Then
        assert_eq!(result.len(), 3);

        assert_eq!(result.contains_key(&key_1), true);
        assert_eq!(*result.get(&key_1).unwrap(), value_details_1);

        assert_eq!(result.contains_key(&key_2), true);
        assert_eq!(*result.get(&key_2).unwrap(), value_details_2);

        assert_eq!(result.contains_key(&key_3_current_file), true);
        assert_eq!(*result.get(&key_3_current_file).unwrap(), value_details_3);
    }

    #[test]
    fn test_override_key_in_the_new_file() {
        // Given
        let db_path = get_unique_db_path();
        let mut chunks = Chunks::new(db_path.clone(), ONE_KB_IN_BYTES).unwrap();

        let key = b"key_1".to_vec();
        let large_value_1 = vec![b'a'; 1024];
        let large_value_2 = vec![b'b'; 1024];
        let key_current_file = b"key_3".to_vec();
        let value_current_file = b"example_value".to_vec();

        let value_details_1 = chunks.put(&key, &large_value_1).unwrap();
        let value_details_2 = chunks.put(&key, &large_value_2).unwrap();
        let value_details_3 = chunks.put(&key_current_file, &value_current_file).unwrap();

        // Verify that keys are placed in different files
        assert!(value_details_1.file_id < value_details_2.file_id);
        assert!(value_details_2.file_id < value_details_3.file_id);

        // When
        let result = chunks.recreate_index_from_old_chunks().unwrap();

        // Then
        assert_eq!(result.len(), 2);

        assert_eq!(result.contains_key(&key), true);
        assert_eq!(*result.get(&key).unwrap(), value_details_2);

        assert_eq!(result.contains_key(&key_current_file), true);
        assert_eq!(*result.get(&key_current_file).unwrap(), value_details_3);
    }

    #[test]
    fn test_delete_key_in_the_new_file() {
        // Given
        let db_path = get_unique_db_path();
        let mut chunks = Chunks::new(db_path.clone(), ONE_KB_IN_BYTES).unwrap();

        let key = b"key".to_vec();
        let value = b"example_value".to_vec();

        let key_1 = b"key_1".to_vec();
        let large_value_1 = vec![b'a'; 1024];
        let key_2 = b"key_2".to_vec();
        let large_value_2 = vec![b'a'; 1024];
        let key_current_file = b"key_3".to_vec();
        let value_current_file = b"example_value".to_vec();

        // first file
        chunks.put(&key, &value).unwrap();
        chunks.put(&key_1, &large_value_1).unwrap();

        // second file
        chunks.delete(&key).unwrap();
        chunks.put(&key_2, &large_value_2).unwrap();

        // third and current file
        chunks.put(&key_current_file, &value_current_file).unwrap();

        // When
        let result = chunks.recreate_index_from_old_chunks().unwrap();

        // Then
        assert_eq!(result.contains_key(&key), false);
    }
}

// creates correct index for value deleted in the second file
