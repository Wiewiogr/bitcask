use std::{
    collections::HashMap,
    fs::File,
    io::{Error, Read, Seek, SeekFrom, Write},
    path::PathBuf,
};

use memmap2::Mmap;

use super::db::ValueDetails;

pub struct Chunk {
    file: File,
    file_id: u32,
    max_file_size: usize,
    current_size: usize,
}

impl Chunk {
    const TOMBSTONE_TIMESTAMP: u64 = 0;
    const TOMBSTONE_VALUE: Vec<u8> = Vec::new();

    pub fn from_existing(
        path: &PathBuf,
        file_id: u32,
        max_file_size: usize,
    ) -> Result<Self, Error> {
        let file = match File::options().read(true).write(false).open(path) {
            Ok(file) => file,
            Err(error) => panic!("Problem opening the file: {:?}", error),
        };
        Ok(Chunk {
            file,
            file_id,
            max_file_size,
            current_size: 0,
        })
    }

    pub fn new(base_path: &PathBuf, file_id: u32, max_file_size: usize) -> Result<Self, Error> {
        let path = base_path.join(format!("{}.data", file_id));
        let file = match File::create_new(path) {
            Ok(file) => file,
            Err(error) => panic!("Problem creating the file: {:?}", error),
        };
        Ok(Chunk {
            file,
            file_id,
            max_file_size,
            current_size: 0,
        })
    }

    pub fn is_full(&mut self) -> bool {
        self.current_size >= self.max_file_size
    }

    pub fn put(
        &mut self,
        key: &Vec<u8>,
        value: &Vec<u8>,
        timestamp: u64,
    ) -> Result<ValueDetails, Error> {
        let value_size = value.len() as u32;

        let record_bytes = self.prepare_record_bytes(key, value, timestamp);

        self.file.write(&record_bytes)?;

        let current_position = self.file.stream_position().unwrap();
        self.current_size = current_position as usize;
        let value_pos = current_position as u32 - value_size;

        Ok(ValueDetails {
            file_id: self.file_id,
            value_size,
            value_pos,
            timestamp,
        })
    }

    pub fn get(&mut self, value_details: &ValueDetails) -> Result<Vec<u8>, Error> {
        let mut value: Vec<u8> = vec![0; value_details.value_size as usize];
        self.file
            .seek(SeekFrom::Start(value_details.value_pos as u64))?;
        self.file.read_exact(&mut value)?;
        Ok(value)
    }

    pub fn delete(&mut self, key: &Vec<u8>) -> Result<(), Error> {
        let record_bytes =
            self.prepare_record_bytes(key, &Chunk::TOMBSTONE_VALUE, Chunk::TOMBSTONE_TIMESTAMP);

        self.file.write(&record_bytes)?;
        self.file.flush()?;
        Ok(())
    }

    pub fn recreate_index(
        &mut self,
        index: &mut HashMap<Vec<u8>, ValueDetails>,
    ) -> Result<(), Error> {
        let mmap = unsafe { Mmap::map(&self.file)? };
        let file_len = mmap.len();
        let mut cursor = 0;
        while cursor < file_len {
            let _checksum = self.read_u32(&mmap, cursor)?; // 4 bytes
            let timestamp = self.read_u64(&mmap, cursor + 4)?; // 8 bytes
            let key_size = self.read_u32(&mmap, cursor + 12)?; // 4 bytes
            let value_size = self.read_u32(&mmap, cursor + 16)?; // 4 bytes
            cursor += 20;

            let key = self.read_vec_u8(&mmap, cursor, key_size as usize)?;
            cursor += key_size as usize;

            if timestamp == Chunk::TOMBSTONE_TIMESTAMP {
                index.remove(&key);
            } else {
                let value_details = ValueDetails {
                    file_id: self.file_id,
                    value_size,
                    value_pos: cursor as u32,
                    timestamp,
                };
                index.insert(key, value_details);
            }
            cursor += value_size as usize;
        }
        Ok(())
    }

    fn read_u32(&mut self, mmap: &Mmap, cursor: usize) -> Result<u32, Error> {
        let bytes = mmap.get(cursor..cursor + 4).unwrap();
        Ok(u32::from_be_bytes(bytes.try_into().unwrap()))
    }

    fn read_u64(&mut self, mmap: &Mmap, cursor: usize) -> Result<u64, Error> {
        let bytes = mmap.get(cursor..cursor + 8).unwrap();
        Ok(u64::from_be_bytes(bytes.try_into().unwrap()))
    }

    fn read_vec_u8(&mut self, mmap: &Mmap, cursor: usize, size: usize) -> Result<Vec<u8>, Error> {
        let bytes = mmap.get(cursor..cursor + size).unwrap();
        Ok(bytes.to_vec())
    }

    fn prepare_record_bytes(&mut self, key: &Vec<u8>, value: &Vec<u8>, timestamp: u64) -> Vec<u8> {
        let key_size = key.len() as u32;
        let value_size = value.len() as u32;

        let mut record: Vec<u8> = Vec::new();
        record.extend_from_slice(&timestamp.to_be_bytes().to_vec());
        record.extend_from_slice(&key_size.to_be_bytes().to_vec());
        record.extend_from_slice(&value_size.to_be_bytes().to_vec());
        record.extend_from_slice(&key);
        record.extend_from_slice(&value);

        let checksum = crc32fast::hash(&record);

        let mut record_with_crc: Vec<u8> = Vec::new();
        record_with_crc.extend_from_slice(&checksum.to_be_bytes().to_vec());
        record_with_crc.extend_from_slice(&record);
        record_with_crc
    }
}

#[cfg(test)]
mod tests {
    use once_cell::sync::Lazy;

    use super::*;
    use std::env::temp_dir;
    use std::fs::{self, OpenOptions};
    use std::io::Read;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    const ONE_KB_IN_BYTES: usize = 1024;

    static FILE_ID_COUNTER: AtomicU32 = AtomicU32::new(1);

    static SETUP: Lazy<PathBuf> = Lazy::new(|| {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let mut temp_dir_path = temp_dir();
        temp_dir_path.push(format!("bitcask_test_{}", current_time));
        let _ = fs::create_dir_all(temp_dir_path.clone());
        temp_dir_path
    });

    fn get_unique_file_id() -> u32 {
        FILE_ID_COUNTER.fetch_add(1, Ordering::SeqCst)
    }

    #[test]
    fn test_put() {
        // Create a new chunk
        let file_id = get_unique_file_id();
        let mut chunk = Chunk::new(&SETUP, file_id, ONE_KB_IN_BYTES).unwrap();

        // Define key, value and timestamp
        let key = b"example_key".to_vec();
        let value = b"example_value".to_vec();
        let timestamp: u64 = 1620867414000;

        // Use put method
        let _ = chunk.put(&key, &value, timestamp);

        // Open the file to read its contents
        let file_path = SETUP.join(format!("{}.data", file_id));
        let mut file = OpenOptions::new().read(true).open(file_path).unwrap();

        // Read the contents of the file
        let mut file_contents = Vec::new();
        file.read_to_end(&mut file_contents).unwrap();

        // Expected record
        let key_size: u32 = key.len() as u32;
        let value_size: u32 = value.len() as u32;
        let mut record: Vec<u8> = Vec::new();
        record.extend_from_slice(&timestamp.to_be_bytes().to_vec());
        record.extend_from_slice(&key_size.to_be_bytes().to_vec());
        record.extend_from_slice(&value_size.to_be_bytes().to_vec());
        record.extend_from_slice(&key);
        record.extend_from_slice(&value);

        let checksum = crc32fast::hash(&record);

        let mut expected_content: Vec<u8> = Vec::new();
        expected_content.extend_from_slice(&checksum.to_be_bytes().to_vec());
        expected_content.extend_from_slice(&record);

        // Assert that the file contents match the expected contents
        assert_eq!(file_contents, expected_content);
    }

    #[test]
    fn test_delete() {
        // Create a new chunk
        let file_id = get_unique_file_id();
        let mut chunk = Chunk::new(&SETUP, file_id, ONE_KB_IN_BYTES).unwrap();

        // Define key, value and timestamp
        let key = b"example_key".to_vec();

        // Use delete method
        chunk.delete(&key);

        // Open the file to read its contents
        let file_path = SETUP.join(format!("{}.data", file_id));
        let mut file = OpenOptions::new().read(true).open(file_path).unwrap();

        // Read the contents of the file
        let mut file_contents = Vec::new();
        file.read_to_end(&mut file_contents).unwrap();

        // Expected record
        let key_size: u32 = key.len() as u32;
        let value = Chunk::TOMBSTONE_VALUE;
        let value_size: u32 = value.len() as u32;
        let mut record: Vec<u8> = Vec::new();
        record.extend_from_slice(&Chunk::TOMBSTONE_TIMESTAMP.to_be_bytes().to_vec());
        record.extend_from_slice(&key_size.to_be_bytes().to_vec());
        record.extend_from_slice(&value_size.to_be_bytes().to_vec());
        record.extend_from_slice(&key);
        record.extend_from_slice(&value);

        let checksum = crc32fast::hash(&record);

        let mut expected_content: Vec<u8> = Vec::new();
        expected_content.extend_from_slice(&checksum.to_be_bytes().to_vec());
        expected_content.extend_from_slice(&record);

        // Assert that the file contents match the expected contents
        assert_eq!(file_contents, expected_content);
    }

    #[test]
    fn test_get_after_put() {
        // Given
        let file_id = get_unique_file_id();
        let mut chunk = Chunk::new(&SETUP, file_id, ONE_KB_IN_BYTES).unwrap();

        let key = b"example_key".to_vec();
        let value = b"example_value".to_vec();
        let timestamp: u64 = 1620867414000;

        // When
        let value_details = chunk.put(&key, &value, timestamp).unwrap();
        let result = chunk.get(&value_details).unwrap();

        // Then
        assert_eq!(result, value);
    }

    #[test]
    fn test_empty_file_is_not_empty() {
        // Given
        let file_id = get_unique_file_id();
        let mut chunk = Chunk::new(&SETUP, file_id, ONE_KB_IN_BYTES).unwrap();

        // When
        let is_full = chunk.is_full();

        // Then
        assert_eq!(is_full, false);
    }

    #[test]
    fn test_is_not_full_if_there_is_still_place() {
        // Given
        let file_id = get_unique_file_id();
        let mut chunk = Chunk::new(&SETUP, file_id, ONE_KB_IN_BYTES).unwrap();

        let key = b"example_key".to_vec();
        let value = b"example_value".to_vec();
        let timestamp: u64 = 1620867414000;
        let _ = chunk.put(&key, &value, timestamp);

        // When
        let is_full = chunk.is_full();

        // Then
        assert_eq!(is_full, false);
    }

    #[test]
    fn test_chunk_is_full_if_it_is_past_the_max_size() {
        // Given
        let file_id = get_unique_file_id();
        let mut chunk = Chunk::new(&SETUP, file_id, ONE_KB_IN_BYTES).unwrap();

        let key = b"example_key".to_vec();
        let value = vec![b'a'; 1024]; // Vector of 1024 'a's
        let timestamp: u64 = 1620867414000;
        chunk.put(&key, &value, timestamp);

        // When
        let is_full = chunk.is_full();

        // Then
        assert_eq!(is_full, true);
    }

    #[test]
    fn test_correctly_opens_existing_file() {
        // Given
        let file_id = get_unique_file_id();
        let mut chunk = Chunk::new(&SETUP, file_id, ONE_KB_IN_BYTES).unwrap();

        let key = b"example_key".to_vec();
        let value = b"example_value".to_vec();
        let timestamp: u64 = 1620867414000;

        // We are populating the chunk
        let value_details = chunk.put(&key, &value, timestamp).unwrap();

        // When
        let mut existing_chunk = Chunk::from_existing(
            &SETUP.join(format!("{}.data", file_id)),
            file_id,
            ONE_KB_IN_BYTES,
        )
        .unwrap();
        let result = existing_chunk.get(&value_details).unwrap();

        // Then
        assert_eq!(result, value);
    }

    #[test]
    fn test_should_not_be_able_to_write_to_the_existing_file() {
        // Given
        let file_id = get_unique_file_id();
        let mut chunk = Chunk::new(&SETUP, file_id, ONE_KB_IN_BYTES).unwrap();

        let key = b"example_key".to_vec();
        let value = b"example_value".to_vec();
        let timestamp: u64 = 1620867414000;

        // We are populating the chunk
        let _ = chunk.put(&key, &value, timestamp);

        let new_key = b"new_key".to_vec();
        let new_value = b"new_value".to_vec();
        let new_timestamp: u64 = 1620868614000;

        // When
        let mut existing_chunk = Chunk::from_existing(
            &SETUP.join(format!("{}.data", file_id)),
            file_id,
            ONE_KB_IN_BYTES,
        )
        .unwrap();

        // should panic
        let result = existing_chunk.put(&new_key, &new_value, new_timestamp);
        assert_eq!(result.is_err(), true)
    }

    #[test]
    fn test_should_correctly_read_index_with_one_value() {
        // Given
        let file_id = get_unique_file_id();
        let mut chunk = Chunk::new(&SETUP, file_id, ONE_KB_IN_BYTES).unwrap();

        // populating chunk
        let key = b"example_key".to_vec();
        let value = b"example_value".to_vec();
        let timestamp: u64 = 1620867414000;
        let value_details = chunk.put(&key, &value, timestamp).unwrap();

        // When
        let mut index: HashMap<Vec<u8>, ValueDetails> = HashMap::new();
        let _ = chunk.recreate_index(&mut index);

        // Then
        assert_eq!(index.contains_key(&key), true);
        assert_eq!(index.len(), 1);
        assert_eq!(*index.get(&key).unwrap(), value_details);
    }

    #[test]
    fn test_do_not_record_deleted_values() {
        // Given
        let file_id = get_unique_file_id();
        let mut chunk = Chunk::new(&SETUP, file_id, ONE_KB_IN_BYTES).unwrap();

        // populating chunk
        let key = b"example_key".to_vec();
        let value = b"example_value".to_vec();
        let timestamp: u64 = 1620867414000;
        let _ = chunk.put(&key, &value, timestamp).unwrap();
        let _ = chunk.delete(&key).unwrap();

        // When
        let mut index: HashMap<Vec<u8>, ValueDetails> = HashMap::new();
        let _ = chunk.recreate_index(&mut index);

        // Then
        assert_eq!(index.len(), 0);
    }

    #[test]
    fn test_only_store_latest_value() {
        // Given
        let file_id = get_unique_file_id();
        let mut chunk = Chunk::new(&SETUP, file_id, ONE_KB_IN_BYTES).unwrap();

        // populating chunk
        let key = b"example_key".to_vec();
        let old_value = b"old_value".to_vec();
        let old_timestamp: u64 = 1000;
        let new_value = b"new_value".to_vec();
        let new_timestamp: u64 = 2000;

        let _ = chunk.put(&key, &old_value, old_timestamp).unwrap();
        let new_value_details = chunk.put(&key, &new_value, new_timestamp).unwrap();

        // When
        let mut index: HashMap<Vec<u8>, ValueDetails> = HashMap::new();
        let _ = chunk.recreate_index(&mut index);

        // Then
        assert_eq!(index.contains_key(&key), true);
        assert_eq!(index.len(), 1);
        assert_eq!(*index.get(&key).unwrap(), new_value_details);
    }
}
