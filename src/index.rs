use std::collections::HashMap;

use super::db::ValueDetails;

pub struct Index {
    map: HashMap<Vec<u8>, ValueDetails>,
}

impl Index {
    pub fn new(map: HashMap<Vec<u8>, ValueDetails>) -> Self {
        Index{map}
    }

    pub fn put(&mut self, key: Vec<u8>, value: ValueDetails) {
        self.map.insert(key,   value);
    }

    pub fn get(&mut self, key: &Vec<u8>) -> Option<&ValueDetails> {
        return self.map.get(key);
    }

    pub fn delete(&mut self, key: &Vec<u8>) {
        self.map.remove(key);
    }
}