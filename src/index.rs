use std::{collections::HashMap, cell::RefCell, sync::{RwLock, Arc}};

use super::db::ValueDetails;

pub struct Index {
    map: Arc<RwLock<HashMap<Vec<u8>, ValueDetails>>>,
}

impl Index {
    pub fn new(map: HashMap<Vec<u8>, ValueDetails>) -> Self {
        Index{
            map: Arc::new(RwLock::new(map))
        }
    }

    pub fn put(&self, key: Vec<u8>, value: ValueDetails) {
        self.map.write().unwrap().insert(key,   value);
    }

    pub fn get(&self, key: &Vec<u8>) -> Option<ValueDetails> {
        self.map.read().unwrap().get(key).cloned()
    }

    pub fn delete(&self, key: &Vec<u8>) {
        self.map.write().unwrap().remove(key);
    }
}