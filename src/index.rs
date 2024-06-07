use std::{collections::HashMap, cell::RefCell};

use super::db::ValueDetails;

pub struct Index {
    map: RefCell<HashMap<Vec<u8>, ValueDetails>>,
}

impl Index {
    pub fn new(map: HashMap<Vec<u8>, ValueDetails>) -> Self {
        Index{
            map: RefCell::new(map)
        }
    }

    pub fn put(&self, key: Vec<u8>, value: ValueDetails) {
        self.map.borrow_mut().insert(key,   value);
    }

    pub fn get(&self, key: &Vec<u8>) -> Option<ValueDetails> {
        self.map.borrow().get(key).cloned()
    }

    pub fn delete(&self, key: &Vec<u8>) {
        self.map.borrow_mut().remove(key);
    }
}