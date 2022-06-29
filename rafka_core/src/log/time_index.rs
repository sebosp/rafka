//! From kafka/log/TimeIndex.scala
//! A TimeIndex file contains a set of 12-byte values.
//! These 12-bytes consist of:
//! - (8 bytes) timestamp of a message
//! - (4 bytes) relative offset of such message

use std::path::PathBuf;

#[derive(Debug)]
pub struct TimeIndex {
    file: PathBuf,
    base_offset: i64,
    max_index_size: i32,
    writable: bool,
    entry_size: i32,
}

impl TimeIndex {
    pub fn new(
        file: PathBuf,
        base_offset: i64,
        max_index_size: Option<i32>,
        writable: Option<bool>,
    ) -> Self {
        let max_index_size = max_index_size.unwrap_or(-1);
        let writeble = writable.unwrap_or(true);
        Self { file, base_offset, max_index_size, writable, entry_size: 12 }
    }
}
