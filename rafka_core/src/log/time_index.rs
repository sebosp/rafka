//! From kafka/log/TimeIndex.scala
//! A TimeIndex file contains a set of 12-byte values.
//! These 12-bytes consist of:
//! - (8 bytes) timestamp of a message
//! - (4 bytes) relative offset of such message

use std::path::PathBuf;

pub struct TimeIndex {
    file: PathBuf,
    base_offset: i64,
    max_index_size: i32,
    writable: bool,
    entry_size: i32,
}

impl TimeIndex {
    fn new(file: PathBuf, base_offset: i64) -> Self {
        Self { file, base_offset, max_index_size: -1, writable: true, entry_size: 12 }
    }
}
