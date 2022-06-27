//! From kafka/log/OffsetIndex.scala
//! An OffsetIndex file contains a set of 8-byte values.
//! These 8-byte values consist of:
//! - (4 bytes) relative offset of a message
//! - (4 bytes) physical position inside the file on disk of such message

use std::path::PathBuf;

#[derive(Debug)]
pub struct OffsetIndex {
    file: PathBuf,
    base_offset: i64,
    max_index_size: i32,
    writable: bool,
    entry_size: i32,
}

impl OffsetIndex {
    pub fn new(
        file: PathBuf,
        base_offset: i64,
        max_index_size: Option<i32>,
        writable: Option<bool>,
    ) -> Self {
        let max_index_size = max_index_size.unwrap_or(-1);
        let writable = writable.unwrap_or(true);
        Self { file, base_offset, max_index_size, writable, entry_size: 8 }
    }
}
