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
    // RAFKA TODO: defaults are: max_index_size: -1, writable: true, entry_size: 8
    fn new(file: PathBuf, base_offset: i64, max_index_size: i32, writable: bool) -> Self {
        Self { file, base_offset, max_index_size, writable, entry_size: 8 }
    }
}
