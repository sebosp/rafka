//! From kafka/log/TransactionIndex.scala

use std::path::PathBuf;
#[derive(Debug)]
pub struct TransactionIndex {
    start_offset: i64,
    file: PathBuf,
}

impl TransactionIndex {
    pub fn new(start_offset: i64, file: PathBuf) -> Self {
        Self { start_offset, file }
    }
}
