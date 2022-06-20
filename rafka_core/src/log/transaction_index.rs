//! From kafka/log/TransactionIndex.scala
//!
//! Contains medata about aborted transactions for a segment. The metadata includes start and end
//! offsets of such aborted transaction and the last stable offset (LSO) when the abort took place.
//! The aborted transactions in a fetch request range at READ_COMMITTED isolation level are found
//! Individual transactions may reference multiple segments, the recovery process needs to scan
//! early segments to locate the start of the transaction.
//! There is max one transaction index for a segment, its entries point to commit markers that were
//! written in a log segment.

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
