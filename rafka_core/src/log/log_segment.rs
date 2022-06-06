//! From kafka/log/LogSegment.scala
//! A segment has a base offset, which is an offset less than or equal to any of the offsets of any
//! messages it contains and it's greater than any offset in any previous segments.
//! A Segment consists of two files:
//! - [`<base_offset>.log`] containing the messages.
//! - [`<base_offset>.index`] that allows mapping of logical offsets to physical file positions.

use std::time::Instant;

#[derive(Debug)]
pub struct LogSegment {
    /// The file records containing log entries
    log: FileRecords,
    /// The offset index
    lazy_offset_index: LazyIndex<OffsetIndex>,
    /// Timestamp index
    lazy_time_index: LazyIndex<TimeIndex>,
    /// Transaction index
    txn_index: TransactionIndex,
    /// Lower bound of offsets in the segment
    base_offset: i64,
    /// Approximate bytes between entries in the index
    index_interval_bytes: i32,
    /// Max random jitter subtracted from the scheduled roll time of the segment.
    roll_jitter_ms: i64,
    time: Instant,
}
impl LogSegment {
    pub fn new(
        log: FileRecords,
        lazy_offset_index: LazyIndex<OffsetIndex>,
        lazy_time_index: LazyIndex<TimeIndex>,
        txn_index: TransactionIndex,
        base_offset: i64,
        index_interval_bytes: i32,
        roll_jitter_ms: i64,
        time: Instant,
    ) -> Self {
        Self {
            log,
            lazy_offset_index,
            lazy_time_index,
            txn_index,
            base_offset,
            index_interval_bytes,
            roll_jitter_ms,
            time,
        }
    }
}
