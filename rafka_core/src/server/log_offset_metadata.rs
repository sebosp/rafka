//! From kafka/server/LogOffsetMetadata.scala

use crate::log::log;

// RAFKA TODO: maybe an enum?
pub const UNKNOWN_FILE_POSITION: i32 = -1;

/// A log offset structure
pub struct LogOffsetMetadata {
    /// The message offset in the partition
    message_offset: i64,
    /// The base message offset of the located segment
    segment_base_offset: i64,
    /// The "physical" position on the located segment
    relative_position_in_segment: i32,
}

impl Default for LogOffsetMetadata {
    fn default() -> Self {
        Self {
            message_offset: 0,
            segment_base_offset: log::UNKNOWN_OFFSET,
            relative_position_in_segment: UNKNOWN_FILE_POSITION,
        }
    }
}
