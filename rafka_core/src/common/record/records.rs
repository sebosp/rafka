//! From clients/src/main/java/org/apache/kafka/common/record/Records.java
//! A log is a sequence of record batches.

pub const OFFSET_OFFSET: usize = 0;
pub const OFFSET_LENGTH: usize = 8;
pub const SIZE_OFFSET: usize = OFFSET_OFFSET + OFFSET_LENGTH;
pub const SIZE_LENGTH: usize = 4;
pub const LOG_OVERHEAD: usize = SIZE_OFFSET + SIZE_LENGTH;
