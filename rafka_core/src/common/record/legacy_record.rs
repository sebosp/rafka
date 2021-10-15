//! From clients/src/main/java/org/apache/kafka/common/record/LegacyRecord.java

/// The current offset and size for all the fixed-length fields
pub const CRC_LENGTH: usize = 4;
pub const MAGIC_LENGTH: usize = 1;
pub const ATTRIBUTES_LENGTH: usize = 1;
pub const KEY_SIZE_LENGTH: usize = 4;
pub const VALUE_SIZE_LENGTH: usize = 4;
/// The size for the record header
pub const HEADER_SIZE_V0: usize = CRC_LENGTH + MAGIC_LENGTH + ATTRIBUTES_LENGTH;
/// The amount of overhead bytes in a record
pub const RECORD_OVERHEAD_V0: usize = HEADER_SIZE_V0 + KEY_SIZE_LENGTH + VALUE_SIZE_LENGTH;
