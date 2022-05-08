//! From kafka/server/epoch/LeaderEpochFileCache.scala

use core::fmt;

#[derive(Debug)]
pub struct LeaderEpochFileCache {}

// Mapping of epoch to the first offset of the subsequent epoch
pub struct EpochEntry {
    epoch: i32,
    start_offset: i64,
}

impl fmt::Display for EpochEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EpochEntry(epoch={}, startOffset={})", self.epoch, self.start_offset)
    }
}
