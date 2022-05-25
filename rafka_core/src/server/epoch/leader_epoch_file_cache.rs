//! From kafka/server/epoch/LeaderEpochFileCache.scala

use core::fmt;

use crate::common::topic_partition::TopicPartition;
use crate::server::checkpoints::leader_epoch_checkpoint_file::LeaderEpochCheckpointFile;

#[derive(Debug)]
pub struct LeaderEpochFileCache {
    topic_partition: TopicPartition,
    // This used to be `() => Long`, a function that returns the offset.
    log_end_offset: i64,
    checkpoint: LeaderEpochCheckpointFile,
    epochs: Vec<EpochEntry>,
    log_ident: String,
}

impl LeaderEpochFileCache {
    pub fn new(
        topic_partition: TopicPartition,
        log_end_offset: i64,
        checkpoint: LeaderEpochCheckpointFile,
    ) -> Self {
        Self {
            topic_partition,
            log_end_offset,
            checkpoint,
            epochs: checkpoint.read(),
            log_ident: format!("[LeaderEpochCache {topic_partition}] "),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.epochs.is_empty()
    }
}

// Mapping of epoch to the first offset of the subsequent epoch
#[derive(Debug)]
pub struct EpochEntry {
    pub epoch: i32,
    pub start_offset: i64,
}

impl EpochEntry {
    pub fn new(epoch: i32, start_offset: i64) -> Self {
        Self { epoch, start_offset }
    }
}

impl fmt::Display for EpochEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EpochEntry(epoch={}, startOffset={})", self.epoch, self.start_offset)
    }
}
