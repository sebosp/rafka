//! From kafka/server/epoch/LeaderEpochFileCache.scala

use core::fmt;

use crate::common::topic_partition::TopicPartition;
use crate::majordomo::AsyncTaskError;
use crate::server::checkpoints::checkpoint_file::CheckpointFileFormatter;
use crate::server::checkpoints::leader_epoch_checkpoint_file::{
    LeaderEpochCheckpointFile, WHITE_SPACES_PATTERN,
};

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
    pub async fn new(
        topic_partition: TopicPartition,
        log_end_offset: i64,
        checkpoint: LeaderEpochCheckpointFile,
    ) -> Result<Self, AsyncTaskError> {
        let epochs = checkpoint.read().await?;
        let log_ident = format!("[LeaderEpochCache {topic_partition}] ");
        Ok(Self { topic_partition, log_end_offset, checkpoint, epochs, log_ident })
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

impl CheckpointFileFormatter for EpochEntry {
    fn to_line(&self) -> String {
        format!("{} {}", self.epoch, self.start_offset)
    }

    fn from_line(line: &str) -> Option<Self> {
        let split_line = WHITE_SPACES_PATTERN.split(line).collect::<Vec<&str>>();
        if split_line.len() != 2 {
            None
        } else {
            Some(EpochEntry::new(
                split_line[0].parse::<i32>().ok()?,
                split_line[1].parse::<i64>().ok()?,
            ))
        }
    }
}
