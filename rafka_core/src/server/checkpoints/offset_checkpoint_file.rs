//! From core/src/main/scala/kafka/server/checkpoints/OffsetCheckpointFile.scala

use crate::common::topic_partition::TopicPartition;
use crate::majordomo::AsyncTask;
use std::io;
use std::path::PathBuf;
use tokio::sync::mpsc::Sender;

use super::checkpoint_file::TopicPartitionCheckpointFile;

const CURRENT_VERSION: i32 = 0;

#[derive(Debug)]
/// Stores in a file a HashMap of (Partition -> Offsets) for a replica
pub struct OffsetCheckpointFile {
    /// A file in which to store the partition -> offsets.
    file: PathBuf,
    /// In case of errors, we need to notify the LogDirsFailureChannel state in
    /// MajorDomoCoordinator
    pub async_task_tx: Sender<AsyncTask>,
    pub checkpoint: TopicPartitionCheckpointFile,
}

impl OffsetCheckpointFile {
    pub fn new(file: PathBuf, async_task_tx: Sender<AsyncTask>) -> Result<Self, io::Error> {
        let checkpoint = TopicPartitionCheckpointFile::new(
            file.clone(),
            async_task_tx.clone(),
            CURRENT_VERSION,
        )?;
        Ok(Self { file, async_task_tx, checkpoint })
    }
}
