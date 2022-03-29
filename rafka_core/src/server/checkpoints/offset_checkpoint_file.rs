//! From core/src/main/scala/kafka/server/checkpoints/OffsetCheckpointFile.scala

use crate::common::topic_partition::TopicPartition;
use crate::majordomo::AsyncTask;
use crate::majordomo::AsyncTaskError;
use crate::server::log_failure_channel::LogDirFailureChannelAsyncTask;
use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use tokio::sync::mpsc::Sender;

use super::checkpoint_file::TopicPartitionCheckpointFile;
use super::checkpoint_file::TopicPartitionCheckpointReadBuffer;

const CURRENT_VERSION: i32 = 0;

#[derive(Debug)]
/// Stores in a file a HashMap of (Partition -> Offsets) for a replica
pub struct OffsetCheckpointFile {
    /// A file in which to store the partition -> offsets.
    file: PathBuf,
    /// In case of errors, we need to notify the LogDirsFailureChannel state in
    /// MajorDomoCoordinator
    async_task_tx: Sender<AsyncTask>,
    checkpoint: TopicPartitionCheckpointFile,
    log_dir: PathBuf,
}

impl OffsetCheckpointFile {
    pub fn new(
        file: PathBuf,
        async_task_tx: Sender<AsyncTask>,
        log_dir: PathBuf,
    ) -> Result<Self, io::Error> {
        let checkpoint = TopicPartitionCheckpointFile::new(
            file.clone(),
            async_task_tx.clone(),
            CURRENT_VERSION,
        )?;
        Ok(Self { file, async_task_tx, checkpoint, log_dir })
    }

    pub async fn read(&self) -> Result<HashMap<TopicPartition, i64>, AsyncTaskError> {
        // RAFKA TODO: Figure out why canonicalize could fail here:
        let abs_path = self.file.canonicalize().unwrap();
        let abs_path = abs_path.display();
        let topic_partition_buf_reader = TopicPartitionCheckpointReadBuffer::new(
            abs_path.to_string(),
            self.file.clone(),
            self.checkpoint.get_version(),
        );
        match topic_partition_buf_reader.read() {
            Ok(val) => Ok(val),
            Err(err) => {
                tracing::error!("Error while reading checkpoint file {abs_path}: {:?}", err);
                LogDirFailureChannelAsyncTask::send_maybe_add_offline_log_dir(
                    self.async_task_tx.clone(),
                    self.log_dir.display().to_string(),
                    err.into(),
                )
                .await?;
                // RAFKA TODO: Originally KafkaStorageException, maybe create KafkaStorageException
                // io::Error cannot be cloned, so the error sent to LogDirsFailureChannel is
                // already consumed there. Otherwise we could return the cause:
                Err(AsyncTaskError::KafkaStorageException(self.file.clone()))
            },
        }
    }
}
