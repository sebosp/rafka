//! From core/src/main/scala/kafka/server/checkpoints/OffsetCheckpointFile.scala

use crate::common::topic_partition::{TopicPartition, TopicPartitionOffset};
use crate::majordomo::AsyncTask;
use crate::majordomo::AsyncTaskError;
use crate::server::log_failure_channel::LogDirFailureChannelAsyncTask;
use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use tokio::sync::mpsc::Sender;

use super::checkpoint_file::{CheckpointFile, CheckpointReadBuffer};

const CURRENT_VERSION: i32 = 0;

#[derive(Debug)]
/// Stores in a file a HashMap of (Partition -> Offsets) for a replica
pub struct OffsetCheckpointFile {
    /// A file in which to store the partition -> offsets.
    file: PathBuf,
    /// In case of errors, we need to notify the LogDirsFailureChannel state in
    /// MajorDomoCoordinator
    async_task_tx: Sender<AsyncTask>,
    checkpoint: CheckpointFile,
    log_dir: PathBuf,
}

impl OffsetCheckpointFile {
    pub fn new(
        file: PathBuf,
        async_task_tx: Sender<AsyncTask>,
        log_dir: PathBuf,
    ) -> Result<Self, io::Error> {
        let dir_parent = file.parent().unwrap_or(&PathBuf::from("/")).display().to_string();
        let checkpoint =
            CheckpointFile::new(file.clone(), async_task_tx.clone(), CURRENT_VERSION, dir_parent)?;
        Ok(Self { file, async_task_tx, checkpoint, log_dir })
    }

    pub async fn read(&self) -> Result<HashMap<TopicPartition, i64>, AsyncTaskError> {
        let topic_partition_buf_reader =
            CheckpointReadBuffer::new(self.file.clone(), self.checkpoint.get_version());
        match topic_partition_buf_reader.read::<TopicPartitionOffset>() {
            Ok(tpos) => {
                let mut res = HashMap::new();
                for item in tpos {
                    res.insert(item.topic_partition, item.offset);
                }
                Ok(res)
            },
            Err(err) => {
                let abs_path = self.file.canonicalize().unwrap();
                tracing::error!(
                    "Error while reading checkpoint file {:?}: {:?}",
                    abs_path.display(),
                    err
                );
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
