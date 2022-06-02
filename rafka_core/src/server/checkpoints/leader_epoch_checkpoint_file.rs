//! From kafka/server/checkpoints/LeaderEpochCheckpointFile.scala

use crate::majordomo::{AsyncTask, AsyncTaskError};
use crate::server::checkpoints::checkpoint_file::CheckpointFile;
use crate::server::epoch::leader_epoch_file_cache::EpochEntry;
use lazy_static::lazy_static;
use regex::Regex;
use std::path::PathBuf;
use tokio::sync::mpsc;

const LEADER_EPOCH_CHECKPOINT_FILENAME: &str = "leader-epoch-checkpoint";
lazy_static! {
    pub static ref WHITE_SPACES_PATTERN: Regex = Regex::new(r"\s+").unwrap();
}

const CURRENT_VERSION: i32 = 0;

#[derive(Debug)]
pub struct LeaderEpochCheckpointFile {
    file: PathBuf,
    checkpoint: CheckpointFile,
}
impl LeaderEpochCheckpointFile {
    pub fn new_file(dir: PathBuf) -> PathBuf {
        PathBuf::from(format!("{}/{}", dir.display(), LEADER_EPOCH_CHECKPOINT_FILENAME))
    }

    pub fn new(
        file: PathBuf,
        majordomo_tx: mpsc::Sender<AsyncTask>,
    ) -> Result<Self, AsyncTaskError> {
        // The leader epoch is inside a sub-dir of the log.dir and so we need to fetch two levels
        // above to identify the log.dir to mark as offline.
        // RAFKA TODO: Figure out what to do in case of symlink layers, maybe it's easier to pass
        // the log_dir from the KafkaConfig all the way here to avoidh re-calculating the
        // hierarchy all the time, because, if the directory is moved, should the process continue
        // to work by means of inodes/fd caches?
        let dir_parent = match file.parent() {
            Some(val) => match val.parent() {
                Some(val) => val.display().to_string(),
                None => String::from("/"),
            },
            None => String::from("/"),
        };
        Ok(Self {
            file: file.clone(),
            checkpoint: CheckpointFile::new(file, majordomo_tx, CURRENT_VERSION, dir_parent)?,
        })
    }

    pub async fn write(&self, epochs: Vec<EpochEntry>) -> Result<(), AsyncTaskError> {
        self.checkpoint.write(epochs).await
    }

    pub async fn read(&self) -> Result<Vec<EpochEntry>, AsyncTaskError> {
        self.checkpoint.read().await
    }
}
