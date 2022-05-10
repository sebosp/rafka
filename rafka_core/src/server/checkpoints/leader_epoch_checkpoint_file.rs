//! From kafka/server/checkpoints/LeaderEpochCheckpointFile.scala

use super::checkpoint_file::CheckpointFileFormatter;
use crate::majordomo::{AsyncTask, AsyncTaskError};
use crate::server::checkpoints::checkpoint_file::CheckpointFile;
use crate::server::epoch::leader_epoch_file_cache::EpochEntry;
use lazy_static::lazy_static;
use regex::Regex;
use std::path::PathBuf;
use tokio::sync::mpsc;

pub trait LeaderEpochCheckpoint {
    // RAFKA TODO: Figure out why write returned Unit
    fn write(&self, epochs: Vec<EpochEntry>) -> Result<(), AsyncTaskError>;
    fn read(&self) -> Vec<EpochEntry>;
}

const LEADER_EPOCH_CHECKPOINT_FILENAME: &str = "leader-epoch-checkpoint";
lazy_static! {
    static ref WHITE_SPACES_PATTERN: Regex = Regex::new(r"\s+").unwrap();
}

const CURRENT_VERSION: u32 = 0;

pub struct LeaderEpochCheckpointFile {
    file: PathBuf,
    checkpoint: CheckpointFile<EpochEntry>,
}
impl LeaderEpochCheckpointFile {
    pub fn new_file(dir: PathBuf) -> PathBuf {
        PathBuf::from(format!("{}/{}", dir.display(), LEADER_EPOCH_CHECKPOINT_FILENAME))
    }

    pub fn new(
        file: PathBuf,
        majordomo_tx: mpsc::Sender<AsyncTask>,
    ) -> Result<Self, AsyncTaskError> {
        let dir_parent = match file.parent() {
            Some(val) => match val.get_parent() {
                Some(val) => val.display().to_string(),
                None => String::from("/"),
            },
            None => String::from("/"),
        };
        Ok(Self {
            file,
            checkpoint: CheckpointFile::new(file, CURRENT_VERSION, (), majordomo_tx, dir_parent)?,
        })
    }
}

impl LeaderEpochCheckpoint for LeaderEpochCheckpointFile {
    fn write(&self, epochs: Vec<EpochEntry>) -> Result<(), AsyncTaskError> {
        self.checkpoint.write(epochs)
    }

    fn read(&self) -> Vec<EpochEntry> {
        self.checkpoint.read()
    }
}

impl CheckpointFileFormatter for CheckpointFile<EpochEntry> {
    type Data = EpochEntry;

    fn to_line(entry: Self::Data) -> String {
        format!("{} {}", entry.epoch, entry.start_offset)
    }

    fn from_line(line: &str) -> Option<Self::Data> {
        let split_line = line.split(&WHITE_SPACES_PATTERN).collect::<Vec<&str>>();
        if split_line.len() != 2 {
            None
        } else {
            Some(EpochEntry::new(split_line[0].parse::<i32>(), split_line[1].parse::<i64>()))
        }
    }
}
