//! From core/src/main/scala/kafka/server/checkpoints/CheckpointFile.scala

use crate::common::topic_partition::{TopicPartition, TopicPartitionOffset};
use crate::majordomo::AsyncTask;
use crate::server::epoch::leader_epoch_file_cache::EpochEntry;
use crate::server::log_failure_channel::LogDirFailureChannelAsyncTask;
use std::collections::HashMap;
use std::fs::File;
use std::io::prelude::*;
use std::io::{self, BufReader};
use std::num;
use std::path::PathBuf;
use thiserror::Error;
use tokio::sync::mpsc::Sender;

pub trait CheckpointFileFormatter {
    fn to_line(&self) -> String;
    fn from_line(line: &str) -> Option<Self>;
}

#[derive(Debug, Error)]
pub enum CheckpointFileError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Malformed line in checkpoint file {0}: '{1}'")]
    MalformedLineException(String, String),
    #[error("ParseInt error: {0}")]
    ParseInt(#[from] num::ParseIntError),
    #[error("Unrecognized version of the checkpoint file (0): {1}")]
    UnrecognizedVersion(String, i32),
    #[error("Expected {0} entries in checkpoint file ({1}), but found {2}")]
    UnexpectedItemsLength(usize, String, usize),
}

#[derive(Debug)]
pub enum CheckpointFileType {
    TopicPartition,
    LeaderEpoch,
}

#[derive(Debug)]
pub struct CheckpointFile {
    file: PathBuf,
    async_task_tx: Sender<AsyncTask>,
    version: i32,
    log_dir: String,
    path: PathBuf,
    temp_path: PathBuf,
    checkpoint_type: CheckpointFileType,
}

impl CheckpointFile {
    pub fn new(
        file: PathBuf,
        async_task_tx: Sender<AsyncTask>,
        version: i32,
        checkpoint_type: CheckpointFileType,
        log_dir: String,
    ) -> Result<Self, io::Error> {
        // RAFKA TODO: This is about the third time that we unwrap the canonicalization of the dir.
        // MAybe we should create a struct that holds:
        // - Configured log-name
        // - Absolute(canonicalized) path
        // - Absolute dir in String already unwrapped of above.
        // - If the above fails, then send to LogDirFailureChannel.
        let path = file.canonicalize().unwrap();
        let temp_path = PathBuf::from(format!("{log_dir}.tmp"));

        // Create the file early to catch Io errors, the file may already exist and it's ok
        match File::create(&path) {
            Err(why) => match why.kind() {
                io::ErrorKind::AlreadyExists => {},
                _ => return Err(why),
            },
            Ok(_file) => tracing::trace!("CheckpointFile: Created File: {}", path.display()),
        };
        Ok(Self { file, async_task_tx, version, log_dir, path, temp_path, checkpoint_type })
    }

    pub fn get_version(&self) -> i32 {
        self.version
    }

    pub fn read_topic_partition_format(&self) {
        unimplemented!()
    }

    pub fn write_topic_partition_format(&self) {
        unimplemented!()
    }

    pub fn read_leader_epoch_format(&self) {
        unimplemented!()
    }

    pub fn write_leader_epoch_format(&self, epochs: Vec<EpochEntry>) {
        unimplemented!()
    }
}

pub struct CheckpointReadBuffer {
    location: String,
    file: PathBuf,
    version: i32,
}

impl CheckpointReadBuffer {
    pub fn new(location: String, file: PathBuf, version: i32) -> Self {
        Self { location, file, version }
    }

    /// Previously a formatter was passed to this function that would transform the checkpoint file
    /// into a specific type. In this version the types are defined separately and there's a bit of
    /// duplication.
    pub fn read_topic_partition_format(
        &self,
    ) -> Result<HashMap<TopicPartition, i64>, CheckpointFileError> {
        let mut res = HashMap::new();
        let f = File::open(self.file.clone())?;
        let mut reader = BufReader::new(f);
        let mut line = String::new();
        // Get and validate the version of the TopicPartitionCheckpointFile
        reader.read_line(&mut line)?;
        if line.is_empty() {
            return Ok(res);
        }
        let file_version = line.parse::<i32>()?;
        if file_version != self.version {
            return Err(CheckpointFileError::UnrecognizedVersion(
                self.location.clone(),
                file_version,
            ));
        }

        // Get the number of expected items.
        reader.read_line(&mut line)?;
        if line.is_empty() {
            return Ok(res);
        }
        let expected_size = line.parse::<usize>()?;

        while reader.read_line(&mut line)? != 0 {
            // Ok(0) is EOF
            match TopicPartitionOffset::from_line(&line) {
                Some(tpo) => res.insert(tpo.topic_partition, tpo.offset),
                None => {
                    return Err(CheckpointFileError::MalformedLineException(
                        self.location.clone(),
                        line,
                    ))
                },
            };
        }
        if res.len() != expected_size {
            return Err(CheckpointFileError::UnexpectedItemsLength(
                expected_size,
                self.location.clone(),
                res.len(),
            ));
        }
        Ok(res)
    }

    pub fn read_leader_epoch_format(&self) -> Result<Vec<EpochEntry>, CheckpointFileError> {
        let mut res = vec![];
        let f = File::open(self.file.clone())?;
        let mut reader = BufReader::new(f);
        let mut line = String::new();
        // Get and validate the version of the TopicPartitionCheckpointFile
        reader.read_line(&mut line)?;
        if line.is_empty() {
            return Ok(res);
        }
        let file_version = line.parse::<i32>()?;
        if file_version != self.version {
            return Err(CheckpointFileError::UnrecognizedVersion(
                self.location.clone(),
                file_version,
            ));
        }

        // Get the number of expected items.
        reader.read_line(&mut line)?;
        if line.is_empty() {
            return Ok(res);
        }
        let expected_size = line.parse::<usize>()?;

        while reader.read_line(&mut line)? != 0 {
            // Ok(0) is EOF
            match CheckpointReadBuffer::from_line(&line) {
                Some(epoch_entry) => res.push(epoch_entry),
                None => {
                    return Err(CheckpointFileError::MalformedLineException(
                        self.location.clone(),
                        line,
                    ))
                },
            };
        }
        if res.len() != expected_size {
            return Err(CheckpointFileError::UnexpectedItemsLength(
                expected_size,
                self.location.clone(),
                res.len(),
            ));
        }
        Ok(res)
    }
}
