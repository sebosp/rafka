//! From core/src/main/scala/kafka/server/checkpoints/CheckpointFile.scala

use crate::common::topic_partition::TopicPartition;
use crate::majordomo::AsyncTask;
use std::collections::HashMap;
use std::fs::File;
use std::io::prelude::*;
use std::io::{self, BufReader};
use std::num;
use std::path::PathBuf;
use thiserror::Error;
use tokio::sync::mpsc::Sender;
use tracing::trace;

pub trait CheckpointFileFormatter {
    type Data;
    fn to_line(entry: Self::Data) -> String;
    fn from_line(line: String) -> Option<Self::Data>;
}

#[derive(Debug, Error)]
pub enum TopicPartitionCheckpointFileError {
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
pub struct TopicPartitionCheckpointFile {
    file: PathBuf,
    async_task_tx: Sender<AsyncTask>,
    version: i32,
    log_dir: String,
}

impl TopicPartitionCheckpointFile {
    pub fn new(
        file: PathBuf,
        async_task_tx: Sender<AsyncTask>,
        version: i32,
    ) -> Result<Self, io::Error> {
        // RAFKA TODO: This is about the third time that we unwrap the canonicalization of the dir.
        // MAybe we should create a struct that holds:
        // - Configured log-name
        // - Absolute(canonicalized) path
        // - Absolute dir in String already unwrapped of above.
        // - If the above fails, then send to LogDirFailureChannel.
        let log_dir = match file.parent() {
            Some(val) => val.canonicalize().unwrap().display().to_string(),
            None => String::from("/"),
        };
        let path = file.canonicalize().unwrap();
        let temp_path = format!("{log_dir}.tmp");

        // Create the file early to catch Io errors, the file may already exist and it's ok
        match File::create(&path) {
            Err(why) => match why.kind() {
                io::ErrorKind::AlreadyExists => {},
                _ => return Err(why),
            },
            Ok(_file) => trace!("TopicPartitionCheckpointFile: Created File: {}", path.display()),
        };
        Ok(Self { file, async_task_tx, version, log_dir })
    }

    pub fn get_version(&self) -> i32 {
        self.version
    }
}

pub struct TopicPartitionCheckpointReadBuffer {
    location: String,
    file: PathBuf,
    version: i32,
}

impl TopicPartitionCheckpointReadBuffer {
    pub fn new(location: String, file: PathBuf, version: i32) -> Self {
        Self { location, file, version }
    }

    pub fn read(&self) -> Result<HashMap<TopicPartition, i64>, TopicPartitionCheckpointFileError> {
        let mut res = HashMap::new();
        let f = File::open(self.file)?;
        let mut reader = BufReader::new(f);
        let mut line = String::new();
        // Get and validate the version of the TopicPartitionCheckpointFile
        reader.read_line(&mut line)?;
        if line.is_empty() {
            return Ok(res);
        }
        let file_version = line.parse::<i32>()?;
        if file_version != self.version {
            return Err(TopicPartitionCheckpointFileError::UnrecognizedVersion(
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
            match TopicPartitionCheckpointReadBuffer::from_line(line) {
                Some((topic_partition, offset)) => res.insert(topic_partition, offset),
                None => {
                    return Err(TopicPartitionCheckpointFileError::MalformedLineException(
                        self.location.clone(),
                        line,
                    ))
                },
            };
        }
        if res.len() != expected_size {
            return Err(TopicPartitionCheckpointFileError::UnexpectedItemsLength(
                expected_size,
                self.location.clone(),
                res.len(),
            ));
        }
        Ok(res)
    }
}

impl CheckpointFileFormatter for TopicPartitionCheckpointReadBuffer {
    type Data = (TopicPartition, i64);

    fn to_line(entry: Self::Data) -> String {
        format!("{} {} {}", entry.0.topic(), entry.0.partition(), entry.1)
    }

    fn from_line(line: String) -> Option<Self::Data> {
        let fragments: Vec<&str> = line.split(' ').collect();
        if fragments.len() != 3 {
            return None;
        } else {
            // RAFKA TODO: Should parse() errors cause
            // LogDirFailureChannelAsyncTask::send_maybe_add_offline_log_dir() ?
            Some((
                TopicPartition::new(fragments[0].to_string(), fragments[1].parse::<u32>().unwrap()),
                fragments[2].parse::<i64>().unwrap(),
            ))
        }
    }
}
