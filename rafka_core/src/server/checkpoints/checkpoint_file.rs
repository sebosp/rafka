//! From core/src/main/scala/kafka/server/checkpoints/CheckpointFile.scala

use crate::majordomo::AsyncTask;
use std::fs::File;
use std::io;
use std::path::PathBuf;
use tokio::sync::mpsc::Sender;
use tracing::trace;

pub trait CheckpointFileFormatter {
    type Data;
    fn to_line(entry: Self::Data) -> String;
    fn from_line(line: String) -> Option<Self::Data>;
}

#[derive(Debug)]
pub struct TopicPartitionCheckpointFile {
    file: PathBuf,
    pub async_task_tx: Sender<AsyncTask>,
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
}
