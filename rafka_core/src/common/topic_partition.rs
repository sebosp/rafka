//! From clients/src/main/java/org/apache/kafka/common/TopicPartition.java

use crate::log::log::{self, DELETE_DIR_SUFFIX, FUTURE_DIR_SUFFIX};
use crate::server::checkpoints::checkpoint_file::CheckpointFileFormatter;
use crate::KafkaException;
use std::collections::hash_map::DefaultHasher;
use std::convert::TryFrom;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;

pub const SERIAL_VERSION_UID: i64 = -613627415771699627;

/// A topic name and partition number
/// RAFKA TODO: The hash is for now unused
#[derive(Debug, Hash, Eq, Clone)]
pub struct TopicPartition {
    topic: String,
    partition: u32,
    hash: u64,
}

impl PartialEq for TopicPartition {
    fn eq(&self, rhs: &Self) -> bool {
        self.topic == rhs.topic && self.partition == rhs.partition
    }
}

impl fmt::Display for TopicPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.topic, self.partition)
    }
}

impl TopicPartition {
    pub fn new(topic: String, partition: u32) -> Self {
        Self { partition, topic, hash: 0 }
    }

    pub fn partition(&self) -> u32 {
        self.partition
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn hash_code(&mut self) -> u64 {
        if self.hash != 0 {
            return self.hash;
        }
        let prime: u64 = 31;
        let mut result: u64 = 1;
        result = prime * result + u64::from(self.partition);
        let mut hasher = DefaultHasher::new();
        self.topic.hash(&mut hasher);
        let topic_hash = hasher.finish();
        result = prime * result + topic_hash;
        self.hash = result;
        result
    }
}

impl TryFrom<PathBuf> for TopicPartition {
    type Error = KafkaException;

    fn try_from(dir: PathBuf) -> Result<Self, Self::Error> {
        let full_path = dir.canonicalize().unwrap().display().to_string();

        let dir_name = dir
            .file_name()
            .ok_or(KafkaException::InvalidTopicPartitionDir(full_path.clone(), String::from("")))?
            .to_str()
            .ok_or(KafkaException::InvalidTopicPartitionDir(full_path.clone(), String::from("")))?
            .to_string();
        if dir_name.is_empty() || !dir_name.contains('-') {
            return Err(KafkaException::InvalidTopicPartitionDir(full_path, dir_name));
        }
        // RAFKA TODO: By the time we can inspect the is_none(), we have already ran the Regex
        // engine and would have already captured the topic-partition-<maybe delete/future>,
        // consider catching early the <topic>-<partition> by the regex and then fall back to parse
        // it manually with rsplit/rfind
        if dir_name.ends_with(DELETE_DIR_SUFFIX) && log::delete_dir_pattern(&dir_name).is_none()
            || dir_name.ends_with(FUTURE_DIR_SUFFIX) && log::future_dir_pattern(&dir_name).is_none()
        {
            return Err(KafkaException::InvalidTopicPartitionDir(full_path, dir_name));
        }

        let name: String =
            if dir_name.ends_with(DELETE_DIR_SUFFIX) || dir_name.ends_with(FUTURE_DIR_SUFFIX) {
                dir_name.split_at(dir_name.rfind('.').unwrap()).0.to_string()
            } else {
                dir_name
            };

        let (topic, partition_string) = name.rsplit_once('-').unwrap();
        if topic.is_empty() || partition_string.is_empty() {
            return Err(KafkaException::InvalidTopicPartitionDir(full_path, name));
        }

        match partition_string.parse::<u32>() {
            Ok(val) => Ok(TopicPartition::new(topic.to_string(), val)),
            Err(err) => {
                tracing::error!(
                    "Unable to parse partition number from {partition_string} in full_path '{}' : \
                     {:?}",
                    full_path,
                    err
                );
                Err(KafkaException::InvalidTopicPartitionDir(full_path, name))
            },
        }
    }
}

/// A Helper struct to impl CheckpointFileFormatter for TopicPartition -> Offset
#[derive(Debug)]
pub struct TopicPartitionOffset {
    pub topic_partition: TopicPartition,
    pub offset: i64,
}

impl CheckpointFileFormatter for TopicPartitionOffset {
    fn to_line(&self) -> String {
        format!(
            "{} {} {}",
            self.topic_partition.topic(),
            self.topic_partition.partition(),
            self.offset
        )
    }

    fn from_line(line: &str) -> Option<Self> {
        let fragments: Vec<&str> = line.split(' ').collect();
        if fragments.len() != 3 {
            return None;
        } else {
            // RAFKA TODO: Should parse() errors cause
            // LogDirFailureChannelAsyncTask::send_maybe_add_offline_log_dir() ?
            Some(Self {
                topic_partition: TopicPartition::new(
                    fragments[0].to_string(),
                    fragments[1].parse::<u32>().unwrap(),
                ),
                offset: fragments[2].parse::<i64>().unwrap(),
            })
        }
    }
}
