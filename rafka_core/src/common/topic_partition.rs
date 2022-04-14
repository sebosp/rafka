//! From clients/src/main/java/org/apache/kafka/common/TopicPartition.java

use crate::log::log::{self, DELETE_DIR_SUFFIX, FUTURE_DIR_SUFFIX};
use crate::KafkaException;
use std::collections::hash_map::DefaultHasher;
use std::convert::TryFrom;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;

pub const SERIAL_VERSION_UID: i64 = -613627415771699627;

/// A topic name and partition number
/// RAFKA TODO: The hash is for now unused
#[derive(Debug, Hash, Eq)]
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

        let dir_name: String = match dir.file_name() {
            Some(val) => *val.to_string(),
            None => {
                return Err(KafkaException::InvalidTopicPartitionDir(full_path, String::from("")));
            },
        };
        if dir_name.is_empty() || !dir_name.contains('-') {
            return Err(KafkaException::InvalidTopicPartitionDir(full_path, dir_name));
        }
        if dir_name.ends_with(DELETE_DIR_SUFFIX) && log::delete_dir_pattern(&dir_name).is_none()
            || dir_name.ends_with(FUTURE_DIR_SUFFIX) && log::future_dir_pattern(&dir_name).is_none()
        {
            return Err(KafkaException::InvalidTopicPartitionDir(full_path, dir_name));
        }

        let name: String =
            if dir_name.ends_with(DELETE_DIR_SUFFIX) || dir_name.ends_with(FUTURE_DIR_SUFFIX) {
                dir_name.substring(0, dir_name.last_index_of('.'))
            } else {
                dirName
            };

        let index = name.last_index_of('-');
        let topic = name.substring(0, index);
        let partition_string = name.substring(index + 1);
        if topic.isEmpty || partition_string.is_empty() {
            return Err(KafkaException::InvalidTopicPartitionDir(full_path, dir_name));
        }

        let partition = match partition_string.parse::<i32>() {
            Ok(val) => val,
            Err(err) => return Err(KafkaException::InvalidTopicPartitionDir(full_path, dir_name)),
        };

        Ok(TopicPartition::new(topic, partition))
    }
}
