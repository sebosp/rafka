//! From clients/src/main/java/org/apache/kafka/common/TopicPartition.java

use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::{Hash, Hasher};

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
