#![warn(rust_2018_idioms)]
use thiserror::Error;

pub mod api;
pub mod cluster;
pub mod common;
pub mod coordinator;
pub mod log;
pub mod majordomo;
pub mod message;
pub mod server;
mod utils;
pub mod zk;
mod zookeeper;

#[derive(Debug, Error)]
pub enum KafkaException {
    #[error(
        "Found directory {0}, '{1}' is not in the form of topic-partition or \
         topic-partition.uniqueId-delete (if marked for deletion) Kafka's log directories (and \
         children) should only contain Kafka topic data."
    )]
    InvalidTopicPartitionDir(String, String),
}
