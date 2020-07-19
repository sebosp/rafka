//! Provides higher level Kafka-specific operations on top of the pipelined
//! [[kafka::zookeeper::ZooKeeperClient]]. ( TODO RAFKA version may not be pipelined?)
//! core/src/main/scala/kafka/zk/KafkaZkClient.scala
//!
//! Implementation note: this class includes methods for various components (Controller, Configs,
//! Old Consumer, etc.) and returns instances of classes from the calling packages in some cases.
//! This is not ideal, but it made it easier to migrate away from `ZkUtils` (since removed). We
//! should revisit this. We should also consider whether a monolithic [[kafka.zk.ZkData]] is the way
//! to go.

// RAFKA TODO: The documentation may not be accurate anymore.

use crate::zookeeper::zoo_keeper_client::ZooKeeperClient;
use std::time::SystemTime;
pub struct KafkaZkClient {
    zoo_keeper_client: ZooKeeperClient,
    is_secure: bool,
    time: SystemTime,
}

impl Default for KafkaZkClient {
    fn default() -> Self {
        KafkaZkClient {
            zoo_keeper_client: ZooKeeperClient::default(),
            is_secure: false,
            time: SystemTime::now(),
        }
    }
}
