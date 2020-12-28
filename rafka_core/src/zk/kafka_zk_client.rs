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

use crate::zookeeper::zoo_keeper_client::ZKClientConfig;
use crate::zookeeper::zoo_keeper_client::ZooKeeperClient;
use std::time::Instant;
pub struct KafkaZkClient {
    zoo_keeper_client: ZooKeeperClient,
    // is_secure: bool,
    time: Instant,
    // This variable holds the Zookeeper session id at the moment a Broker gets registered in
    // Zookeeper and the subsequent updates of the session id. It is possible that the session
    // id changes over the time for 'Session expired'. This code is part of the work around
    // done in the KAFKA-7165, once ZOOKEEPER-2985 is complete, this code must be deleted.
    current_zookeeper_session_id: i32,
}

impl Default for KafkaZkClient {
    fn default() -> Self {
        KafkaZkClient {
            zoo_keeper_client: ZooKeeperClient::default(),
            time: Instant::now(),
            current_zookeeper_session_id: -1i32,
        }
    }
}

impl KafkaZkClient {
    pub fn new(zoo_keeper_client: ZooKeeperClient, time: Instant) -> Self {
        KafkaZkClient { zoo_keeper_client, time, current_zookeeper_session_id: -1i32 }
    }

    /// The builder receives params to create the ZookeeperClient and builds a local instance.
    /// in java this maps to the apply() of the KafkaZkClient Object
    pub fn build(
        connect_string: &str,
        session_timeout_ms: u32,
        connect_timeout_ms: u32,
        max_inflight_requests: u32,
        time: Instant,
        name: Option<String>,
        zk_client_config: Option<ZKClientConfig>,
    ) -> Self {
        let zooKeeper_client = ZooKeeperClient::new(
            connect_string.to_string(),
            session_timeout_ms,
            connect_timeout_ms,
            max_inflight_requests,
            time,
            name,
            zk_client_config,
            None, // TODO: This is tx
        );
        KafkaZkClient::new(zooKeeper_client, time)
    }
}
