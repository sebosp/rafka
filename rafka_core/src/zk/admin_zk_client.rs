//! Kafka Admin Methods
//! core/src/main/scala/kafka/zk/AdminZkClient.scala
//!
//! Provides admin related methods for interacting with ZooKeeper.
//!
//! This is an internal class and no compatibility guarantees are provided,
//! see org.apache.kafka.clients.admin.AdminClient for publicly supported APIs.

pub struct AdminZkClient {
    zk_client: KafkaZkClient,
}
