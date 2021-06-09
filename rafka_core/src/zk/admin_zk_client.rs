//! Kafka Admin Methods
//! core/src/main/scala/kafka/zk/AdminZkClient.scala
//!
//! Provides admin related methods for interacting with ZooKeeper.
//!
//! This is an internal class and no compatibility guarantees are provided,
//! see org.apache.kafka.clients.admin.AdminClient for publicly supported APIs.

use crate::majordomo::AsyncTask;
use crate::zk::kafka_zk_client::KafkaZkClient;
use crate::zk::zk_data::ZkData;
use tokio::sync::mpsc;

#[derive(Debug)]
pub struct AdminZkClient {
    tx: mpsc::Sender<AsyncTask>,
    zk_data: ZkData,
}

impl AdminZkClient {
    pub fn new(tx: mpsc::Sender<AsyncTask>, zk_data: ZkData) -> Self {
        Self { tx, zk_data }
    }

    pub async fn fetch_entity_config(
        &mut self,
        root_entity_type: String,
        sanitized_entity_name: String,
    ) -> String {
        // XXX: This returns Properties
        KafkaZkClient::getEntityConfigs(tx, root_entity_type, sanitized_entity_name).await?
    }
}
