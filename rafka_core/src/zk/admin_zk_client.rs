//! Kafka Admin Methods
//! core/src/main/scala/kafka/zk/AdminZkClient.scala
//!
//! Provides admin related methods for interacting with ZooKeeper.
//!
//! This is an internal class and no compatibility guarantees are provided,
//! see org.apache.kafka.clients.admin.AdminClient for publicly supported APIs.

use crate::majordomo::{AsyncTask, AsyncTaskError};
use crate::zk::kafka_zk_client::KafkaZkClient;
use crate::zk::zk_data::{ConfigEntityZNode, ZkData};
use std::collections::HashMap;
use tokio::sync::mpsc;

#[derive(Debug)]
pub struct AdminZkClient {
    tx: mpsc::Sender<AsyncTask>,
    zk_data: ZkData,
}

impl AdminZkClient {
    pub fn new(tx: mpsc::Sender<AsyncTask>) -> Self {
        let zk_data = ZkData::default();
        Self { tx, zk_data }
    }

    pub async fn fetch_entity_config(
        &self,
        root_entity_type: &str,
        sanitized_entity_name: &str,
    ) -> Result<HashMap<String, String>, AsyncTaskError> {
        let entity_data = KafkaZkClient::get_entity_configs(
            self.tx.clone(),
            root_entity_type,
            sanitized_entity_name,
        )
        .await?;
        Ok(ConfigEntityZNode::decode(entity_data)?)
    }
}
