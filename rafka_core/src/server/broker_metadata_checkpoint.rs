//! Broker Metadata  Checkpoint saves brokre metada to a file.
//! core/src/main/scala/kafka/server/BrokerMetadataCheckpoint.scala
#[derive(Debug)]
pub struct BrokerMetadataCheckpoint {
    broker_id: u32,
    cluster_id: Option<String>,
}

impl BrokerMetadataCheckpoint {
    pub fn to_string(self) -> String {
        format!(
            "BrokerMetadata(brokerId={}, clusterId={})",
            self.broker_id,
            self.cluster_id.unwrap_or(String::from("None"))
        )
    }
}
