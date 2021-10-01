//! From core/src/main/scala/kafka/cluster/EndPoint.scala

use crate::{common::network::listener_name::ListenerName, server::kafka_config::KafkaConfigError};
#[derive(PartialOrd, PartialEq, Clone)]
pub struct EndPoint {
    pub host: String,
    pub port: i32,
    pub listener_name: ListenerName,
}

impl EndPoint {
    pub fn create_end_point(listener: String) -> Result<Self, KafkaConfigError> {
        unimplemented!();
    }
}
