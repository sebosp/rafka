/// Dynamic Broker Configurations
/// core/src/main/scala/kafka/server/DynamicBrokerConfig.scala
/// Dynamic broker configurations are stored in ZooKeeper and may be defined at two levels:
/// - Per-broker configs persisted at `/configs/brokers/{brokerId}`: These can be
///   described/altered using AdminClient using the resource name brokerId.
/// - Cluster-wide defaults persisted at `/configs/brokers/<default>`: These can be
///   described/altered using AdminClient using an empty resource name.
/// The order of precedence for broker configs is:
///   - DYNAMIC_BROKER_CONFIG: stored in ZK at /configs/brokers/{brokerId}
///   - DYNAMIC_DEFAULT_BROKER_CONFIG: stored in ZK at /configs/brokers/<default>
///   - STATIC_BROKER_CONFIG: properties that broker is started up with, typically from
///     server.properties file
///   - DEFAULT_CONFIG: Default configs defined in KafkaConfig
/// Log configs use topic config overrides if defined and fallback to broker defaults using the
/// order of precedence above. Topic config overrides may use a different config name from the
/// default broker config. See [[kafka.log.LogConfig#TopicConfigSynonyms]] for the mapping.
///
/// AdminClient returns all config synonyms in the order of precedence when configs are
/// described with `includeSynonyms`. In addition to configs that may be defined with the same
/// name at different levels, some configs have additional synonyms.
/// - Listener configs may be defined using the prefix
///   `listener.name.{listenerName}.{configName}`. These may be configured as dynamic or static
///   broker configs. Listener configs have higher precedence than the base configs that don't
///   specify the listener name. Listeners without a listener config use the base config. Base
///   configs may be defined only as STATIC_BROKER_CONFIG or DEFAULT_CONFIG and cannot be
///   updated dynamically.
/// - Some configs may be defined using multiple properties. For example, `log.roll.ms` and
///   `log.roll.hours` refer to the same config that may be defined in milliseconds or hours.
///   The order of precedence of these synonyms is described in the docs of these configs in
///   [[kafka.server.KafkaConfig]].
use crate::majordomo::{AsyncTask, AsyncTaskError};
use crate::server::dynamic_config_manager::ConfigEntityName;
use crate::server::kafka_config::KafkaConfig;
use crate::zk::admin_zk_client::AdminZkClient;
use crate::zk::zk_data::ZkData;
use tokio::sync::mpsc;

#[derive(Debug, PartialEq, Clone)]
pub struct DynamicBrokerConfig {
    pub kafka_config: KafkaConfig,
}

impl Default for DynamicBrokerConfig {
    fn default() -> Self {
        Self { kafka_config: KafkaConfig::default() }
    }
}

impl DynamicBrokerConfig {
    pub fn new(kafka_config: KafkaConfig) -> Self {
        Self { kafka_config }
    }

    pub async fn initialize(&mut self, tx: mpsc::Sender<AsyncTask>) -> Result<(), AsyncTaskError> {
        let admin_zk_client = AdminZkClient::new(tx.clone());
        let zk_data = ZkData::default();
        let (entity_conf_key, entity_conf_value) = admin_zk_client
            .fetch_entity_config(
                zk_data.config_types.broker,
                ConfigEntityName::default().default.to_string(),
            )
            .await?;
        self.update_default_config(entity_conf_key, entity_conf_value);
        let (prop_key, prop_value) = admin_zk_client
            .fetch_entity_config(
                zk_data.config_types.broker,
                self.kafka_config.broker_id.to_string(),
            )
            .await?;
        self.update_broker_config(self.kafka_config.broker_id, prop_key, prop_value)?;
        Ok(())
    }

    fn update_default_config(&mut self, key: String, value: String) -> Result<(), AsyncTaskError> {
        unimplemented!()
    }

    fn update_broker_config(
        &mut self,
        broker_id: i32,
        key: String,
        value: String,
    ) -> Result<(), AsyncTaskError> {
        unimplemented!()
    }
}
