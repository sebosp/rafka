/// Class used to hold dynamic configs. These are configs which have no physical manifestation
/// in the server.properties and can only be set dynamically.
/// RAFKA NOTES:
/// - The properties are LONG and must be at least 0. They have been set as u64 here.
use crate::common::config_def::{ConfigDef, ConfigDefImportance};
use crate::server::kafka_config::{KafkaConfigError, KafkaConfigProperties};
use crate::server::replication_quota_manager::ReplicationQuotaManagerConfig;
use std::collections::HashMap;

pub const LEADER_REPLICATION_THROTTLED_RATE_PROP: &str = "leader.replication.throttled.rate";
pub const FOLLOWER_REPLICATION_THROTTLED_RATE_PROP: &str = "follower.replication.throttled.rate";
pub const REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_PROP: &str =
    "replica.alter.log.dirs.io.max.bytes.per.second";

#[derive(Debug, Default)]
pub struct DynamicConfig {
    pub broker: BrokerConfigs,
}

#[derive(Debug)]
pub struct BrokerConfigs {
    pub dynamic_props: DynamicBrokerConfigDefs,
    pub non_dynamic_props: Vec<String>,
}

impl Default for BrokerConfigs {
    fn default() -> Self {
        // Get the KafkaConfigProperties and remove the DynamicBrokerConfigDefs (in case they are by
        // mistake maybe added?)
        let configs = KafkaConfigProperties::config_names();
        let dynamic_conf = DynamicBrokerConfigDefs::config_names();
        let (_, non_dynamic_props): (Vec<_>, Vec<_>) =
            configs.into_iter().partition(|&e| dynamic_conf.contains(&e));

        Self { dynamic_props: DynamicBrokerConfigDefs::default(), non_dynamic_props }
    }
}

#[derive(Debug)]
pub struct DynamicBrokerConfigDefs {
    leader_replication_throttled_rate_prop: ConfigDef<u64>,
    follower_replication_throttled_rate_prop: ConfigDef<u64>,
    replica_alter_log_dirs_io_max_bytes_per_second_prop: ConfigDef<u64>,
}

impl Default for DynamicBrokerConfigDefs {
    fn default() -> Self {
        Self {
            leader_replication_throttled_rate_prop: ConfigDef::default()
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(String::from(
                    "A long representing the upper bound (bytes/sec) on replication traffic for \
                     leaders enumerated in the property \
                     ${LogConfig.LeaderReplicationThrottledReplicasProp} (for each topic). This \
                     property can be only set dynamically. It is suggested that the limit be kept \
                     above 1MB/s for accurate behaviour.",
                ))
                .with_default(String::from(
                    ReplicationQuotaManagerConfig::default()
                        .quota_bytes_per_second_default
                        .to_string(),
                )),

            follower_replication_throttled_rate_prop: ConfigDef::default()
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(String::from(
                    "A long representing the upper bound (bytes/sec) on replication traffic for \
                     followers enumerated in the property \
                     ${LogConfig.FollowerReplicationThrottledReplicasProp} (for each topic). This \
                     property can be only set dynamically. It is suggested that the limit be kept \
                     above 1MB/s for accurate behaviour.",
                )),
            replica_alter_log_dirs_io_max_bytes_per_second_prop: ConfigDef::default()
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(String::from(
                    "A long representing the upper bound (bytes/sec) on disk IO used for moving \
                     replica between log directories on the same broker. This property can be \
                     only set dynamically. It is suggested that the limit be kept above 1MB/s for \
                     accurate behaviour.",
                )),
        }
    }
}

impl DynamicBrokerConfigDefs {
    /// `try_from_config_property` transforms a string value from the config into our actual types
    pub fn try_set_property(
        &mut self,
        property_name: &str,
        property_value: &str,
    ) -> Result<(), KafkaConfigError> {
        match property_name {
            LEADER_REPLICATION_THROTTLED_RATE_PROP => {
                self.leader_replication_throttled_rate_prop.try_set_parsed_value(property_value)?
            },
            FOLLOWER_REPLICATION_THROTTLED_RATE_PROP => self
                .follower_replication_throttled_rate_prop
                .try_set_parsed_value(property_value)?,
            REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_PROP => self
                .replica_alter_log_dirs_io_max_bytes_per_second_prop
                .try_set_parsed_value(property_value)?,
            _ => return Err(KafkaConfigError::UnknownKey(property_name.to_string())),
        }
        Ok(())
    }

    /// `config_names` returns a list of config keys used by KafkaConfigProperties
    pub fn config_names() -> Vec<String> {
        // TODO: This should be derivable somehow too.
        vec![
            LEADER_REPLICATION_THROTTLED_RATE_PROP.to_string(),
            FOLLOWER_REPLICATION_THROTTLED_RATE_PROP.to_string(),
            REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_PROP.to_string(),
        ]
    }

    pub fn validate(props: HashMap<String, String>) -> Result<(), String> {
        unimplemented!()
    }
}
