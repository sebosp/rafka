/// Class used to hold dynamic configs. These are configs which have no physical manifestation
/// in the server.properties and can only be set dynamically.
/// RAFKA NOTES:
/// - The properties are LONG and must be at least 0. They have been set as u64 here.
use crate::common::config_def::{ConfigDef, ConfigDefImportance};
use crate::server::kafka_config::{KafkaConfigError, KafkaConfigProperties};
use crate::server::replication_quota_manager::ReplicationQuotaManagerConfig;

pub const LEADER_REPLICATION_THROTTLED_RATE_PROP: &str = "leader.replication.throttled.rate";
pub const FOLLOWER_REPLICATION_THROTTLED_RATE_PROP: &str = "follower.replication.throttled.rate";
pub const REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_PROP: &str =
    "replica.alter.log.dirs.io.max.bytes.per.second";

#[derive(Debug)]
pub struct DynamicConfig {
    broker: BrokerConfigs,
}

#[derive(Debug)]
pub struct BrokerConfigs {
    dynamic_props: DynamicBrokerConfigs,
    non_dynamic_configs: Vec<String>,
}

impl Default for BrokerConfigs {
    fn default() -> Self {
        // Get the KafkaConfigProperties and remove the DynamicBrokerConfigs (in case they are by
        // mistake maybe added?)
        let mut configs = KafkaConfigProperties::config_names();
        for dynamic_conf in DynamicBrokerConfigs::config_names() {
            static_configs.drain_filter(|x| *x == dynamic_conf).collect::<Vec<_>>();
        }
        Self { dynamic_props: DynamicBrokerConfigs::default(), non_dynamic_configs }
    }
}

#[derive(Debug)]
pub struct DynamicBrokerConfigs {
    leader_replication_throttled_rate_prop: ConfigDef<u64>,
    follower_replication_throttled_rate_prop: ConfigDef<u64>,
    replica_alter_log_dirs_io_max_bytes_per_second_prop: ConfigDef<u64>,
}

impl Default for DynamicBrokerConfigs {
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

impl DynamicBrokerConfigs {
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
            LEADER_REPLICATION_THROTTLED_RATE_PROP,
            FOLLOWER_REPLICATION_THROTTLED_RATE_PROP,
            REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_PROP,
        ]
    }
}
