/// Class used to hold dynamic configs. These are configs which have no physical manifestation
/// in the server.properties and can only be set dynamically.
/// RAFKA NOTES:
/// - The properties are LONG and must be at least 0. They have been set as u64 here.
use crate::common::config_def::{ConfigDef, ConfigDefImportance};
use crate::server::kafka_config::{KafkaConfigError, KafkaConfigProperties, TrySetProperty};
use crate::server::replication_quota_manager::ReplicationQuotaManagerConfig;
use rafka_derive::ConfigDef;
use std::collections::HashMap;
use std::str::FromStr;
use tracing::error;

// Config Keys
pub const LEADER_REPLICATION_THROTTLED_RATE_PROP: &str = "leader.replication.throttled.rate";
pub const FOLLOWER_REPLICATION_THROTTLED_RATE_PROP: &str = "follower.replication.throttled.rate";
pub const REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_PROP: &str =
    "replica.alter.log.dirs.io.max.bytes.per.second";

// Documentation
pub const LEADER_REPLICATION_THROTTLED_RATE_DOC: &str =
    "A long representing the upper bound (bytes/sec) on replication traffic for leaders \
     enumerated in the property ${LogConfig.LeaderReplicationThrottledReplicasProp} (for each \
     topic). This property can be only set dynamically. It is suggested that the limit be kept \
     above 1MB/s for accurate behaviour.";
pub const FOLLOWER_REPLICATION_THROTTLED_RATE_DOC: &str =
    "A long representing the upper bound (bytes/sec) on replication traffic for followers \
     enumerated in the property ${LogConfig.FollowerReplicationThrottledReplicasProp} (for each \
     topic). This property can be only set dynamically. It is suggested that the limit be kept \
     above 1MB/s for accurate behaviour.";
pub const REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_DOC: &str =
    "A long representing the upper bound (bytes/sec) on disk IO used for moving replica between \
     log directories on the same broker. This property can be only set dynamically. It is \
     suggested that the limit be kept above 1MB/s for accurate behaviour.";

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
            configs.iter().map(|x| x.to_string()).partition(|e| dynamic_conf.contains(&e.as_str()));

        Self { dynamic_props: DynamicBrokerConfigDefs::default(), non_dynamic_props }
    }
}

#[derive(Debug, ConfigDef)]
pub struct DynamicBrokerConfigDefs {
    #[config_def(
        key = LEADER_REPLICATION_THROTTLED_RATE_PROP,
        importance = Medium,
        doc = LEADER_REPLICATION_THROTTLED_RATE_DOC,
        with_default_fn,
    )]
    leader_replication_throttled_rate_prop: ConfigDef<u64>,

    #[config_def(
        key = FOLLOWER_REPLICATION_THROTTLED_RATE_PROP,
        importance = Medium,
        doc = FOLLOWER_REPLICATION_THROTTLED_RATE_DOC,
    )]
    follower_replication_throttled_rate_prop: ConfigDef<u64>,

    #[config_def(
        key = REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_PROP,
        importance = Medium,
        doc = REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_DOC,
    )]
    replica_alter_log_dirs_io_max_bytes_per_second_prop: ConfigDef<u64>,
}

impl DynamicBrokerConfigDefs {
    fn default_leader_replication_throttled_rate_prop() -> u64 {
        ReplicationQuotaManagerConfig::default().quota_bytes_per_second_default
    }

    pub fn validate(props: HashMap<String, String>) -> Result<(), KafkaConfigError> {
        // Validate Names
        let names = Self::config_names();
        let custom_props_allowed = true;
        if !custom_props_allowed {
            let unknown_keys: Vec<String> =
                props.keys().filter(|x| names.contains(&x.as_str())).map(|x| x.clone()).collect();
            if !unknown_keys.is_empty() {
                error!("Unknown Dynamic Configuration: {:?}.", unknown_keys);
                // Fail early with the latest failed key. the error line above would show all the
                // keys so operator needs to check the logs
                return Err(KafkaConfigError::UnknownKey(
                    unknown_keys.first().unwrap().to_string(),
                ));
            }
        }
        // Validate Values
        let mut test = Self::default();
        let mut last_invalid_settings: Option<KafkaConfigError> = None;
        for (prop_key, prop_value) in props {
            if let Err(err) = test.try_set_property(&prop_key, &prop_value) {
                error!("Invalid key: '{}' with value '{}': {:?}", prop_key, prop_value, err);
                last_invalid_settings = Some(err);
            }
        }
        // DynamicConfig::validate(Self???, props, custom_props_allowed = true)
        match last_invalid_settings {
            Some(val) => Err(val),
            None => Ok(()),
        }
    }
}
