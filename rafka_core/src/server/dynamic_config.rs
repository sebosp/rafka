/// Class used to hold dynamic configs. These are configs which have no physical manifestation
/// in the server.properties and can only be set dynamically.
/// RAFKA NOTES:
/// - The properties are LONG and must be at least 0. They have been set as u64 here.
use crate::common::config_def::{ConfigDef, ConfigDefImportance};
use crate::server::replication_quota_manager::ReplicationQuotaManagerConfig;

pub const LEADER_REPLICATION_THROTTLED_RATE_PROP: &str = "leader.replication.throttled.rate";
pub const FOLLOWER_REPLICATION_THROTTLED_RATE_PROP: &str = "follower.replication.throttled.rate";
pub const REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_PROP: &str =
    "replica.alter.log.dirs.io.max.bytes.per.second";

#[derive(Debug)]
pub struct DynamicConfig {
    broker: BrokerConfigs,
}

#[derive(Debug, Default)]
pub struct BrokerConfigs {
    non_dynamic_props: NonDynamicBrokerConfigs,
}

#[derive(Debug)]
pub struct NonDynamicBrokerConfigs {
    leader_replication_throttled_rate_prop: ConfigDef<u64>,
    follower_replication_throttled_rate_prop: ConfigDef<u64>,
    replica_alter_log_dirs_io_max_bytes_per_second_prop: ConfigDef<u64>,
}

impl Default for NonDynamicBrokerConfigs {
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
