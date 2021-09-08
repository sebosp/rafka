//! From core/src/main/scala/kafka/log/LogConfig.scala

use std::convert::TryFrom;

use crate::common::config::topic_config::{
    MAX_COMPACTION_LAG_MS_CONFIG, MIN_COMPACTION_LAG_MS_CONFIG,
};
use crate::server::kafka_config::KafkaConfig;
use crate::{common::config_def::ConfigDef, server::kafka_config::KafkaConfigError};

#[derive(Debug, Default)]
pub struct LogConfig {
    min_compaction_lag: u64,
    max_compaction_lag: u64,
    segment_size: i32,
    segment_ms: i64,
    segment_jitter_ms: i64,
    max_index_size: i32,
    flush_interval: i64,
    flush_ms: i64,
    retention_size: i64,
    retention_ms: i64,
    max_message_size: i32,
    index_interval: i32,
    file_delete_delay_ms: i64,
    delete_retention_ms: i64,
    compaction_lag_ms: i64,
    max_compaction_lag_ms: i64,
    min_cleanable_ratio: f64,
    compact: String,
    delete: bool,
    unclean_leader_election_enable: bool,
    min_in_sync_replicas: i32,
    compression_type: String,
    preallocate: bool,
    message_format_version: String,
    message_timestamp_type: String,
    message_timestamp_difference_max_ms: i64,
    leader_replication_throttled_replicas: String,
    follower_replication_throttled_replicas: String,
    message_down_conversion_enable: bool,
}

impl LogConfig {
    pub fn validate_values(&self) -> Result<(), KafkaConfigError> {
        if self.min_compaction_lag > self.max_compaction_lag {
            return Err(KafkaConfigError::InvalidValue(format!(
                "conflict topic config setting {} ({}) > {} ({})",
                MIN_COMPACTION_LAG_MS_CONFIG,
                self.min_compaction_lag,
                MAX_COMPACTION_LAG_MS_CONFIG,
                self.max_compaction_lag,
            )));
        }
        Ok(())
    }
}

impl TryFrom<KafkaConfig> for LogConfig {
    type Error = KafkaConfigError;

    fn try_from(kafka_config: KafkaConfig) -> Result<Self, Self::Error> {
        let res = Self::default();
        res.segment_size = kafka_config.log_segment_bytes;
        res.validate_values()?;
        Ok(res)
    }
}
