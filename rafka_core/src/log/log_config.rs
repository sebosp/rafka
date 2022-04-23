//! From core/src/main/scala/kafka/log/LogConfig.scala

use crate::api::api_version::{ApiVersionValidator, KafkaApiVersion};
use crate::common::config::topic_config::*;
use crate::common::config_def::{ConfigDef, ConfigDefImportance, Validator};
use crate::common::record::legacy_record;
use crate::message::compression_codec::BrokerCompressionCodec;
use crate::server::config_handler::ThrottledReplicaListValidator;
use crate::server::kafka_config::general::GeneralConfigProperties;
use crate::server::kafka_config::log::{
    DefaultLogConfigProperties, LogCleanupPolicy, LogMessageTimestampType,
};
use crate::server::kafka_config::replication::ReplicationConfigProperties;
use crate::server::kafka_config::transaction_management::DEFAULT_COMPRESSION_TYPE;
use crate::server::kafka_config::{ConfigSet, KafkaConfig, KafkaConfigError, TrySetProperty};
use rafka_derive::ConfigDef;
use std::collections::HashMap;
use std::str::FromStr;
use tracing::{info, trace};

// Config Keys
pub const CLEANUP_POLICY_PROP: &str = CLEANUP_POLICY_CONFIG;
pub const COMPRESSION_TYPE_PROP: &str = COMPRESSION_TYPE_CONFIG;
pub const DELETE_RETENTION_MS_PROP: &str = DELETE_RETENTION_MS_CONFIG;
pub const FILE_DELETE_DELAY_MS_PROP: &str = FILE_DELETE_DELAY_MS_CONFIG;
pub const FLUSH_MESSAGES_PROP: &str = FLUSH_MESSAGES_INTERVAL_CONFIG;
pub const FLUSH_MS_PROP: &str = FLUSH_MS_CONFIG;

// Leave these out of TopicConfig for now as they are replication quota configs
pub const FOLLOWER_REPLICATION_THROTTLED_REPLICAS_PROP: &str =
    "follower.replication.throttled.replicas";
pub const LEADER_REPLICATION_THROTTLED_REPLICAS_PROP: &str =
    "leader.replication.throttled.replicas";

pub const INDEX_INTERVAL_BYTES_PROP: &str = INDEX_INTERVAL_BYTES_CONFIG;
pub const MAX_COMPACTION_LAG_MS_PROP: &str = MAX_COMPACTION_LAG_MS_CONFIG;
pub const MAX_MESSAGE_BYTES_PROP: &str = MAX_MESSAGE_BYTES_CONFIG;
pub const MESSAGE_DOWNCONVERSION_ENABLE_PROP: &str = MESSAGE_DOWNCONVERSION_ENABLE_CONFIG;
pub const MESSAGE_FORMAT_VERSION_PROP: &str = MESSAGE_FORMAT_VERSION_CONFIG;
pub const MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_PROP: &str =
    MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG;
pub const MESSAGE_TIMESTAMP_TYPE_PROP: &str = MESSAGE_TIMESTAMP_TYPE_CONFIG;
pub const MIN_CLEANABLE_DIRTY_RATIO_PROP: &str = MIN_CLEANABLE_DIRTY_RATIO_CONFIG;
pub const MIN_COMPACTION_LAG_MS_PROP: &str = MIN_COMPACTION_LAG_MS_CONFIG;
pub const MIN_IN_SYNC_REPLICAS_PROP: &str = MIN_IN_SYNC_REPLICAS_CONFIG;
pub const PREALLOCATE_ENABLE_PROP: &str = PREALLOCATE_CONFIG;
pub const RETENTION_BYTES_PROP: &str = RETENTION_BYTES_CONFIG;
pub const RETENTION_MS_PROP: &str = RETENTION_MS_CONFIG;
pub const SEGMENT_BYTES_PROP: &str = SEGMENT_BYTES_CONFIG;
pub const SEGMENT_INDEX_BYTES_PROP: &str = SEGMENT_INDEX_BYTES_CONFIG;
pub const SEGMENT_JITTER_MS_PROP: &str = SEGMENT_JITTER_MS_CONFIG;
pub const SEGMENT_MS_PROP: &str = SEGMENT_MS_CONFIG;
pub const UNCLEAN_LEADER_ELECTION_ENABLE_PROP: &str = UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG;

// Documentation
pub const FOLLOWER_REPLICATION_THROTTLED_REPLICAS_DOC: &str =
    "A list of replicas for which log replication should be throttled on the follower side. The \
     list should describe a set of replicas in the form \
     [PartitionId]:[BrokerId],[PartitionId]:[BrokerId]:... or alternatively the wildcard '*' can \
     be used to throttle all replicas for this topic.";

pub const LEADER_REPLICATION_THROTTLED_REPLICAS_DOC: &str =
    "A list of replicas for which log replication should be throttled on the leader side. The \
     list should describe a set of replicas in the form \
     [PartitionId]:[BrokerId],[PartitionId]:[BrokerId]:... or alternatively the wildcard '*' can \
     be used to throttle all replicas for this topic.";

#[derive(Debug, ConfigDef)]
pub struct LogConfigProperties {
    #[config_def(
        key = CLEANUP_POLICY_CONFIG,
        importance = Medium,
        doc = CLEANUP_POLICY_DOC,
        with_validator_fn,
        with_default_fn,
        no_default_resolver,
        no_default_builder,
    )]
    cleanup_policy: ConfigDef<String>,

    #[config_def(
        key = COMPRESSION_TYPE_CONFIG,
        importance = Medium,
        doc = COMPRESSION_TYPE_DOC,
        with_default_fn,
        no_default_resolver,
        no_default_builder,
    )]
    compression_type: ConfigDef<String>,

    #[config_def(
        key = DELETE_RETENTION_MS_CONFIG,
        importance = Medium,
        doc = DELETE_RETENTION_MS_DOC,
        with_default_fn,
        with_validator_fn,
    )]
    delete_retention_ms: ConfigDef<i64>,

    #[config_def(
        key = FILE_DELETE_DELAY_MS_CONFIG,
        importance = Medium,
        doc = FILE_DELETE_DELAY_MS_DOC,
        with_default_fn,
        with_validator_fn,
    )]
    file_delete_delay_ms: ConfigDef<i64>,

    #[config_def(
        key = FLUSH_MESSAGES_INTERVAL_CONFIG,
        importance = Medium,
        doc = FLUSH_MESSAGES_INTERVAL_DOC,
        with_default_fn,
        with_validator_fn,
    )]
    flush_messages: ConfigDef<i64>,

    #[config_def(
        key = FLUSH_MS_CONFIG,
        importance = Medium,
        doc = FLUSH_MS_DOC,
        with_default_fn,
        with_validator_fn,
    )]
    flush_ms: ConfigDef<i64>,

    #[config_def(
        key = FOLLOWER_REPLICATION_THROTTLED_REPLICAS_PROP,
        importance = Medium,
        doc = FOLLOWER_REPLICATION_THROTTLED_REPLICAS_DOC,
        with_validator_fn,
        no_default_resolver,
        no_default_builder,
    )]
    follower_replication_throttled_replicas: ConfigDef<String>,

    #[config_def(
        key = INDEX_INTERVAL_BYTES_PROP,
        importance = Medium,
        doc = INDEX_INTERVAL_BYTES_DOCS,
        with_default_fn,
        with_validator_fn,
    )]
    index_interval_bytes: ConfigDef<i32>,

    #[config_def(
        key = LEADER_REPLICATION_THROTTLED_REPLICAS_PROP,
        importance = Medium,
        doc = LEADER_REPLICATION_THROTTLED_REPLICAS_DOC,
        with_validator_fn,
        no_default_resolver,
        no_default_builder,
    )]
    leader_replication_throttled_replicas: ConfigDef<String>,

    #[config_def(
        key = MAX_COMPACTION_LAG_MS_CONFIG,
        importance = Medium,
        doc = MAX_COMPACTION_LAG_MS_DOC,
        with_default_fn,
        with_validator_fn,
    )]
    max_compaction_lag_ms: ConfigDef<i64>,

    #[config_def(
        key = MAX_MESSAGE_BYTES_CONFIG,
        importance = Medium,
        doc = MAX_MESSAGE_BYTES_DOC,
        with_default_fn,
        with_validator_fn,
    )]
    max_message_bytes: ConfigDef<usize>,

    #[config_def(
        key = MESSAGE_DOWNCONVERSION_ENABLE_CONFIG,
        importance = Low,
        doc = MESSAGE_DOWNCONVERSION_ENABLE_DOC,
        with_default_fn,
    )]
    message_down_conversion_enable: ConfigDef<bool>,

    #[config_def(
        key = MESSAGE_FORMAT_VERSION_CONFIG,
        importance = Medium,
        doc = MESSAGE_FORMAT_VERSION_DOC,
        with_default_fn,
        with_validator_fn,
        no_default_resolver
        no_default_builder,
    )]
    message_format_version: ConfigDef<String>,

    #[config_def(
        key = MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG,
        importance = Medium,
        doc = MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_DOC,
        with_default_fn,
        with_validator_fn,
    )]
    message_timestamp_difference_max_ms: ConfigDef<i64>,

    #[config_def(
        key = MESSAGE_TIMESTAMP_TYPE_CONFIG,
        importance = Medium,
        doc = MESSAGE_TIMESTAMP_TYPE_DOC,
        with_default_fn,
        with_validator_fn,
    )]
    message_timestamp_type: ConfigDef<String>,

    #[config_def(
        key = MIN_CLEANABLE_DIRTY_RATIO_CONFIG,
        importance = Medium,
        doc = MIN_CLEANABLE_DIRTY_RATIO_DOC,
        with_default_fn,
        with_validator_fn,
    )]
    min_cleanable_dirty_ratio: ConfigDef<f64>,

    #[config_def(
        key = MIN_COMPACTION_LAG_MS_CONFIG,
        importance = Medium,
        doc = MIN_COMPACTION_LAG_MS_DOC,
        with_default_fn,
    )]
    min_compaction_lag_ms: ConfigDef<i64>,

    #[config_def(
        key = MIN_IN_SYNC_REPLICAS_CONFIG,
        importance = Medium,
        doc = MIN_IN_SYNC_REPLICAS_DOC,
        with_default_fn,
        with_validator_fn,
    )]
    min_in_sync_replicas: ConfigDef<i32>,

    #[config_def(
        key = PREALLOCATE_CONFIG,
        importance = Medium,
        doc = PREALLOCATE_DOC,
        with_default_fn,
    )]
    pre_allocate_enable: ConfigDef<bool>,

    #[config_def(
        key = RETENTION_BYTES_CONFIG,
        importance = Medium,
        doc = RETENTION_BYTES_DOC,
        with_default_fn,
    )]
    retention_bytes: ConfigDef<i64>,

    // can be negative. See kafka.log.LogManager.cleanupExpiredSegments
    #[config_def(
        key = RETENTION_MS_CONFIG,
        importance = Medium,
        doc = RETENTION_MS_DOC,
        with_default_fn,
        with_validator_fn,
    )]
    retention_ms: ConfigDef<i64>,

    #[config_def(
        key = SEGMENT_BYTES_CONFIG,
        importance = Medium,
        doc = SEGMENT_BYTES_DOC,
        with_default_fn,
        with_validator_fn,
    )]
    segment_bytes: ConfigDef<usize>,

    #[config_def(
        key = SEGMENT_INDEX_BYTES_CONFIG,
        importance = Medium,
        doc = SEGMENT_INDEX_BYTES_DOC,
        with_default_fn,
        with_validator_fn,
    )]
    segment_index_bytes: ConfigDef<usize>,

    #[config_def(
        key = SEGMENT_JITTER_MS_CONFIG,
        importance = Medium,
        doc = SEGMENT_JITTER_MS_DOC,
        with_default_fn,
        with_validator_fn,
    )]
    segment_jitter_ms: ConfigDef<i64>,

    #[config_def(
        key = SEGMENT_MS_CONFIG,
        importance = Medium,
        doc = SEGMENT_MS_DOC,
        with_default_fn,
        with_validator_fn,
    )]
    segment_ms: ConfigDef<i64>,

    #[config_def(
        key = UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG,
        importance = Medium,
        doc = UNCLEAN_LEADER_ELECTION_ENABLE_DOC,
        with_default_fn,
    )]
    unclean_leader_election_enable: ConfigDef<bool>,
}

impl LogConfigProperties {
    // Defaults
    pub fn default_cleanup_policy() -> String {
        LogCleanupPolicy::Delete.to_string()
    }

    pub fn default_compression_type() -> String {
        DEFAULT_COMPRESSION_TYPE.name.to_string()
    }

    pub fn default_delete_retention_ms() -> i64 {
        DefaultLogConfigProperties::default().build_log_cleaner_delete_retention_ms().unwrap()
    }

    pub fn default_file_delete_delay_ms() -> i64 {
        DefaultLogConfigProperties::default().build_log_delete_delay_ms().unwrap()
    }

    pub fn default_flush_messages() -> i64 {
        DefaultLogConfigProperties::default().build_log_flush_interval_messages().unwrap()
    }

    pub fn default_flush_ms() -> i64 {
        DefaultLogConfigProperties::default().build_log_flush_scheduler_interval_ms().unwrap()
    }

    pub fn default_index_interval_bytes() -> i32 {
        DefaultLogConfigProperties::default().build_log_index_interval_bytes().unwrap()
    }

    pub fn default_max_compaction_lag_ms() -> i64 {
        DefaultLogConfigProperties::default().build_log_cleaner_max_compaction_lag_ms().unwrap()
    }

    pub fn default_max_message_bytes() -> usize {
        GeneralConfigProperties::default().build_message_max_bytes().unwrap()
    }

    pub fn default_message_down_conversion_enable() -> bool {
        DefaultLogConfigProperties::default().build_log_message_down_conversion_enable().unwrap()
    }

    pub fn default_message_format_version() -> String {
        DefaultLogConfigProperties::default()
            .build_log_message_format_version()
            .unwrap()
            .to_string()
    }

    pub fn default_message_timestamp_difference_max_ms() -> i64 {
        DefaultLogConfigProperties::default()
            .build_log_message_timestamp_difference_max_ms()
            .unwrap()
    }

    pub fn default_message_timestamp_type() -> String {
        DefaultLogConfigProperties::default()
            .build_log_message_timestamp_type()
            .unwrap()
            .to_string()
    }

    pub fn default_min_cleanable_dirty_ratio() -> f64 {
        DefaultLogConfigProperties::default().build_log_cleaner_min_clean_ratio().unwrap()
    }

    pub fn default_min_compaction_lag_ms() -> i64 {
        DefaultLogConfigProperties::default().build_log_cleaner_min_compaction_lag_ms().unwrap()
    }

    pub fn default_min_in_sync_replicas() -> i32 {
        DefaultLogConfigProperties::default().build_min_in_sync_replicas().unwrap()
    }

    pub fn default_pre_allocate_enable() -> bool {
        DefaultLogConfigProperties::default().build_log_pre_allocate_enable().unwrap()
    }

    pub fn default_retention_bytes() -> i64 {
        DefaultLogConfigProperties::default().build_log_retention_bytes().unwrap()
    }

    pub fn default_retention_ms() -> i64 {
        DefaultLogConfigProperties::default().build_log_retention_time_millis().unwrap()
    }

    pub fn default_segment_bytes() -> usize {
        DefaultLogConfigProperties::default().build_log_segment_bytes().unwrap()
    }

    pub fn default_segment_index_bytes() -> usize {
        DefaultLogConfigProperties::default().build_log_index_size_max_bytes().unwrap()
    }

    pub fn default_segment_jitter_ms() -> i64 {
        DefaultLogConfigProperties::default().build_log_roll_time_jitter_millis().unwrap()
    }

    pub fn default_segment_ms() -> i64 {
        DefaultLogConfigProperties::default().build_log_roll_time_millis().unwrap()
    }

    pub fn default_unclean_leader_election_enable() -> bool {
        ReplicationConfigProperties::default().build_unclean_leader_election_enable().unwrap()
    }

    // Validators
    pub fn validate_cleanup_policy(&self) -> Result<(), KafkaConfigError> {
        self.cleanup_policy.validate_value_in_list(vec![
            &LogCleanupPolicy::Delete.to_string(),
            &LogCleanupPolicy::Compact.to_string(),
        ])
    }

    pub fn validate_delete_retention_ms(&self) -> Result<(), KafkaConfigError> {
        self.delete_retention_ms.validate_at_least(0)
    }

    pub fn validate_file_delete_delay_ms(&self) -> Result<(), KafkaConfigError> {
        self.file_delete_delay_ms.validate_at_least(0)
    }

    pub fn validate_flush_messages(&self) -> Result<(), KafkaConfigError> {
        self.flush_messages.validate_at_least(0)
    }

    pub fn validate_flush_ms(&self) -> Result<(), KafkaConfigError> {
        self.flush_ms.validate_at_least(0)
    }

    pub fn validate_follower_replication_throttled_replicas(&self) -> Result<(), KafkaConfigError> {
        match self.follower_replication_throttled_replicas.value_as_ref() {
            Some(val) => ThrottledReplicaListValidator::ensure_valid_string(
                FOLLOWER_REPLICATION_THROTTLED_REPLICAS_PROP,
                val,
            ),
            None => Ok(()),
        }
    }

    pub fn validate_index_interval_bytes(&self) -> Result<(), KafkaConfigError> {
        self.index_interval_bytes.validate_at_least(0)
    }

    pub fn validate_leader_replication_throttled_replicas(&self) -> Result<(), KafkaConfigError> {
        match self.leader_replication_throttled_replicas.value_as_ref() {
            Some(val) => ThrottledReplicaListValidator::ensure_valid_string(
                LEADER_REPLICATION_THROTTLED_REPLICAS_PROP,
                val,
            ),
            None => Ok(()),
        }
    }

    pub fn validate_max_compaction_lag_ms(&self) -> Result<(), KafkaConfigError> {
        self.max_compaction_lag_ms.validate_at_least(1)
    }

    pub fn validate_max_message_bytes(&self) -> Result<(), KafkaConfigError> {
        self.max_message_bytes.validate_at_least(0)
    }

    pub fn validate_message_format_version(&self) -> Result<(), KafkaConfigError> {
        match self.message_format_version.value_as_ref() {
            Some(val) => {
                // XXX: This shouldn't need a String clone()
                ApiVersionValidator::ensure_valid(&self.message_format_version.key, val.clone())
            },
            None => Ok(()),
        }
    }

    pub fn validate_message_timestamp_difference_max_ms(&self) -> Result<(), KafkaConfigError> {
        self.message_timestamp_difference_max_ms.validate_at_least(0)
    }

    pub fn validate_message_timestamp_type(&self) -> Result<(), KafkaConfigError> {
        self.message_timestamp_type.validate_value_in_list(vec![
            &LogMessageTimestampType::CreateTime.to_string(),
            &LogMessageTimestampType::LogAppendTime.to_string(),
        ])
    }

    pub fn validate_min_cleanable_dirty_ratio(&self) -> Result<(), KafkaConfigError> {
        self.min_cleanable_dirty_ratio.validate_between(0., 1.)
    }

    pub fn validate_min_in_sync_replicas(&self) -> Result<(), KafkaConfigError> {
        self.min_in_sync_replicas.validate_at_least(1)
    }

    pub fn validate_retention_ms(&self) -> Result<(), KafkaConfigError> {
        self.retention_ms.validate_at_least(-1)
    }

    pub fn validate_segment_bytes(&self) -> Result<(), KafkaConfigError> {
        self.segment_bytes.validate_at_least(legacy_record::RECORD_OVERHEAD_V0)
    }

    pub fn validate_segment_index_bytes(&self) -> Result<(), KafkaConfigError> {
        self.segment_index_bytes.validate_at_least(4)
    }

    pub fn validate_segment_jitter_ms(&self) -> Result<(), KafkaConfigError> {
        self.segment_jitter_ms.validate_at_least(0)
    }

    pub fn validate_segment_ms(&self) -> Result<(), KafkaConfigError> {
        self.segment_ms.validate_at_least(1)
    }

    // Custom resolvers
    pub fn resolve_follower_replication_throttled_replicas(
        &self,
    ) -> Result<Vec<String>, KafkaConfigError> {
        self.follower_replication_throttled_replicas.validate()?;
        match self.follower_replication_throttled_replicas.get_value() {
            Some(val) => Ok(val.split(",").map(|val| val.trim().to_string()).collect()),
            None => Ok(vec![]),
        }
    }

    pub fn resolve_leader_replication_throttled_replicas(
        &self,
    ) -> Result<Vec<String>, KafkaConfigError> {
        self.leader_replication_throttled_replicas.validate()?;
        match self.leader_replication_throttled_replicas.get_value() {
            Some(val) => Ok(val.split(",").map(|val| val.trim().to_string()).collect()),
            None => Ok(vec![]),
        }
    }

    pub fn resolve_cleanup_policy(&mut self) -> Result<Vec<LogCleanupPolicy>, KafkaConfigError> {
        match self.cleanup_policy.get_value() {
            Some(val) => LogCleanupPolicy::from_str_to_vec(val),
            None => Ok(vec![]),
        }
    }

    pub fn resolve_compression_type(&mut self) -> Result<BrokerCompressionCodec, KafkaConfigError> {
        BrokerCompressionCodec::from_str(&self.compression_type.build()?)
    }

    pub fn resolve_message_format_version(&mut self) -> Result<KafkaApiVersion, KafkaConfigError> {
        KafkaApiVersion::from_str(&self.message_format_version.build()?)
    }

    // Custom builders, type conversion our deriver can't handle (yet?)

    pub fn build_message_format_version(&mut self) -> Result<KafkaApiVersion, KafkaConfigError> {
        self.validate_message_format_version()?;
        self.resolve_message_format_version()
    }

    pub fn build_compression_type(&mut self) -> Result<BrokerCompressionCodec, KafkaConfigError> {
        // There's no explicit validator, but transforming from Str to BrokerCompressionCodec does
        // it anyway.
        self.resolve_compression_type()
    }

    pub fn build_cleanup_policy(&mut self) -> Result<Vec<LogCleanupPolicy>, KafkaConfigError> {
        self.validate_cleanup_policy()?;
        self.resolve_cleanup_policy()
    }

    pub fn build_follower_replication_throttled_replicas(
        &mut self,
    ) -> Result<Vec<String>, KafkaConfigError> {
        self.validate_follower_replication_throttled_replicas()?;
        self.resolve_follower_replication_throttled_replicas()
    }

    pub fn build_leader_replication_throttled_replicas(
        &mut self,
    ) -> Result<Vec<String>, KafkaConfigError> {
        self.validate_leader_replication_throttled_replicas()?;
        self.resolve_leader_replication_throttled_replicas()
    }

    pub fn try_from(kafka_config: &KafkaConfig) -> Result<Self, KafkaConfigError> {
        // From KafkaServer copyKafkaConfigToLog
        // RAFKA TODO: Could use set_value() and avoid the ToString->FromStr
        let mut res = Self::default();

        res.try_set_property(SEGMENT_BYTES_PROP, &kafka_config.log.log_segment_bytes.to_string())?;
        res.try_set_property(SEGMENT_MS_PROP, &kafka_config.log.log_roll_time_millis.to_string())?;
        res.try_set_property(
            SEGMENT_JITTER_MS_PROP,
            &kafka_config.log.log_roll_time_jitter_millis.to_string(),
        )?;
        res.try_set_property(
            SEGMENT_INDEX_BYTES_PROP,
            &kafka_config.log.log_index_size_max_bytes.to_string(),
        )?;
        res.try_set_property(
            FLUSH_MESSAGES_PROP,
            &kafka_config.log.log_flush_interval_messages.to_string(),
        )?;
        res.try_set_property(FLUSH_MS_PROP, &kafka_config.log.log_flush_interval_ms.to_string())?;
        res.try_set_property(
            RETENTION_BYTES_PROP,
            &kafka_config.log.log_retention_bytes.to_string(),
        )?;
        res.try_set_property(
            RETENTION_MS_PROP,
            &kafka_config.log.log_retention_time_millis.to_string(),
        )?;
        res.try_set_property(
            MAX_MESSAGE_BYTES_PROP,
            &kafka_config.general.message_max_bytes.to_string(),
        )?;
        res.try_set_property(
            INDEX_INTERVAL_BYTES_PROP,
            &kafka_config.log.log_index_interval_bytes.to_string(),
        )?;
        res.try_set_property(
            DELETE_RETENTION_MS_PROP,
            &kafka_config.log.log_cleaner_delete_retention_ms.to_string(),
        )?;
        res.try_set_property(
            MIN_COMPACTION_LAG_MS_PROP,
            &kafka_config.log.log_cleaner_min_compaction_lag_ms.to_string(),
        )?;
        res.try_set_property(
            MAX_COMPACTION_LAG_MS_PROP,
            &kafka_config.log.log_cleaner_max_compaction_lag_ms.to_string(),
        )?;
        res.try_set_property(
            FILE_DELETE_DELAY_MS_PROP,
            &kafka_config.log.log_delete_delay_ms.to_string(),
        )?;
        res.try_set_property(
            MIN_CLEANABLE_DIRTY_RATIO_PROP,
            &kafka_config.log.log_cleaner_min_clean_ratio.to_string(),
        )?;
        res.try_set_property(
            CLEANUP_POLICY_PROP,
            &kafka_config
                .log
                .log_cleanup_policy
                .iter()
                .map(|val| val.to_string())
                .collect::<Vec<String>>()
                .join(","),
        )?;
        res.try_set_property(
            MIN_IN_SYNC_REPLICAS_PROP,
            &kafka_config.log.min_in_sync_replicas.to_string(),
        )?;
        res.try_set_property(
            COMPRESSION_TYPE_PROP,
            &kafka_config.log.compression_type.to_string(),
        )?;
        res.try_set_property(
            UNCLEAN_LEADER_ELECTION_ENABLE_PROP,
            &kafka_config.replication.unclean_leader_election_enable.to_string(),
        )?;
        res.try_set_property(
            PREALLOCATE_ENABLE_PROP,
            &kafka_config.log.log_pre_allocate_enable.to_string(),
        )?;
        res.try_set_property(
            MESSAGE_FORMAT_VERSION_PROP,
            &kafka_config.log.log_message_format_version.to_string(),
        )?;
        res.try_set_property(
            MESSAGE_TIMESTAMP_TYPE_PROP,
            &kafka_config.log.log_message_timestamp_type.to_string(),
        )?;
        res.try_set_property(
            MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_PROP,
            &kafka_config.log.log_message_timestamp_difference_max_ms.to_string(),
        )?;
        res.try_set_property(
            MESSAGE_DOWNCONVERSION_ENABLE_PROP,
            &kafka_config.log.log_message_down_conversion_enable.to_string(),
        )?;

        Ok(res)
    }
}

impl ConfigSet for LogConfigProperties {
    type ConfigType = LogConfig;

    fn resolve(&mut self) -> Result<Self::ConfigType, KafkaConfigError> {
        trace!("LogConfigProperties::resolve()");
        let segment_size = self.segment_bytes.build()?;
        let segment_ms = self.segment_ms.build()?;
        let segment_jitter_ms = self.segment_jitter_ms.build()?;
        let flush_ms = self.flush_ms.build()?;
        let max_index_size = self.segment_index_bytes.build()?;
        let flush_interval = self.flush_messages.build()?;
        let retention_size = self.retention_bytes.build()?;
        let retention_ms = self.retention_ms.build()?;
        let max_message_size = self.max_message_bytes.build()?;
        let index_interval = self.index_interval_bytes.build()?;
        let file_delete_delay_ms = self.file_delete_delay_ms.build()?;
        let delete_retention_ms = self.delete_retention_ms.build()?;
        let compaction_lag_ms = self.min_compaction_lag_ms.build()?;
        let max_compaction_lag_ms = self.max_compaction_lag_ms.build()?;
        let min_cleanable_ratio = self.min_cleanable_dirty_ratio.build()?;
        let cleanup_policy = self.resolve_cleanup_policy()?;
        let compact = cleanup_policy.contains(&LogCleanupPolicy::Compact);
        let delete = cleanup_policy.contains(&LogCleanupPolicy::Delete);
        let unclean_leader_election_enable = self.unclean_leader_election_enable.build()?;
        let min_in_sync_replicas = self.min_in_sync_replicas.build()?;
        let compression_type = self.resolve_compression_type()?;
        let preallocate = self.pre_allocate_enable.build()?;
        let message_format_version = self.resolve_message_format_version()?;
        let message_timestamp_type = self.message_timestamp_type.build()?;
        let message_timestamp_difference_max_ms =
            self.message_timestamp_difference_max_ms.build()?;
        let leader_replication_throttled_replicas =
            self.resolve_leader_replication_throttled_replicas()?;
        let follower_replication_throttled_replicas =
            self.resolve_follower_replication_throttled_replicas()?;
        let message_down_conversion_enable = self.message_down_conversion_enable.build()?;
        Ok(Self::ConfigType {
            segment_size,
            segment_ms,
            segment_jitter_ms,
            flush_ms,
            max_index_size,
            flush_interval,
            retention_size,
            retention_ms,
            max_message_size,
            index_interval,
            file_delete_delay_ms,
            delete_retention_ms,
            compaction_lag_ms,
            max_compaction_lag_ms,
            min_cleanable_ratio,
            compact,
            delete,
            unclean_leader_election_enable,
            min_in_sync_replicas,
            compression_type,
            preallocate,
            message_format_version,
            message_timestamp_type,
            message_timestamp_difference_max_ms,
            leader_replication_throttled_replicas,
            follower_replication_throttled_replicas,
            message_down_conversion_enable,
        })
    }
}

/// `LogConfig` is a topic-specific configuration, in constrant, the `DefaultLogConfig` is the
/// broker-general configs (i.e. if topics are created, their default values is DefaultLogConfig.
#[derive(Debug, Default, Clone)]
pub struct LogConfig {
    segment_size: usize,
    segment_ms: i64,
    segment_jitter_ms: i64,
    flush_ms: i64,
    max_index_size: usize,
    flush_interval: i64,
    retention_size: i64,
    retention_ms: i64,
    max_message_size: usize,
    index_interval: i32,
    file_delete_delay_ms: i64,
    delete_retention_ms: i64,
    /// Contains by default min_compaction_lag_ms
    compaction_lag_ms: i64,
    max_compaction_lag_ms: i64,
    min_cleanable_ratio: f64,
    compact: bool,
    delete: bool,
    unclean_leader_election_enable: bool,
    min_in_sync_replicas: i32,
    compression_type: BrokerCompressionCodec,
    preallocate: bool,
    message_format_version: KafkaApiVersion,
    message_timestamp_type: String,
    message_timestamp_difference_max_ms: i64,
    leader_replication_throttled_replicas: Vec<String>,
    follower_replication_throttled_replicas: Vec<String>,
    message_down_conversion_enable: bool,
}

impl LogConfig {
    // XXX: Move to LogConfigProperties as part of ConfigSet trait
    pub fn validate_values(&self) -> Result<(), KafkaConfigError> {
        if self.compaction_lag_ms > self.max_compaction_lag_ms {
            return Err(KafkaConfigError::InvalidValue(format!(
                "conflict topic config setting {} ({}) > {} ({})",
                MIN_COMPACTION_LAG_MS_CONFIG,
                self.compaction_lag_ms,
                MAX_COMPACTION_LAG_MS_CONFIG,
                self.max_compaction_lag_ms,
            )));
        }
        Ok(())
    }

    /// Create a log config instance coalescing the per-broker log properties with the
    /// zookeeper per-topic configurations, previously known as from_props
    pub fn coalesce_broker_defaults_and_per_topic_override(
        kafka_config: &KafkaConfig,
        topic_overrides: HashMap<String, String>,
    ) -> Result<Self, KafkaConfigError> {
        let mut broker_defaults = LogConfigProperties::try_from(&kafka_config)?;

        let mut overridden_keys: Vec<String> = vec![];
        for (property_name, property_value) in &topic_overrides {
            broker_defaults.try_set_property(&property_name, &property_value)?;
            overridden_keys.push(property_name.to_string());
        }
        info!("Overridden Keys for topic: {:?}", overridden_keys);
        let res = broker_defaults.build()?;
        res.validate_values()?;
        Ok(res)
    }
}
