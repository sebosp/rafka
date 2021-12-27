//! From core/src/main/scala/kafka/log/LogConfig.scala

use crate::api::api_version::{ApiVersionValidator, KafkaApiVersion};
use crate::common::config::topic_config::*;
use crate::common::config_def::{ConfigDef, ConfigDefImportance, Validator};
use crate::common::record::legacy_record;
use crate::message::compression_codec::BrokerCompressionCodec;
use crate::server::config_handler::ThrottledReplicaListValidator;
use crate::server::kafka_config::general::GeneralConfigProperties;
use crate::server::kafka_config::log::{
    DefaultLogConfig, DefaultLogConfigProperties, LogCleanupPolicy, LogMessageTimestampType,
};
use crate::server::kafka_config::replication::ReplicationConfigProperties;
use crate::server::kafka_config::transaction_management::DEFAULT_COMPRESSION_TYPE;
use crate::server::kafka_config::{ConfigSet, KafkaConfigError};
use enum_iterator::IntoEnumIterator;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt;
use std::str::FromStr;
use tracing::info;

// nvim .define() lines copied and translated like:
// s/^\s*.define(\([^,]*\)Prop, \([^,]*\), .*\([LM][OE][^,]*\),.*/\="0pub const ".
// Uppercase(submatch(1))."_PROP :&str =
// ".Uppercase(submatch(1))."_CONFIG;\n1".Mixedcase(submatch(1)).",\n2Self::".
// Mixedcase(submatch(1))." => write!(f, \"{}\",
// ".Uppercase(submatch(1))."),\n3".Uppercase(submatch(1))."_PROP =>
// Ok(Self::".Snakecase(submatch(1))."),\n4".Snakecase(submatch(1)).":
// ConfigDef<".submatch(2).">,\n5".Snakecase(submatch(1)).": ConfigDef::default()\n6
// .with_doc(".Uppercase(submatch(1))."_DOC)\n7    .with_default(XXX.to_string())\n8
// Self::ConfigKey::".Mixedcase(submatch(1)). " =>
// self.".Snakecase(submatch(1)).".try_set_parsed_val ue(property_value)?,\n9".
// Snakecase(submatch(1)).": ".submatch(2).",

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

#[derive(Debug, IntoEnumIterator)]
pub enum LogConfigKey {
    CleanupPolicy,
    CompressionType,
    DeleteRetentionMs,
    FileDeleteDelayMs,
    FlushMessages,
    FlushMs,
    FollowerReplicationThrottledReplicas,
    IndexIntervalBytes,
    LeaderReplicationThrottledReplicas,
    MaxCompactionLagMs,
    MaxMessageBytes,
    MessageDownConversionEnable,
    MessageFormatVersion,
    MessageTimestampDifferenceMaxMs,
    MessageTimestampType,
    MinCleanableDirtyRatio,
    MinCompactionLagMs,
    MinInSyncReplicas,
    PreAllocateEnable,
    RetentionBytes,
    RetentionMs,
    SegmentBytes,
    SegmentIndexBytes,
    SegmentJitterMs,
    SegmentMs,
    UncleanLeaderElectionEnable,
}

impl fmt::Display for LogConfigKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CleanupPolicy => write!(f, "{}", CLEANUP_POLICY_PROP),
            Self::CompressionType => write!(f, "{}", COMPRESSION_TYPE_PROP),
            Self::DeleteRetentionMs => write!(f, "{}", DELETE_RETENTION_MS_PROP),
            Self::FileDeleteDelayMs => write!(f, "{}", FILE_DELETE_DELAY_MS_PROP),
            Self::FlushMessages => write!(f, "{}", FLUSH_MESSAGES_PROP),
            Self::FlushMs => write!(f, "{}", FLUSH_MS_PROP),
            Self::FollowerReplicationThrottledReplicas => {
                write!(f, "{}", FOLLOWER_REPLICATION_THROTTLED_REPLICAS_PROP)
            },
            Self::IndexIntervalBytes => write!(f, "{}", INDEX_INTERVAL_BYTES_PROP),
            Self::LeaderReplicationThrottledReplicas => {
                write!(f, "{}", LEADER_REPLICATION_THROTTLED_REPLICAS_PROP)
            },
            Self::MaxCompactionLagMs => write!(f, "{}", MAX_COMPACTION_LAG_MS_PROP),
            Self::MaxMessageBytes => write!(f, "{}", MAX_MESSAGE_BYTES_PROP),
            Self::MessageDownConversionEnable => {
                write!(f, "{}", MESSAGE_DOWNCONVERSION_ENABLE_PROP)
            },
            Self::MessageFormatVersion => write!(f, "{}", MESSAGE_FORMAT_VERSION_PROP),
            Self::MessageTimestampDifferenceMaxMs => {
                write!(f, "{}", MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_PROP)
            },
            Self::MessageTimestampType => write!(f, "{}", MESSAGE_TIMESTAMP_TYPE_PROP),
            Self::MinCleanableDirtyRatio => write!(f, "{}", MIN_CLEANABLE_DIRTY_RATIO_PROP),
            Self::MinCompactionLagMs => write!(f, "{}", MIN_COMPACTION_LAG_MS_PROP),
            Self::MinInSyncReplicas => write!(f, "{}", MIN_IN_SYNC_REPLICAS_PROP),
            Self::PreAllocateEnable => write!(f, "{}", PREALLOCATE_ENABLE_PROP),
            Self::RetentionBytes => write!(f, "{}", RETENTION_BYTES_PROP),
            Self::RetentionMs => write!(f, "{}", RETENTION_MS_PROP),
            Self::SegmentBytes => write!(f, "{}", SEGMENT_BYTES_PROP),
            Self::SegmentIndexBytes => write!(f, "{}", SEGMENT_INDEX_BYTES_PROP),
            Self::SegmentJitterMs => write!(f, "{}", SEGMENT_JITTER_MS_PROP),
            Self::SegmentMs => write!(f, "{}", SEGMENT_MS_PROP),
            Self::UncleanLeaderElectionEnable => {
                write!(f, "{}", UNCLEAN_LEADER_ELECTION_ENABLE_PROP)
            },
        }
    }
}

impl FromStr for LogConfigKey {
    type Err = KafkaConfigError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            CLEANUP_POLICY_PROP => Ok(Self::CleanupPolicy),
            COMPRESSION_TYPE_PROP => Ok(Self::CompressionType),
            DELETE_RETENTION_MS_PROP => Ok(Self::DeleteRetentionMs),
            FILE_DELETE_DELAY_MS_PROP => Ok(Self::FileDeleteDelayMs),
            FLUSH_MESSAGES_PROP => Ok(Self::FlushMessages),
            FLUSH_MS_PROP => Ok(Self::FlushMs),
            FOLLOWER_REPLICATION_THROTTLED_REPLICAS_PROP => {
                Ok(Self::FollowerReplicationThrottledReplicas)
            },
            INDEX_INTERVAL_BYTES_PROP => Ok(Self::IndexIntervalBytes),
            LEADER_REPLICATION_THROTTLED_REPLICAS_PROP => {
                Ok(Self::LeaderReplicationThrottledReplicas)
            },
            MAX_COMPACTION_LAG_MS_PROP => Ok(Self::MaxCompactionLagMs),
            MAX_MESSAGE_BYTES_PROP => Ok(Self::MaxMessageBytes),
            MESSAGE_DOWNCONVERSION_ENABLE_PROP => Ok(Self::MessageDownConversionEnable),
            MESSAGE_FORMAT_VERSION_PROP => Ok(Self::MessageFormatVersion),
            MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_PROP => Ok(Self::MessageTimestampDifferenceMaxMs),
            MESSAGE_TIMESTAMP_TYPE_PROP => Ok(Self::MessageTimestampType),
            MIN_CLEANABLE_DIRTY_RATIO_PROP => Ok(Self::MinCleanableDirtyRatio),
            MIN_COMPACTION_LAG_MS_PROP => Ok(Self::MinCompactionLagMs),
            MIN_IN_SYNC_REPLICAS_PROP => Ok(Self::MinInSyncReplicas),
            PREALLOCATE_ENABLE_PROP => Ok(Self::PreAllocateEnable),
            RETENTION_BYTES_PROP => Ok(Self::RetentionBytes),
            RETENTION_MS_PROP => Ok(Self::RetentionMs),
            SEGMENT_BYTES_PROP => Ok(Self::SegmentBytes),
            SEGMENT_INDEX_BYTES_PROP => Ok(Self::SegmentIndexBytes),
            SEGMENT_JITTER_MS_PROP => Ok(Self::SegmentJitterMs),
            SEGMENT_MS_PROP => Ok(Self::SegmentMs),
            UNCLEAN_LEADER_ELECTION_ENABLE_PROP => Ok(Self::UncleanLeaderElectionEnable),
            _ => Err(KafkaConfigError::UnknownKey(input.to_string())),
        }
    }
}

#[derive(Debug)]
pub struct LogConfigProperties {
    cleanup_policy: ConfigDef<String>,
    compression_type: ConfigDef<String>,
    delete_retention_ms: ConfigDef<i64>,
    file_delete_delay_ms: ConfigDef<i64>,
    flush_messages: ConfigDef<i64>,
    flush_ms: ConfigDef<i64>,
    // TODO: transform Vec<> to String and build the resulting Vec<> on build()
    follower_replication_throttled_replicas: ConfigDef<String>,
    index_interval_bytes: ConfigDef<i32>,
    // TODO: transform Vec<> to String and build the resulting Vec<> on build()
    leader_replication_throttled_replicas: ConfigDef<String>,
    max_compaction_lag_ms: ConfigDef<i64>,
    max_message_bytes: ConfigDef<usize>,
    message_down_conversion_enable: ConfigDef<bool>,
    message_format_version: ConfigDef<String>,
    message_timestamp_difference_max_ms: ConfigDef<i64>,
    message_timestamp_type: ConfigDef<String>,
    min_cleanable_dirty_ratio: ConfigDef<f64>,
    min_compaction_lag_ms: ConfigDef<i64>,
    min_in_sync_replicas: ConfigDef<i32>,
    pre_allocate_enable: ConfigDef<bool>,
    retention_bytes: ConfigDef<i64>,
    retention_ms: ConfigDef<i64>,
    segment_bytes: ConfigDef<usize>,
    segment_index_bytes: ConfigDef<usize>,
    segment_jitter_ms: ConfigDef<i64>,
    segment_ms: ConfigDef<i64>,
    unclean_leader_election_enable: ConfigDef<bool>,
}

impl Default for LogConfigProperties {
    fn default() -> Self {
        let broker_default_log_properties = DefaultLogConfigProperties::default();
        let general_properties = GeneralConfigProperties::default();
        let replication_properties = ReplicationConfigProperties::default();
        Self {
            cleanup_policy: ConfigDef::default()
                .with_key(CLEANUP_POLICY_CONFIG)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(CLEANUP_POLICY_DOC.to_string())
                .with_default(LogCleanupPolicy::Delete.to_string())
                .with_validator(Box::new(|data| {
                    // Safe to unwrap, we have a default
                    ConfigDef::value_in_list(
                        data,
                        vec![
                            &LogCleanupPolicy::Delete.to_string(),
                            &LogCleanupPolicy::Compact.to_string(),
                        ],
                        CLEANUP_POLICY_CONFIG,
                    )
                })),
            compression_type: ConfigDef::default()
                .with_key(COMPRESSION_TYPE_CONFIG)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(COMPRESSION_TYPE_DOC.to_string())
                .with_default(DEFAULT_COMPRESSION_TYPE.name.to_string())
                .with_validator(Box::new(|data| {
                    // Safe to unwrap, we have a default
                    ConfigDef::value_in_list(
                        data,
                        BrokerCompressionCodec::broker_compression_options()
                            .iter()
                            .map(|val| &val.to_string())
                            .collect(),
                        COMPRESSION_TYPE_CONFIG,
                    )
                })),
            delete_retention_ms: ConfigDef::default()
                .with_key(DELETE_RETENTION_MS_CONFIG)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(DELETE_RETENTION_MS_DOC.to_string())
                .with_default(
                    broker_default_log_properties.log_cleaner_delete_retention_ms.build().unwrap(),
                )
                .with_validator(Box::new(|data| {
                    // Safe to unwrap, we have a default
                    ConfigDef::at_least(data, &0, DELETE_RETENTION_MS_CONFIG)
                })),
            file_delete_delay_ms: ConfigDef::default()
                .with_key(FILE_DELETE_DELAY_MS_CONFIG)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(FILE_DELETE_DELAY_MS_DOC.to_string())
                .with_default(broker_default_log_properties.log_delete_delay_ms.build().unwrap())
                .with_validator(Box::new(|data| {
                    // Safe to unwrap, we have a default
                    ConfigDef::at_least(data, &0, FILE_DELETE_DELAY_MS_CONFIG)
                })),
            flush_messages: ConfigDef::default()
                .with_key(FLUSH_MESSAGES_INTERVAL_CONFIG)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(FLUSH_MESSAGES_INTERVAL_DOC.to_string())
                .with_default(
                    broker_default_log_properties.log_flush_interval_messages.build().unwrap(),
                )
                .with_validator(Box::new(|data| {
                    // Safe to unwrap, we have a default
                    ConfigDef::at_least(data, &0, FILE_DELETE_DELAY_MS_CONFIG)
                })),
            flush_ms: ConfigDef::default()
                .with_key(FLUSH_MS_CONFIG)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(FLUSH_MS_DOC.to_string())
                .with_default(
                    broker_default_log_properties.log_flush_scheduler_interval_ms.build().unwrap(),
                )
                .with_validator(Box::new(|data| {
                    // Safe to unwrap, we have a default
                    ConfigDef::at_least(data, &0, FILE_DELETE_DELAY_MS_CONFIG)
                })),
            follower_replication_throttled_replicas: ConfigDef::default()
                .with_key(FOLLOWER_REPLICATION_THROTTLED_REPLICAS_PROP)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(String::from(
                    "A list of replicas for which log replication should be throttled on the \
                     follower side. The list should describe a set of replicas in the form \
                     [PartitionId]:[BrokerId],[PartitionId]:[BrokerId]:... or alternatively the \
                     wildcard '*' can be used to throttle all replicas for this topic.",
                ))
                .with_validator(Box::new(|data| match data {
                    Some(val) => ThrottledReplicaListValidator::ensure_valid_string(
                        FOLLOWER_REPLICATION_THROTTLED_REPLICAS_PROP,
                        val,
                    ),
                    None => Ok(()),
                })),
            index_interval_bytes: ConfigDef::default()
                .with_key(INDEX_INTERVAL_BYTES_PROP)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(INDEX_INTERVAL_BYTES_DOCS.to_string())
                .with_default(
                    broker_default_log_properties.log_index_interval_bytes.build().unwrap(),
                )
                .with_validator(Box::new(|data| {
                    // Safe to unwrap, we have a default
                    ConfigDef::at_least(data, &0, INDEX_INTERVAL_BYTES_CONFIG)
                })),
            leader_replication_throttled_replicas: ConfigDef::default()
                .with_key(LEADER_REPLICATION_THROTTLED_REPLICAS_PROP)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(String::from(
                    "A list of replicas for which log replication should be throttled on the \
                     leader side. The list should describe a set of replicas in the form \
                     [PartitionId]:[BrokerId],[PartitionId]:[BrokerId]:... or alternatively the \
                     wildcard '*' can be used to throttle all replicas for this topic.",
                ))
                .with_validator(Box::new(|data| match data {
                    Some(val) => ThrottledReplicaListValidator::ensure_valid_string(
                        FOLLOWER_REPLICATION_THROTTLED_REPLICAS_PROP,
                        val,
                    ),
                    None => Ok(()),
                })),
            max_compaction_lag_ms: ConfigDef::default()
                .with_key(MAX_COMPACTION_LAG_MS_CONFIG)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(MAX_COMPACTION_LAG_MS_DOC.to_string())
                .with_default(
                    broker_default_log_properties
                        .log_cleaner_max_compaction_lag_ms
                        .build()
                        .unwrap(),
                )
                .with_validator(Box::new(|data| {
                    // Safe to unwrap, we have a default
                    ConfigDef::at_least(data, &1, MAX_COMPACTION_LAG_MS_CONFIG)
                })),
            max_message_bytes: ConfigDef::default()
                .with_key(MAX_MESSAGE_BYTES_CONFIG)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(MAX_MESSAGE_BYTES_DOC.to_string())
                .with_default(general_properties.message_max_bytes.build().unwrap())
                .with_validator(Box::new(|data| {
                    // NOTE: This being a usize it cannot be lower than 0...
                    // Safe to unwrap, we have a default
                    ConfigDef::at_least(data, &0, MAX_COMPACTION_LAG_MS_CONFIG)
                })),
            message_down_conversion_enable: ConfigDef::default()
                .with_key(MESSAGE_DOWNCONVERSION_ENABLE_CONFIG)
                .with_importance(ConfigDefImportance::Low)
                .with_doc(MESSAGE_DOWNCONVERSION_ENABLE_DOC.to_string())
                .with_default(
                    broker_default_log_properties
                        .log_message_down_conversion_enable
                        .build()
                        .unwrap(),
                ),
            message_format_version: ConfigDef::default()
                .with_key(MESSAGE_FORMAT_VERSION_CONFIG)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(MESSAGE_FORMAT_VERSION_DOC.to_string())
                .with_default(
                    // TODO: Calling build() is a bad idea from outside, because there might be a
                    // resolve_<field> and we may just forget to call it... Perhaps one option is
                    // to always create a resolve_<field> and impl there the custom resolver, if
                    // there is no custom resolver, then simply call build()
                    broker_default_log_properties
                        .resolve_log_message_format_version()
                        .unwrap()
                        .to_string(),
                )
                .with_validator(Box::new(|data| {
                    // safe to unwrap, we have a default
                    ApiVersionValidator::ensure_valid(
                        MESSAGE_FORMAT_VERSION_CONFIG,
                        data.unwrap().clone(),
                    )
                })),
            message_timestamp_difference_max_ms: ConfigDef::default()
                .with_key(MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_DOC.to_string())
                .with_default(
                    broker_default_log_properties
                        .log_message_timestamp_difference_max_ms
                        .build()
                        .unwrap(),
                )
                .with_validator(Box::new(|data| {
                    // Safe to unwrap, we have a default
                    ConfigDef::at_least(data, &0, DELETE_RETENTION_MS_CONFIG)
                })),
            message_timestamp_type: ConfigDef::default()
                .with_key(MESSAGE_TIMESTAMP_TYPE_CONFIG)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(MESSAGE_TIMESTAMP_TYPE_DOC.to_string())
                .with_default(
                    broker_default_log_properties
                        .resolve_log_message_timestamp_type()
                        .unwrap()
                        .to_string(),
                )
                .with_validator(Box::new(|data| {
                    // RAFKA TODO: Technically the FromStr would take care of this, but it doesn't
                    // show the error on "should be in list [x, y]
                    ConfigDef::value_in_list(
                        data,
                        vec![
                            &LogMessageTimestampType::CreateTime.to_string(),
                            &LogMessageTimestampType::LogAppendTime.to_string(),
                        ],
                        MESSAGE_TIMESTAMP_TYPE_CONFIG,
                    )
                })),
            min_cleanable_dirty_ratio: ConfigDef::default()
                .with_key(MIN_CLEANABLE_DIRTY_RATIO_CONFIG)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(MIN_CLEANABLE_DIRTY_RATIO_DOC.to_string())
                .with_default(
                    broker_default_log_properties.log_cleaner_min_clean_ratio.build().unwrap(),
                )
                .with_validator(Box::new(|data| {
                    ConfigDef::between(data, &0f64, &1f64, MIN_CLEANABLE_DIRTY_RATIO_CONFIG)
                })),
            min_compaction_lag_ms: ConfigDef::default()
                .with_key(MIN_COMPACTION_LAG_MS_CONFIG)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(MIN_COMPACTION_LAG_MS_DOC.to_string())
                .with_default(
                    broker_default_log_properties
                        .log_cleaner_min_compaction_lag_ms
                        .build()
                        .unwrap(),
                ),
            min_in_sync_replicas: ConfigDef::default()
                .with_key(MIN_IN_SYNC_REPLICAS_CONFIG)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(MIN_IN_SYNC_REPLICAS_DOC.to_string())
                .with_default(broker_default_log_properties.min_in_sync_replicas.build().unwrap())
                .with_validator(Box::new(|data| {
                    // NOTE: This being a usize it cannot be lower than 0...
                    // Safe to unwrap, we have a default
                    ConfigDef::at_least(data, &1, MAX_COMPACTION_LAG_MS_CONFIG)
                })),
            pre_allocate_enable: ConfigDef::default()
                .with_key(PREALLOCATE_CONFIG)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(PREALLOCATE_DOC.to_string())
                .with_default(
                    broker_default_log_properties.log_pre_allocate_enable.build().unwrap(),
                ),
            retention_bytes: ConfigDef::default()
                .with_key(RETENTION_BYTES_CONFIG)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(RETENTION_BYTES_DOC.to_string())
                .with_default(broker_default_log_properties.log_retention_bytes.build().unwrap()),
            // can be negative. See kafka.log.LogManager.cleanupExpiredSegments
            retention_ms: ConfigDef::default()
                .with_key(RETENTION_MS_CONFIG)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(RETENTION_MS_DOC.to_string())
                .with_default(
                    broker_default_log_properties.resolve_log_retention_time_millis().unwrap(),
                )
                .with_validator(Box::new(|data| {
                    // Safe to unwrap, we have a default
                    ConfigDef::at_least(data, &-1, RETENTION_MS_CONFIG)
                })),
            segment_bytes: ConfigDef::default()
                .with_key(SEGMENT_BYTES_CONFIG)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(SEGMENT_BYTES_DOC.to_string())
                .with_default(broker_default_log_properties.log_segment_bytes.build().unwrap())
                .with_validator(Box::new(|data| {
                    // Safe to unwrap, we have a default
                    ConfigDef::at_least(
                        data,
                        &legacy_record::RECORD_OVERHEAD_V0,
                        SEGMENT_BYTES_CONFIG,
                    )
                })),
            segment_index_bytes: ConfigDef::default()
                .with_key(SEGMENT_INDEX_BYTES_CONFIG)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(SEGMENT_INDEX_BYTES_DOC.to_string())
                .with_default(
                    broker_default_log_properties.log_index_size_max_bytes.build().unwrap(),
                )
                .with_validator(Box::new(|data| {
                    // Safe to unwrap, we have a default
                    // RAFKA NOTE: Originally this should be greater than 0, but
                    // KafkaConfig::Log::LogIndexSizeMaxBytes requires at least 4, setting it to 4
                    // here, not sure...
                    ConfigDef::at_least(data, &4, SEGMENT_INDEX_BYTES_CONFIG)
                })),
            segment_jitter_ms: ConfigDef::default()
                .with_key(SEGMENT_JITTER_MS_CONFIG)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(SEGMENT_JITTER_MS_DOC.to_string())
                .with_default(
                    broker_default_log_properties.resolve_log_roll_time_jitter_millis().unwrap(),
                )
                .with_validator(Box::new(|data| {
                    // Safe to unwrap, we have a default
                    ConfigDef::at_least(data, &0, RETENTION_MS_CONFIG)
                })),
            segment_ms: ConfigDef::default()
                .with_key(SEGMENT_MS_CONFIG)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(SEGMENT_MS_DOC.to_string())
                .with_default(broker_default_log_properties.resolve_log_roll_time_millis().unwrap())
                .with_validator(Box::new(|data| {
                    // Safe to unwrap, we have a default
                    ConfigDef::at_least(data, &1, RETENTION_MS_CONFIG)
                })),
            unclean_leader_election_enable: ConfigDef::default()
                .with_key(UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(UNCLEAN_LEADER_ELECTION_ENABLE_DOC.to_string())
                .with_default(
                    replication_properties.resolve_unclean_leader_election_enable().unwrap(),
                ),
        }
    }
}

impl ConfigSet for LogConfigProperties {
    type ConfigKey = LogConfigKey;
    type ConfigType = LogConfig;

    /// `try_from_config_property` transforms a string value from the config into our actual types
    fn try_set_property(
        &mut self,
        property_name: &str,
        property_value: &str,
    ) -> Result<(), KafkaConfigError> {
        let kafka_config_key = Self::ConfigKey::from_str(property_name)?;
        match kafka_config_key {
            Self::ConfigKey::CleanupPolicy => {
                self.cleanup_policy.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::CompressionType => {
                self.compression_type.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::DeleteRetentionMs => {
                self.delete_retention_ms.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::FileDeleteDelayMs => {
                self.file_delete_delay_ms.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::FlushMessages => {
                self.flush_messages.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::FlushMs => self.flush_ms.try_set_parsed_value(property_value)?,
            Self::ConfigKey::FollowerReplicationThrottledReplicas => {
                self.follower_replication_throttled_replicas.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::IndexIntervalBytes => {
                self.index_interval_bytes.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::LeaderReplicationThrottledReplicas => {
                self.leader_replication_throttled_replicas.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::MaxCompactionLagMs => {
                self.max_compaction_lag_ms.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::MaxMessageBytes => {
                self.max_message_bytes.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::MessageDownConversionEnable => {
                self.message_down_conversion_enable.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::MessageFormatVersion => {
                self.message_format_version.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::MessageTimestampDifferenceMaxMs => {
                self.message_timestamp_difference_max_ms.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::MessageTimestampType => {
                self.message_timestamp_type.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::MinCleanableDirtyRatio => {
                self.min_cleanable_dirty_ratio.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::MinCompactionLagMs => {
                self.min_compaction_lag_ms.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::MinInSyncReplicas => {
                self.min_in_sync_replicas.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::PreAllocateEnable => {
                self.pre_allocate_enable.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::RetentionBytes => {
                self.retention_bytes.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::RetentionMs => {
                self.retention_ms.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::SegmentBytes => {
                self.segment_bytes.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::SegmentIndexBytes => {
                self.segment_index_bytes.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::SegmentJitterMs => {
                self.segment_jitter_ms.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::SegmentMs => self.segment_ms.try_set_parsed_value(property_value)?,
            Self::ConfigKey::UncleanLeaderElectionEnable => {
                self.unclean_leader_election_enable.try_set_parsed_value(property_value)?
            },
        };
        Ok(())
    }

    fn build(&mut self) -> Result<Self::ConfigType, KafkaConfigError> {
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

impl LogConfigProperties {
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

    pub fn resolve_message_format_version(&self) -> Result<KafkaApiVersion, KafkaConfigError> {
        KafkaApiVersion::from_str(&self.message_format_version.build()?)
    }
}

/// `LogConfig` is a topic-specific configuration, in constrant, the `DefaultLogConfig` is the
/// broker-general configs (i.e. if topics are created, their default values is DefaultLogConfig.
#[derive(Debug, Default)]
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
    /// zookeeper per-topic configurations
    pub fn coalesce_broker_defaults_and_per_topic_override(
        defaults: LogConfig,
        topic_overrides: HashMap<String, String>,
    ) -> Result<Self, KafkaConfigError> {
        let log_config_properties = LogConfigProperties::default();
        for (property_name, property_value) in topic_overrides {
            log_config_properties.try_set_property(&property_name, &property_value);
        }
        let overridden_keys: Vec<String> =
            topic_overrides.iter().map(|(&name, &val)| val.to_string()).collect();
        info!("Overridden Keys for topic: {:?}", overridden_keys);
        log_config_properties.build()
    }
}

impl TryFrom<DefaultLogConfig> for LogConfig {
    type Error = KafkaConfigError;

    fn try_from(general_config: DefaultLogConfig) -> Result<Self, Self::Error> {
        // From KafkaServer copyKafkaConfigToLog
        let mut res = Self::default();
        res.segment_size = general_config.log_segment_bytes;

        // logProps.put(LogConfig.SegmentBytesProp, kafkaConfig.logSegmentBytes)
        // logProps.put(LogConfig.SegmentMsProp, kafkaConfig.logRollTimeMillis)
        // logProps.put(LogConfig.SegmentJitterMsProp, kafkaConfig.logRollTimeJitterMillis)
        // logProps.put(LogConfig.SegmentIndexBytesProp, kafkaConfig.logIndexSizeMaxBytes)
        // logProps.put(LogConfig.FlushMessagesProp, kafkaConfig.logFlushIntervalMessages)
        // logProps.put(LogConfig.FlushMsProp, kafkaConfig.logFlushIntervalMs)
        // logProps.put(LogConfig.RetentionBytesProp, kafkaConfig.logRetentionBytes)
        // logProps.put(LogConfig.RetentionMsProp, kafkaConfig.logRetentionTimeMillis)
        // logProps.put(LogConfig.MaxMessageBytesProp, kafkaConfig.messageMaxBytes)
        // logProps.put(LogConfig.IndexIntervalBytesProp, kafkaConfig.logIndexIntervalBytes)
        // logProps.put(LogConfig.DeleteRetentionMsProp, kafkaConfig.logCleanerDeleteRetentionMs)
        // logProps.put(LogConfig.MinCompactionLagMsProp, kafkaConfig.logCleanerMinCompactionLagMs)
        // logProps.put(LogConfig.MaxCompactionLagMsProp, kafkaConfig.logCleanerMaxCompactionLagMs)
        // logProps.put(LogConfig.FileDeleteDelayMsProp, kafkaConfig.logDeleteDelayMs)
        // logProps.put(LogConfig.MinCleanableDirtyRatioProp, kafkaConfig.logCleanerMinCleanRatio)
        // logProps.put(LogConfig.CleanupPolicyProp, kafkaConfig.logCleanupPolicy)
        // logProps.put(LogConfig.MinInSyncReplicasProp, kafkaConfig.minInSyncReplicas)
        // logProps.put(LogConfig.CompressionTypeProp, kafkaConfig.compressionType)
        // logProps.put(LogConfig.UncleanLeaderElectionEnableProp,
        // kafkaConfig.uncleanLeaderElectionEnable) logProps.put(LogConfig.
        // PreAllocateEnableProp, kafkaConfig.logPreAllocateEnable) logProps.put(LogConfig.
        // MessageFormatVersionProp, kafkaConfig.logMessageFormatVersion.version)
        // logProps.put(LogConfig.MessageTimestampTypeProp,
        // kafkaConfig.logMessageTimestampType.name) logProps.put(LogConfig.
        // MessageTimestampDifferenceMaxMsProp, kafkaConfig.logMessageTimestampDifferenceMaxMs)
        // logProps.put(LogConfig.MessageDownConversionEnableProp,
        // kafkaConfig.logMessageDownConversionEnable)

        res.validate_values()?;
        Ok(res)
    }
}
