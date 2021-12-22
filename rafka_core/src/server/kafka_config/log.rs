//! Kafka Config - Broker Default Log Configuration
//! This configuration, read from server.properties, relates to the Default/General Broker-wide
//! configuration. A topic may be individually configured via zookeeper, see
//! `crate::log::log_config`

use super::quota::PRODUCER_QUOTA_BYTES_PER_SECOND_DEFAULT_PROP;
use super::{ConfigSet, KafkaConfigError};
use crate::api::api_version::{ApiVersion, KafkaApiVersion};
use crate::common::config::topic_config::MESSAGE_DOWNCONVERSION_ENABLE_DOC;
use crate::common::config_def::{ConfigDef, ConfigDefImportance};
use crate::common::record::legacy_record;
use const_format::concatcp;
use enum_iterator::IntoEnumIterator;
use std::fmt;
use std::str::FromStr;
use tracing::warn;

pub const LOG_CONFIG_PREFIX: &str = "log.";

pub const LOG_DIR_PROP: &str = "log.dir";
pub const LOG_DIRS_PROP: &str = "log.dirs";
pub const LOG_SEGMENT_BYTES_PROP: &str = "log.segment.bytes";
pub const LOG_ROLL_TIME_MILLIS_PROP: &str = "log.roll.ms";
pub const LOG_ROLL_TIME_HOURS_PROP: &str = "log.roll.hours";
pub const LOG_ROLL_TIME_JITTER_MILLIS_PROP: &str = "log.roll.jitter.ms";
pub const LOG_ROLL_TIME_JITTER_HOURS_PROP: &str = "log.roll.jitter.hours";
pub const LOG_RETENTION_TIME_MILLIS_PROP: &str = "log.retention.ms";
pub const LOG_RETENTION_TIME_MINUTES_PROP: &str = "log.retention.minutes";
pub const LOG_RETENTION_TIME_HOURS_PROP: &str = "log.retention.hours";
pub const LOG_CLEANUP_INTERVAL_MS_PROP: &str = "log.retention.check.interval.ms";
pub const LOG_CLEANUP_POLICY_PROP: &str = "log.cleanup.policy";
pub const LOG_CLEANER_THREADS_PROP: &str = "log.cleaner.threads";
pub const LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP: &str = "log.cleaner.dedupe.buffer.size";
pub const LOG_CLEANER_DEDUPE_BUFFER_LOAD_FACTOR_PROP: &str = "log.cleaner.io.buffer.load.factor";
pub const LOG_CLEANER_IO_MAX_BYTES_PER_SECOND_PROP: &str = "log.cleaner.io.max.bytes.per.second";
pub const LOG_CLEANER_BACKOFF_MS_PROP: &str = "log.cleaner.backoff.ms";
pub const LOG_CLEANER_MIN_CLEAN_RATIO_PROP: &str = "log.cleaner.min.cleanable.ratio";
pub const LOG_CLEANER_ENABLE_PROP: &str = "log.cleaner.enable";
pub const LOG_CLEANER_DELETE_RETENTION_MS_PROP: &str = "log.cleaner.delete.retention.ms";
pub const LOG_CLEANER_MIN_COMPACTION_LAG_MS_PROP: &str = "log.cleaner.min.compaction.lag.ms";
pub const LOG_CLEANER_MAX_COMPACTION_LAG_MS_PROP: &str = "log.cleaner.max.compaction.lag.ms";
pub const LOG_INDEX_INTERVAL_BYTES_PROP: &str = "log.index.interval.bytes";
pub const LOG_FLUSH_INTERVAL_MESSAGES_PROP: &str = "log.flush.interval.messages";
pub const LOG_DELETE_DELAY_MS_PROP: &str = "log.segment.delete.delay.ms";
pub const LOG_CLEANER_IO_BUFFER_SIZE_PROP: &str = "log.cleaner.io.buffer.size";
pub const LOG_FLUSH_SCHEDULER_INTERVAL_MS_PROP: &str = "log.flush.scheduler.interval.ms";
pub const LOG_FLUSH_INTERVAL_MS_PROP: &str = "log.flush.interval.ms";
pub const LOG_FLUSH_OFFSET_CHECKPOINT_INTERVAL_MS_PROP: &str =
    "log.flush.offset.checkpoint.interval.ms";
pub const LOG_FLUSH_START_OFFSET_CHECKPOINT_INTERVAL_MS_PROP: &str =
    "log.flush.start.offset.checkpoint.interval.ms";
pub const LOG_PRE_ALLOCATE_PROP: &str = "log.preallocate";
pub const LOG_MESSAGE_FORMAT_VERSION_PROP: &str =
    concatcp!(LOG_CONFIG_PREFIX, "message.format.version");
pub const LOG_MESSAGE_TIMESTAMP_TYPE_PROP: &str =
    concatcp!(LOG_CONFIG_PREFIX, "message.timestamp.type");
pub const LOG_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_PROP: &str =
    concatcp!(LOG_CONFIG_PREFIX, "message.timestamp.difference.max.ms");
pub const NUM_RECOVERY_THREADS_PER_DATA_DIR_PROP: &str = "num.recovery.threads.per.data.dir";
pub const MIN_IN_SYNC_REPLICAS_PROP: &str = "min.insync.replicas";
pub const LOG_MESSAGE_DOWN_CONVERSION_ENABLE_PROP: &str =
    concatcp!(LOG_CONFIG_PREFIX, "message.downconversion.enable");

// RAFKA TODO: This is a topic property, should be moved to its proper place
#[derive(Debug, Clone, PartialEq)]
pub enum LogCleanupPolicy {
    Delete,
    Compact,
}

impl FromStr for LogCleanupPolicy {
    type Err = KafkaConfigError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            "delete" => Ok(Self::Delete),
            "compact" => Ok(Self::Compact),
            _ => Err(KafkaConfigError::UnknownCleanupPolicy(input.to_string())),
        }
    }
}

impl fmt::Display for LogCleanupPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Delete => write!(f, "delete"),
            Self::Compact => write!(f, "compact"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum LogMessageTimestampType {
    CreateTime,
    LogAppendTime,
}

impl FromStr for LogMessageTimestampType {
    type Err = KafkaConfigError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            "CreateTime" => Ok(Self::CreateTime),
            "LogAppendTime" => Ok(Self::LogAppendTime),
            _ => Err(KafkaConfigError::UnknownCleanupPolicy(input.to_string())),
        }
    }
}

impl fmt::Display for LogMessageTimestampType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CreateTime => write!(f, "CreateTime"),
            Self::LogAppendTime => write!(f, "LogAppendTime"),
        }
    }
}

impl Default for LogMessageTimestampType {
    fn default() -> Self {
        Self::CreateTime
    }
}

#[derive(Debug, IntoEnumIterator)]
pub enum DefaultLogConfigKey {
    LogDir,
    LogDirs,
    LogSegmentBytes,
    LogRollTimeMillis,
    LogRollTimeHours,
    LogRollTimeJitterMillis,
    LogRollTimeJitterHours,
    LogRetentionTimeMillis,
    LogRetentionTimeMinutes,
    LogRetentionTimeHours,
    LogCleanupIntervalMs,
    LogCleanupPolicy,
    LogCleanerThreads,
    LogCleanerDedupeBufferSize,
    LogCleanerIoBufferSize,
    LogCleanerDedupeBufferLoadFactor,
    LogCleanerIoMaxBytesPerSecond,
    LogCleanerBackoffMs,
    LogCleanerMinCleanRatio,
    LogCleanerEnable,
    LogCleanerDeleteRetentionMs,
    LogCleanerMinCompactionLagMs,
    LogCleanerMaxCompactionLagMs,
    LogIndexIntervalBytes,
    LogFlushIntervalMessages,
    LogDeleteDelayMs,
    LogFlushSchedulerIntervalMs,
    LogFlushIntervalMs,
    LogFlushOffsetCheckpointIntervalMs,
    LogFlushStartOffsetCheckpointIntervalMs,
    LogPreAllocateEnable,
    LogMessageFormatVersion,
    LogMessageTimestampType,
    LogMessageTimestampDifferenceMaxMs,
    NumRecoveryThreadsPerDataDir,
    MinInSyncReplicas,
    LogMessageDownConversionEnable,
}

impl fmt::Display for DefaultLogConfigKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LogDir => write!(f, "{}", LOG_DIR_PROP),
            Self::LogDirs => write!(f, "{}", LOG_DIRS_PROP),
            Self::LogSegmentBytes => write!(f, "{}", LOG_SEGMENT_BYTES_PROP),
            Self::LogRollTimeMillis => write!(f, "{}", LOG_ROLL_TIME_MILLIS_PROP),
            Self::LogRollTimeHours => write!(f, "{}", LOG_ROLL_TIME_HOURS_PROP),
            Self::LogRollTimeJitterMillis => write!(f, "{}", LOG_ROLL_TIME_JITTER_MILLIS_PROP),
            Self::LogRollTimeJitterHours => write!(f, "{}", LOG_ROLL_TIME_JITTER_HOURS_PROP),
            Self::LogRetentionTimeMillis => write!(f, "{}", LOG_RETENTION_TIME_MILLIS_PROP),
            Self::LogRetentionTimeMinutes => write!(f, "{}", LOG_RETENTION_TIME_MINUTES_PROP),
            Self::LogRetentionTimeHours => write!(f, "{}", LOG_RETENTION_TIME_HOURS_PROP),
            Self::LogCleanupIntervalMs => write!(f, "{}", LOG_CLEANUP_INTERVAL_MS_PROP),
            Self::LogCleanupPolicy => write!(f, "{}", LOG_CLEANUP_POLICY_PROP),
            Self::LogCleanerThreads => write!(f, "{}", LOG_CLEANER_THREADS_PROP),
            Self::LogCleanerDedupeBufferSize => {
                write!(f, "{}", LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP)
            },
            Self::LogCleanerIoBufferSize => write!(f, "{}", LOG_CLEANER_IO_BUFFER_SIZE_PROP),
            Self::LogCleanerDedupeBufferLoadFactor => {
                write!(f, "{}", LOG_CLEANER_DEDUPE_BUFFER_LOAD_FACTOR_PROP)
            },
            Self::LogCleanerIoMaxBytesPerSecond => {
                write!(f, "{}", LOG_CLEANER_IO_MAX_BYTES_PER_SECOND_PROP)
            },
            Self::LogCleanerBackoffMs => {
                write!(f, "{}", LOG_CLEANER_DEDUPE_BUFFER_LOAD_FACTOR_PROP)
            },
            Self::LogCleanerMinCleanRatio => write!(f, "{}", LOG_CLEANER_MIN_CLEAN_RATIO_PROP),
            Self::LogCleanerEnable => write!(f, "{}", LOG_CLEANER_ENABLE_PROP),
            Self::LogCleanerDeleteRetentionMs => {
                write!(f, "{}", LOG_CLEANER_DELETE_RETENTION_MS_PROP)
            },
            Self::LogCleanerMinCompactionLagMs => {
                write!(f, "{}", LOG_CLEANER_MIN_COMPACTION_LAG_MS_PROP)
            },
            Self::LogCleanerMaxCompactionLagMs => {
                write!(f, "{}", LOG_CLEANER_MAX_COMPACTION_LAG_MS_PROP)
            },
            Self::LogIndexIntervalBytes => write!(f, "{}", LOG_INDEX_INTERVAL_BYTES_PROP),
            Self::LogFlushIntervalMessages => write!(f, "{}", LOG_FLUSH_INTERVAL_MESSAGES_PROP),
            Self::LogDeleteDelayMs => write!(f, "{}", LOG_DELETE_DELAY_MS_PROP),
            Self::LogFlushSchedulerIntervalMs => {
                write!(f, "{}", LOG_FLUSH_SCHEDULER_INTERVAL_MS_PROP)
            },
            Self::LogFlushIntervalMs => write!(f, "{}", LOG_FLUSH_INTERVAL_MS_PROP),
            Self::LogFlushOffsetCheckpointIntervalMs => {
                write!(f, "{}", LOG_FLUSH_OFFSET_CHECKPOINT_INTERVAL_MS_PROP)
            },
            Self::LogFlushStartOffsetCheckpointIntervalMs => {
                write!(f, "{}", LOG_FLUSH_START_OFFSET_CHECKPOINT_INTERVAL_MS_PROP)
            },
            Self::LogPreAllocateEnable => write!(f, "{}", LOG_PRE_ALLOCATE_PROP),
            Self::LogMessageFormatVersion => write!(f, "{}", LOG_MESSAGE_FORMAT_VERSION_PROP),
            Self::LogMessageTimestampType => write!(f, "{}", LOG_MESSAGE_TIMESTAMP_TYPE_PROP),
            Self::LogMessageTimestampDifferenceMaxMs => {
                write!(f, "{}", LOG_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_PROP)
            },
            Self::NumRecoveryThreadsPerDataDir => {
                write!(f, "{}", NUM_RECOVERY_THREADS_PER_DATA_DIR_PROP)
            },
            Self::MinInSyncReplicas => write!(f, "{}", MIN_IN_SYNC_REPLICAS_PROP),
            Self::LogMessageDownConversionEnable => {
                write!(f, "{}", LOG_MESSAGE_DOWN_CONVERSION_ENABLE_PROP)
            },
        }
    }
}

impl FromStr for DefaultLogConfigKey {
    type Err = KafkaConfigError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            LOG_DIR_PROP => Ok(Self::LogDir),
            LOG_DIRS_PROP => Ok(Self::LogDirs),
            LOG_SEGMENT_BYTES_PROP => Ok(Self::LogSegmentBytes),
            LOG_ROLL_TIME_MILLIS_PROP => Ok(Self::LogRollTimeMillis),
            LOG_ROLL_TIME_HOURS_PROP => Ok(Self::LogRollTimeHours),
            LOG_ROLL_TIME_JITTER_MILLIS_PROP => Ok(Self::LogRollTimeJitterMillis),
            LOG_ROLL_TIME_JITTER_HOURS_PROP => Ok(Self::LogRollTimeJitterHours),
            LOG_RETENTION_TIME_MILLIS_PROP => Ok(Self::LogRetentionTimeMillis),
            LOG_RETENTION_TIME_MINUTES_PROP => Ok(Self::LogRetentionTimeMinutes),
            LOG_RETENTION_TIME_HOURS_PROP => Ok(Self::LogRetentionTimeHours),
            LOG_CLEANUP_INTERVAL_MS_PROP => Ok(Self::LogCleanupIntervalMs),
            LOG_CLEANUP_POLICY => Ok(Self::LogCleanupPolicy),
            LOG_CLEANER_THREADS_PROP => Ok(Self::LogCleanerThreads),
            LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP => Ok(Self::LogCleanerDedupeBufferSize),
            LOG_CLEANER_IO_BUFFER_SIZE_PROP => Ok(Self::LogCleanerIoBufferSize),
            LOG_CLEANER_DEDUPE_BUFFER_LOAD_FACTOR_PROP => {
                Ok(Self::LogCleanerDedupeBufferLoadFactor)
            },
            LOG_CLEANER_IO_MAX_BYTES_PER_SECOND_PROP => Ok(Self::LogCleanerIoMaxBytesPerSecond),
            LOG_CLEANER_BACKOFF_MS_PROP => Ok(Self::LogCleanerBackoffMs),
            LOG_CLEANER_MIN_CLEAN_RATIO => Ok(Self::LogCleanerMinCleanRatio),
            LOG_CLEANER_ENABLE_PROP => Ok(Self::LogCleanerEnable),
            LOG_CLEANER_DELETE_RETENTION_MS_PROP => Ok(Self::LogCleanerDeleteRetentionMs),
            LOG_CLEANER_MIN_COMPACTION_LAG_MS_PROP => Ok(Self::LogCleanerMinCompactionLagMs),
            LOG_CLEANER_MAX_COMPACTION_LAG_MS_PROP => Ok(Self::LogCleanerMaxCompactionLagMs),
            LOG_INDEX_INTERVAL_BYTES_PROP => Ok(Self::LogIndexIntervalBytes),
            LOG_FLUSH_INTERVAL_MESSAGES_PROP => Ok(Self::LogFlushIntervalMessages),
            LOG_DELETE_DELAY_MS_PROP => Ok(Self::LogDeleteDelayMs),
            LOG_FLUSH_SCHEDULER_INTERVAL_MS_PROP => Ok(Self::LogFlushSchedulerIntervalMs),
            LOG_FLUSH_INTERVAL_MS_PROP => Ok(Self::LogFlushIntervalMs),
            LOG_FLUSH_OFFSET_CHECKPOINT_INTERVAL_MS_PROP => {
                Ok(Self::LogFlushOffsetCheckpointIntervalMs)
            },
            LOG_FLUSH_START_OFFSET_CHECKPOINT_INTERVAL_MS_PROP => {
                Ok(Self::LogFlushStartOffsetCheckpointIntervalMs)
            },
            LOG_PRE_ALLOCATE_PROP => Ok(Self::LogPreAllocateEnable),
            LOG_MESSAGE_FORMAT_VERSION_PROP => Ok(Self::LogMessageFormatVersion),
            LOG_MESSAGE_TIMESTAMP_TYPE_PROP => Ok(Self::LogMessageTimestampType),
            LOG_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_PROP => {
                Ok(Self::LogMessageTimestampDifferenceMaxMs)
            },
            NUM_RECOVERY_THREADS_PER_DATA_DIR_PROP => Ok(Self::NumRecoveryThreadsPerDataDir),
            MIN_IN_SYNC_REPLICAS_PROP => Ok(Self::MinInSyncReplicas),
            LOG_MESSAGE_DOWN_CONVERSION_ENABLE_PROP => Ok(Self::LogMessageDownConversionEnable),
            _ => Err(KafkaConfigError::UnknownKey(input.to_string())),
        }
    }
}

#[derive(Debug)]
pub struct DefaultLogConfigProperties {
    // Singular log.dir
    log_dir: ConfigDef<String>,
    // Multiple comma separated log.dirs, may include spaces after the comma (will be trimmed)
    log_dirs: ConfigDef<String>,
    log_segment_bytes: ConfigDef<usize>,
    log_roll_time_millis: ConfigDef<i64>,
    log_roll_time_hours: ConfigDef<i32>,
    log_roll_time_jitter_millis: ConfigDef<i64>,
    log_roll_time_jitter_hours: ConfigDef<i32>,
    log_retention_time_millis: ConfigDef<i64>,
    log_retention_time_minutes: ConfigDef<i32>,
    log_retention_time_hours: ConfigDef<i32>,
    log_cleanup_interval_ms: ConfigDef<i64>,
    log_cleanup_policy: ConfigDef<String>,
    log_cleaner_threads: ConfigDef<i32>,
    log_cleaner_dedupe_buffer_size: ConfigDef<i64>,
    log_cleaner_io_buffer_size: ConfigDef<i32>,
    log_cleaner_dedupe_buffer_load_factor: ConfigDef<f64>,
    log_cleaner_io_max_bytes_per_second: ConfigDef<f64>,
    log_cleaner_backoff_ms: ConfigDef<i64>,
    pub log_cleaner_min_clean_ratio: ConfigDef<f64>,
    log_cleaner_enable: ConfigDef<bool>,
    pub log_cleaner_delete_retention_ms: ConfigDef<i64>,
    pub log_cleaner_min_compaction_lag_ms: ConfigDef<i64>,
    pub log_cleaner_max_compaction_lag_ms: ConfigDef<i64>,
    pub log_index_interval_bytes: ConfigDef<i32>,
    pub log_flush_interval_messages: ConfigDef<i64>,
    pub log_delete_delay_ms: ConfigDef<i64>,
    pub log_flush_scheduler_interval_ms: ConfigDef<i64>,
    log_flush_interval_ms: ConfigDef<i64>,
    log_flush_offset_checkpoint_interval_ms: ConfigDef<i32>,
    log_flush_start_offset_checkpoint_interval_ms: ConfigDef<i32>,
    pub log_pre_allocate_enable: ConfigDef<bool>,
    pub log_message_format_version: ConfigDef<String>,
    pub log_message_timestamp_type: ConfigDef<String>,
    pub log_message_timestamp_difference_max_ms: ConfigDef<i64>,
    num_recovery_threads_per_data_dir: ConfigDef<i32>,
    pub min_in_sync_replicas: ConfigDef<i32>,
    pub log_message_down_conversion_enable: ConfigDef<bool>,
}

impl Default for DefaultLogConfigProperties {
    fn default() -> Self {
        let inter_broker_protocol_version = ApiVersion::latest_version();
        Self {
            log_dir: ConfigDef::default()
                .with_key(LOG_DIR_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(format!(
                    "The directory in which the log data is kept (supplemental for {} property)",
                    LOG_DIRS_PROP
                ))
                .with_default(String::from("/tmp/kafka-logs")),
            log_dirs: ConfigDef::default()
                .with_key(LOG_DIRS_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(format!(
                    "The directories in which the log data is kept. If not set, the value in {} \
                     is used",
                    LOG_DIR_PROP
                )),
            log_segment_bytes: ConfigDef::default()
                .with_key(LOG_SEGMENT_BYTES_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(String::from("The maximum size of a single log file"))
                .with_default(1 * 1024 * 1024 * 1024)
                .with_validator(Box::new(|data| {
                    // RAFKA TODO: This doesn't make much sense if it's u32...
                    ConfigDef::at_least(
                        data,
                        &legacy_record::RECORD_OVERHEAD_V0,
                        LOG_SEGMENT_BYTES_PROP,
                    )
                })),
            log_roll_time_millis: ConfigDef::default()
                .with_key(LOG_ROLL_TIME_MILLIS_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_validator(Box::new(|data| {
                    // Safe to unwrap, we have a default
                    ConfigDef::at_least(data, &0, PRODUCER_QUOTA_BYTES_PER_SECOND_DEFAULT_PROP)
                }))
                .with_doc(format!(
                    "The maximum time before a new log segment is rolled out (in milliseconds). \
                     If not set, the value in {} is used",
                    LOG_ROLL_TIME_HOURS_PROP
                )),
            log_roll_time_hours: ConfigDef::default()
                .with_key(LOG_ROLL_TIME_HOURS_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(format!(
                    "The maximum time before a new log segment is rolled out (in hours), \
                     secondary to {} property",
                    LOG_ROLL_TIME_MILLIS_PROP
                ))
                .with_default(24 * 7)
                .with_validator(Box::new(|data| {
                    // Safe to unwrap, we have a default
                    ConfigDef::at_least(data, &1, LOG_ROLL_TIME_HOURS_PROP)
                })),
            log_roll_time_jitter_millis: ConfigDef::default()
                .with_key(LOG_ROLL_TIME_JITTER_MILLIS_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(format!(
                    "The maximum jitter to subtract from logRollTimeMillis (in milliseconds). If \
                     not set, the value in {} is used",
                    LOG_ROLL_TIME_JITTER_HOURS_PROP
                )),
            log_roll_time_jitter_hours: ConfigDef::default()
                .with_key(LOG_ROLL_TIME_JITTER_HOURS_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(format!(
                    "The maximum jitter to subtract from logRollTimeMillis (in hours), secondary \
                     to {} property",
                    LOG_ROLL_TIME_JITTER_MILLIS_PROP
                ))
                .with_default(0)
                .with_validator(Box::new(|data| {
                    // Safe to unwrap, we have a default
                    ConfigDef::at_least(data, &0, LOG_ROLL_TIME_JITTER_HOURS_PROP)
                })),
            log_retention_time_millis: ConfigDef::default()
                .with_key(LOG_RETENTION_TIME_MILLIS_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(format!(
                    "The number of milliseconds to keep a log file before deleting it (in \
                     milliseconds), If not set, the value in {} is used. If set to -1, no time \
                     limit is applied.",
                    LOG_RETENTION_TIME_MINUTES_PROP
                )),
            log_retention_time_minutes: ConfigDef::default()
                .with_key(LOG_RETENTION_TIME_MINUTES_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(format!(
                    "The number of minutes to keep a log file before deleting it (in minutes), \
                     secondary to {} property. If not set, the value in {} is used",
                    LOG_RETENTION_TIME_MILLIS_PROP, LOG_RETENTION_TIME_HOURS_PROP
                )),
            log_retention_time_hours: ConfigDef::default()
                .with_key(LOG_RETENTION_TIME_HOURS_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(format!(
                    "The number of hours to keep a log file before deleting it (in hours), \
                     tertiary to {} property",
                    LOG_RETENTION_TIME_MILLIS_PROP
                ))
                .with_default(24 * 7)
                .with_validator(Box::new(|data| {
                    // Safe to unwrap, we have a default
                    ConfigDef::at_least(data, &1, LOG_CLEANER_THREADS_PROP)
                })),
            log_cleanup_interval_ms: ConfigDef::default()
                .with_key(LOG_CLEANUP_INTERVAL_MS_PROP)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(String::from(
                    "The frequency in milliseconds that the log cleaner checks whether any log is \
                     eligible for deletion",
                ))
                .with_default(5 * 60 * 1000),
            log_cleanup_policy: ConfigDef::default()
                .with_key(LOG_CLEANUP_POLICY_PROP)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(String::from(
                    "The default cleanup policy for segments beyond the retention window. A comma \
                     separated list of valid policies. Valid policies are: \"delete\" and \
                     \"compact\"",
                ))
                .with_default(String::from("delete"))
                .with_validator(Box::new(|data| {
                    // Safe to unwrap, we have a default
                    ConfigDef::value_in_list(
                        data,
                        vec![
                            &LogCleanupPolicy::Delete.to_string(),
                            &LogCleanupPolicy::Compact.to_string(),
                        ],
                        LOG_CLEANUP_POLICY_PROP,
                    )
                })),
            log_cleaner_threads: ConfigDef::default()
                .with_key(LOG_CLEANER_THREADS_PROP)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(String::from("The number of background threads to use for log cleaning"))
                .with_default(1)
                .with_validator(Box::new(|data| {
                    // Safe to unwrap, we have a default
                    ConfigDef::at_least(data, &0, LOG_CLEANER_THREADS_PROP)
                })),
            log_cleaner_dedupe_buffer_size: ConfigDef::default()
                .with_key(LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(String::from(
                    "The total memory used for log deduplication across all cleaner threads",
                ))
                .with_default(128 * 1024 * 1024),
            log_cleaner_io_buffer_size: ConfigDef::default()
                .with_key(LOG_CLEANER_IO_BUFFER_SIZE_PROP)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(String::from(
                    "The total memory used for log cleaner I/O buffers across all cleaner threads",
                ))
                .with_default(512 * 1024),
            log_cleaner_dedupe_buffer_load_factor: ConfigDef::default()
                .with_key(LOG_CLEANER_DEDUPE_BUFFER_LOAD_FACTOR_PROP)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(String::from(
                    "Log cleaner dedupe buffer load factor. The percentage full the dedupe buffer \
                     can become. A higher value will allow more log to be cleaned at once but \
                     will lead to more hash collisions",
                ))
                .with_default(0.9), // Contained a 0.9d before, double check
            log_cleaner_io_max_bytes_per_second: ConfigDef::default()
                .with_key(LOG_CLEANER_IO_MAX_BYTES_PER_SECOND_PROP)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(String::from(
                    "The log cleaner will be throttled so that the sum of its read and write i/o \
                     will be less than this value on average",
                ))
                .with_default(f64::MAX),
            log_cleaner_backoff_ms: ConfigDef::default()
                .with_key(LOG_CLEANER_BACKOFF_MS_PROP)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(String::from(
                    "The amount of time to sleep when there are no logs to clean",
                ))
                .with_default(15 * 1000)
                .with_validator(Box::new(|data| {
                    // Safe to unwrap, we have a default
                    ConfigDef::at_least(data, &0, LOG_CLEANER_BACKOFF_MS_PROP)
                })),
            log_cleaner_min_clean_ratio: ConfigDef::default()
                .with_key(LOG_CLEANER_MIN_CLEAN_RATIO_PROP)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(format!(
                    "The minimum ratio of dirty log to total log for a log to eligible for \
                     cleaning. If the {}  or the {} configurations are also specified, then the \
                     log compactor considers the log eligible for compaction as soon as either: \
                     (i) the dirty ratio threshold has been met and the log has had dirty \
                     (uncompacted) records for at least the {} duration, or (ii) if the log has \
                     had dirty (uncompacted) records for at most the {} period.",
                    LOG_CLEANER_MAX_COMPACTION_LAG_MS_PROP,
                    LOG_CLEANER_MIN_COMPACTION_LAG_MS_PROP,
                    LOG_CLEANER_MIN_COMPACTION_LAG_MS_PROP,
                    LOG_CLEANER_MAX_COMPACTION_LAG_MS_PROP
                ))
                .with_default(0.5),
            log_cleaner_enable: ConfigDef::default()
                .with_key(LOG_CLEANER_ENABLE_PROP)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(String::from(
                    "Enable the log cleaner process to run on the server. Should be enabled if \
                     using any topics with a cleanup.policy=compact including the internal \
                     offsets topic. If disabled those topics will not be compacted and \
                     continually grow in size.",
                ))
                .with_default(true),
            log_cleaner_delete_retention_ms: ConfigDef::default()
                .with_key(LOG_CLEANER_DELETE_RETENTION_MS_PROP)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(String::from("How long are delete records retained?"))
                .with_default(24 * 60 * 60 * 1000),
            log_cleaner_min_compaction_lag_ms: ConfigDef::default()
                .with_key(LOG_CLEANER_MIN_COMPACTION_LAG_MS_PROP)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(String::from(
                    "The minimum time a message will remain uncompacted in the log. Only \
                     applicable for logs that are being compacted.",
                ))
                .with_default(0),
            log_cleaner_max_compaction_lag_ms: ConfigDef::default()
                .with_key(LOG_CLEANER_MAX_COMPACTION_LAG_MS_PROP)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(String::from(
                    "The maximum time a message will remain ineligible for compaction in the log. \
                     Only applicable for logs that are being compacted.",
                ))
                .with_default(i64::MAX),
            log_index_interval_bytes: ConfigDef::default()
                .with_key(LOG_INDEX_INTERVAL_BYTES_PROP)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(String::from(
                    "The interval with which we add an entry to the offset index",
                ))
                .with_default(4096),
            log_flush_interval_messages: ConfigDef::default()
                .with_key(LOG_FLUSH_INTERVAL_MESSAGES_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(String::from(
                    "The number of messages accumulated on a log partition before messages are \
                     flushed to disk ",
                ))
                .with_default(i64::MAX)
                .with_validator(Box::new(|data| {
                    // Safe to unwrap, we have a default
                    ConfigDef::at_least(data, &1, LOG_FLUSH_INTERVAL_MESSAGES_PROP)
                })),
            log_delete_delay_ms: ConfigDef::default()
                .with_key(LOG_DELETE_DELAY_MS_PROP)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(String::from(
                    "The amount of time to wait before deleting a file from the filesystem",
                ))
                .with_default(60000)
                .with_validator(Box::new(|data| {
                    // Safe to unwrap, we have a default
                    ConfigDef::at_least(data, &0, LOG_DELETE_DELAY_MS_PROP)
                })),
            log_flush_scheduler_interval_ms: ConfigDef::default()
                .with_key(LOG_FLUSH_SCHEDULER_INTERVAL_MS_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(String::from(
                    "The frequency in ms that the log flusher checks whether any log needs to be \
                     flushed to disk",
                ))
                .with_default(i64::MAX),
            log_flush_interval_ms: ConfigDef::default()
                .with_key(LOG_FLUSH_INTERVAL_MS_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(format!(
                    "The maximum time in ms that a message in any topic is kept in memory before \
                     flushed to disk. If not set, the value in {} is used",
                    LOG_FLUSH_SCHEDULER_INTERVAL_MS_PROP
                ))
                .with_default(i64::MAX),
            log_flush_offset_checkpoint_interval_ms: ConfigDef::default()
                .with_key(LOG_FLUSH_OFFSET_CHECKPOINT_INTERVAL_MS_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(String::from(
                    "The frequency with which we update the persistent record of the last flush \
                     which acts as the log recovery point",
                ))
                .with_default(60000)
                .with_validator(Box::new(|data| {
                    // Safe to unwrap, we have a default
                    ConfigDef::at_least(data, &0, LOG_CLEANER_THREADS_PROP)
                })),
            log_flush_start_offset_checkpoint_interval_ms: ConfigDef::default()
                .with_key(LOG_FLUSH_START_OFFSET_CHECKPOINT_INTERVAL_MS_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(String::from(
                    "The frequency with which we update the persistent record of log start offset",
                ))
                .with_default(60000)
                .with_validator(Box::new(|data| {
                    // Safe to unwrap, we have a default
                    ConfigDef::at_least(data, &0, LOG_CLEANER_THREADS_PROP)
                })),
            log_pre_allocate_enable: ConfigDef::default()
                .with_key(LOG_PRE_ALLOCATE_PROP)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(String::from(
                    "Should pre allocate file when create new segment? If you are using Kafka on \
                     Windows, you probably need to set it to true.",
                ))
                .with_default(false),
            log_message_format_version: ConfigDef::default()
                .with_key(LOG_MESSAGE_FORMAT_VERSION_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(String::from(
                    "Specify the message format version the broker will use to append messages to \
                     the logs. The value should be a valid ApiVersion. Some examples are: 0.8.2, \
                     0.9.0.0, 0.10.0, check ApiVersion for more details. By setting a particular \
                     message format version, the user is certifying that all the existing \
                     messages on disk are smaller or equal than the specified version. Setting \
                     this value incorrectly will cause consumers with older versions to break as \
                     they will receive messages with a format that they don't understand.",
                ))
                .with_default(inter_broker_protocol_version.to_string()),
            log_message_timestamp_type: ConfigDef::default()
                .with_key(LOG_MESSAGE_TIMESTAMP_TYPE_PROP)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(String::from(
                    "Define whether the timestamp in the message is message create time or log \
                     append time. The value should be either `CreateTime` or `LogAppendTime`",
                ))
                .with_default(LogMessageTimestampType::default().to_string()),
            log_message_timestamp_difference_max_ms: ConfigDef::default()
                .with_key(LOG_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_PROP)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(String::from(
                    "The maximum difference allowed between the timestamp when a broker receives \
                     a message and the timestamp specified in the message. If \
                     log.message.timestamp.type=CreateTime, a message will be rejected if the \
                     difference in timestamp exceeds this threshold. This configuration is \
                     ignored if log.message.timestamp.type=LogAppendTime.The maximum timestamp \
                     difference allowed should be no greater than log.retention.ms to avoid \
                     unnecessarily frequent log rolling.",
                ))
                .with_default(i64::MAX),
            num_recovery_threads_per_data_dir: ConfigDef::default()
                .with_key(NUM_RECOVERY_THREADS_PER_DATA_DIR_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(String::from(
                    "The number of threads per data directory to be used for log recovery at \
                     startup and flushing at shutdown",
                ))
                .with_default(1),
            min_in_sync_replicas: ConfigDef::default()
                .with_key(MIN_IN_SYNC_REPLICAS_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(String::from(
                    "When a producer sets acks to \"all\" (or \"-1\"),  min.insync.replicas \
                     specifies the minimum number of replicas that must acknowledge  a write for \
                     the write to be considered successful. If this minimum cannot be met,  then \
                     the producer will raise an exception (either NotEnoughReplicas or  \
                     NotEnoughReplicasAfterAppend). When used together, min.insync.replicas and \
                     acks  allow you to enforce greater durability guarantees. A typical scenario \
                     would be to  create a topic with a replication factor of 3, set \
                     min.insync.replicas to 2, and  produce with acks of \"all\". This will \
                     ensure that the producer raises an exception  if a majority of replicas do \
                     not receive a write.",
                ))
                .with_default(1)
                .with_validator(Box::new(|data| {
                    // Safe to unwrap, we have a default
                    ConfigDef::at_least(data, &1, MIN_IN_SYNC_REPLICAS_PROP)
                })),
            log_message_down_conversion_enable: ConfigDef::default()
                .with_key(LOG_MESSAGE_DOWN_CONVERSION_ENABLE_PROP)
                .with_importance(ConfigDefImportance::Low)
                .with_doc(MESSAGE_DOWNCONVERSION_ENABLE_DOC.to_string())
                .with_default(true),
        }
    }
}

impl ConfigSet for DefaultLogConfigProperties {
    type ConfigKey = DefaultLogConfigKey;
    type ConfigType = DefaultLogConfig;

    /// `try_from_config_property` transforms a string value from the config into our actual types
    fn try_set_property(
        &mut self,
        property_name: &str,
        property_value: &str,
    ) -> Result<(), KafkaConfigError> {
        let kafka_config_key = Self::ConfigKey::from_str(property_name)?;
        match kafka_config_key {
            Self::ConfigKey::LogDir => self.log_dir.try_set_parsed_value(property_value)?,
            Self::ConfigKey::LogDirs => self.log_dirs.try_set_parsed_value(property_value)?,
            Self::ConfigKey::LogSegmentBytes => {
                self.log_segment_bytes.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::LogRollTimeMillis => {
                self.log_roll_time_millis.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::LogRollTimeHours => {
                self.log_roll_time_hours.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::LogRollTimeJitterMillis => {
                self.log_roll_time_jitter_millis.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::LogRollTimeJitterHours => {
                self.log_roll_time_jitter_hours.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::LogRetentionTimeMillis => {
                self.log_retention_time_millis.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::LogRetentionTimeMinutes => {
                self.log_retention_time_minutes.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::LogRetentionTimeHours => {
                self.log_retention_time_hours.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::LogCleanupIntervalMs => {
                self.log_cleanup_interval_ms.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::LogCleanupPolicy => {
                self.log_cleanup_policy.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::LogCleanerThreads => {
                self.log_cleaner_threads.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::LogCleanerDedupeBufferSize => {
                self.log_cleaner_dedupe_buffer_size.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::LogCleanerIoBufferSize => {
                self.log_cleaner_io_buffer_size.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::LogCleanerDedupeBufferLoadFactor => {
                self.log_cleaner_dedupe_buffer_load_factor.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::LogCleanerIoMaxBytesPerSecond => {
                self.log_cleaner_io_max_bytes_per_second.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::LogCleanerBackoffMs => {
                self.log_cleaner_backoff_ms.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::LogCleanerMinCleanRatio => {
                self.log_cleaner_min_clean_ratio.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::LogCleanerEnable => {
                self.log_cleaner_enable.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::LogCleanerDeleteRetentionMs => {
                self.log_cleaner_delete_retention_ms.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::LogCleanerMinCompactionLagMs => {
                self.log_cleaner_min_compaction_lag_ms.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::LogCleanerMaxCompactionLagMs => {
                self.log_cleaner_max_compaction_lag_ms.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::LogIndexIntervalBytes => {
                self.log_index_interval_bytes.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::LogFlushIntervalMessages => {
                self.log_flush_interval_messages.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::LogDeleteDelayMs => {
                self.log_delete_delay_ms.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::LogFlushSchedulerIntervalMs => {
                self.log_flush_scheduler_interval_ms.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::LogFlushIntervalMs => {
                self.log_flush_interval_ms.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::LogFlushOffsetCheckpointIntervalMs => {
                self.log_flush_offset_checkpoint_interval_ms.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::LogFlushStartOffsetCheckpointIntervalMs => self
                .log_flush_start_offset_checkpoint_interval_ms
                .try_set_parsed_value(property_value)?,
            Self::ConfigKey::LogPreAllocateEnable => {
                self.log_pre_allocate_enable.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::LogMessageFormatVersion => {
                self.log_message_format_version.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::LogMessageTimestampType => {
                self.log_message_timestamp_type.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::LogMessageTimestampDifferenceMaxMs => {
                self.log_message_timestamp_difference_max_ms.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::NumRecoveryThreadsPerDataDir => {
                self.num_recovery_threads_per_data_dir.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::MinInSyncReplicas => {
                self.min_in_sync_replicas.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::LogMessageDownConversionEnable => {
                self.log_message_down_conversion_enable.try_set_parsed_value(property_value)?
            },
        };
        Ok(())
    }

    fn build(&mut self) -> Result<Self::ConfigType, KafkaConfigError> {
        let log_segment_bytes = self.log_segment_bytes.build()?;
        let log_roll_time_millis = self.resolve_log_roll_time_millis()?;
        let log_roll_time_jitter_millis = self.resolve_log_roll_time_jitter_millis()?;
        let log_retention_time_millis = self.resolve_log_retention_time_millis()?;
        let log_cleanup_interval_ms = self.log_cleanup_interval_ms.build()?;
        let log_cleanup_policy = self.resolve_log_cleanup_policy()?;
        let log_cleaner_threads = self.log_cleaner_threads.build()?;
        let log_cleaner_dedupe_buffer_size = self.log_cleaner_dedupe_buffer_size.build()?;
        let log_cleaner_io_buffer_size = self.log_cleaner_io_buffer_size.build()?;
        let log_cleaner_dedupe_buffer_load_factor =
            self.log_cleaner_dedupe_buffer_load_factor.build()?;
        let log_cleaner_io_max_bytes_per_second =
            self.log_cleaner_io_max_bytes_per_second.build()?;
        let log_cleaner_backoff_ms = self.log_cleaner_backoff_ms.build()?;
        let log_cleaner_min_clean_ratio = self.log_cleaner_min_clean_ratio.build()?;
        let log_cleaner_enable = self.log_cleaner_enable.build()?;
        let log_cleaner_delete_retention_ms = self.log_cleaner_delete_retention_ms.build()?;
        let log_cleaner_min_compaction_lag_ms = self.log_cleaner_min_compaction_lag_ms.build()?;
        let log_cleaner_max_compaction_lag_ms = self.log_cleaner_max_compaction_lag_ms.build()?;
        let log_index_interval_bytes = self.log_index_interval_bytes.build()?;
        let log_flush_interval_messages = self.log_flush_interval_messages.build()?;
        let log_delete_delay_ms = self.log_delete_delay_ms.build()?;
        let log_flush_scheduler_interval_ms = self.log_flush_scheduler_interval_ms.build()?;
        let log_flush_interval_ms = self.log_flush_interval_ms.build()?;
        let log_flush_offset_checkpoint_interval_ms =
            self.log_flush_offset_checkpoint_interval_ms.build()?;
        let log_flush_start_offset_checkpoint_interval_ms =
            self.log_flush_start_offset_checkpoint_interval_ms.build()?;
        let log_pre_allocate_enable = self.log_pre_allocate_enable.build()?;
        let log_message_format_version = self.resolve_log_message_format_version()?;
        let log_message_timestamp_type = self.resolve_log_message_timestamp_type()?;
        let log_message_timestamp_difference_max_ms =
            self.log_message_timestamp_difference_max_ms.build()?;
        let num_recovery_threads_per_data_dir = self.num_recovery_threads_per_data_dir.build()?;
        let min_in_sync_replicas = self.min_in_sync_replicas.build()?;
        let log_message_down_conversion_enable = self.log_message_down_conversion_enable.build()?;
        let log_dirs = self.resolve_log_dirs()?;
        Ok(Self::ConfigType {
            log_dirs,
            log_segment_bytes,
            log_roll_time_millis,
            log_roll_time_jitter_millis,
            log_retention_time_millis,
            log_cleanup_interval_ms,
            log_cleanup_policy,
            log_cleaner_threads,
            log_cleaner_dedupe_buffer_size,
            log_cleaner_io_buffer_size,
            log_cleaner_dedupe_buffer_load_factor,
            log_cleaner_io_max_bytes_per_second,
            log_cleaner_backoff_ms,
            log_cleaner_min_clean_ratio,
            log_cleaner_enable,
            log_cleaner_delete_retention_ms,
            log_cleaner_min_compaction_lag_ms,
            log_cleaner_max_compaction_lag_ms,
            log_index_interval_bytes,
            log_flush_interval_messages,
            log_delete_delay_ms,
            log_flush_scheduler_interval_ms,
            log_flush_interval_ms,
            log_flush_offset_checkpoint_interval_ms,
            log_flush_start_offset_checkpoint_interval_ms,
            log_pre_allocate_enable,
            log_message_format_version,
            log_message_timestamp_type,
            log_message_timestamp_difference_max_ms,
            num_recovery_threads_per_data_dir,
            min_in_sync_replicas,
            log_message_down_conversion_enable,
        })
    }
}

impl DefaultLogConfigProperties {
    /// `resolve_log_dirs` validates the log.dirs and log.dir combination. Note that the end value
    /// in KafkaConfig has a default, so even if they are un-set, they will be marked as provided
    fn resolve_log_dirs(&mut self) -> Result<Vec<String>, KafkaConfigError> {
        // TODO: Consider checking for valid Paths and return KafkaConfigError for them
        // NOTE: When the directories do not exist, KafkaServer simply gets a list of offline_dirs
        if let Some(log_dirs) = &self.log_dirs.get_value() {
            Ok((*log_dirs).clone().split(',').map(|x| x.trim_start().to_string()).collect())
        } else if let Some(log_dir) = &self.log_dir.get_value() {
            Ok(vec![log_dir.to_string()])
        } else {
            Ok(vec![])
        }
    }

    /// The `resolve()` from `ConfigDef` cannot be used because the units (hours to millis) cannot
    /// be currently performed by the resolver.
    pub fn resolve_log_roll_time_millis(&mut self) -> Result<i64, KafkaConfigError> {
        if let Some(log_roll_time_millis) = self.log_roll_time_millis.get_value() {
            Ok(*log_roll_time_millis)
        } else {
            Ok(i64::from(self.log_roll_time_hours.build()?) * 60 * 60 * 1000)
        }
    }

    /// The `resolve()` from `ConfigDef` cannot be used because the units (hours to millis) cannot
    /// be currently performed by the resolver.
    pub fn resolve_log_roll_time_jitter_millis(&mut self) -> Result<i64, KafkaConfigError> {
        if let Some(log_roll_time_jitter_millis) = self.log_roll_time_jitter_millis.get_value() {
            Ok(*log_roll_time_jitter_millis)
        } else {
            Ok(i64::from(self.log_roll_time_jitter_hours.build()?) * 60 * 60 * 1000)
        }
    }

    /// The `resolve()` from `ConfigDef` cannot be used as we need to transform hours to minutes to
    /// millis
    pub fn resolve_log_retention_time_millis(&mut self) -> Result<i64, KafkaConfigError> {
        let millis_in_minute = 60 * 1000;
        let millis_in_hour = 60 * millis_in_minute;

        let mut millis: i64 = match self.log_retention_time_millis.get_value() {
            Some(millis) => *millis,
            None => match self.log_retention_time_minutes.get_value() {
                Some(mins) => i64::from(millis_in_minute) * i64::from(*mins),
                None => {
                    i64::from(*self.log_retention_time_hours.get_value().unwrap()) * millis_in_hour
                },
            },
        };
        if millis < 0 {
            warn!(
                "Resolved Log Retention Time millis is below zero: '{}' Setting to -1 (unlimited)",
                millis
            );
            millis = -1;
        } else if millis == 0 {
            return Err(KafkaConfigError::InvalidValue(String::from(
                "log.retention.ms must be unlimited (-1) or, equal or greater than 1",
            )));
        }
        Ok(millis)
    }

    pub fn resolve_log_message_timestamp_type(
        &mut self,
    ) -> Result<LogMessageTimestampType, KafkaConfigError> {
        // LogMessageTimestampType has a default, safe to unwrap
        LogMessageTimestampType::from_str(self.log_message_timestamp_type.get_value().unwrap())
    }

    pub fn resolve_log_cleanup_policy(
        &mut self,
    ) -> Result<Vec<LogCleanupPolicy>, KafkaConfigError> {
        match self.log_cleanup_policy.get_value() {
            Some(val) => {
                let mut res: Vec<LogCleanupPolicy> = vec![];
                let policies: Vec<&str> = val.split(",").collect();
                for policy in policies {
                    res.push(LogCleanupPolicy::from_str(policy)?);
                }
                Ok(res)
            },
            None => Ok(vec![]),
        }
    }

    pub fn resolve_log_message_format_version(
        &mut self,
    ) -> Result<KafkaApiVersion, KafkaConfigError> {
        // self.log_message_format_version has a default, so it's safe to unwrap().
        KafkaApiVersion::from_str(&self.log_message_format_version.get_value().unwrap().clone())
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct DefaultLogConfig {
    pub log_dirs: Vec<String>,
    pub log_segment_bytes: usize,
    /// The coalesced roll time, resolving hours to its millis
    pub log_roll_time_millis: i64,
    /// The coalesced roll time jitter, resolving hours to its millis
    pub log_roll_time_jitter_millis: i64,
    /// The coalesced time retention, resolving hours or minutes to its millis
    pub log_retention_time_millis: i64,
    pub log_cleanup_interval_ms: i64,
    pub log_cleanup_policy: Vec<LogCleanupPolicy>,
    pub log_cleaner_threads: i32,
    pub log_cleaner_dedupe_buffer_size: i64,
    pub log_cleaner_io_buffer_size: i32,
    pub log_cleaner_dedupe_buffer_load_factor: f64,
    pub log_cleaner_io_max_bytes_per_second: f64,
    pub log_cleaner_backoff_ms: i64,
    pub log_cleaner_min_clean_ratio: f64,
    pub log_cleaner_enable: bool,
    pub log_cleaner_delete_retention_ms: i64,
    pub log_cleaner_min_compaction_lag_ms: i64,
    pub log_cleaner_max_compaction_lag_ms: i64,
    pub log_index_interval_bytes: i32,
    pub log_flush_interval_messages: i64,
    pub log_delete_delay_ms: i64,
    pub log_flush_scheduler_interval_ms: i64,
    pub log_flush_interval_ms: i64,
    pub log_flush_offset_checkpoint_interval_ms: i32,
    pub log_flush_start_offset_checkpoint_interval_ms: i32,
    pub log_pre_allocate_enable: bool,
    pub log_message_format_version: KafkaApiVersion,
    pub log_message_timestamp_type: LogMessageTimestampType,
    pub log_message_timestamp_difference_max_ms: i64,
    pub num_recovery_threads_per_data_dir: i32,
    pub min_in_sync_replicas: i32,
    pub log_message_down_conversion_enable: bool,
}

impl Default for DefaultLogConfig {
    fn default() -> Self {
        let mut config_properties = DefaultLogConfigProperties::default();
        let log_dirs = config_properties.resolve_log_dirs().unwrap();
        let log_segment_bytes = config_properties.log_segment_bytes.build().unwrap();
        let log_roll_time_millis = config_properties.resolve_log_roll_time_millis().unwrap();
        let log_roll_time_jitter_millis =
            config_properties.resolve_log_roll_time_jitter_millis().unwrap();
        let log_retention_time_millis =
            config_properties.resolve_log_retention_time_millis().unwrap();
        let log_cleanup_interval_ms = config_properties.log_cleanup_interval_ms.build().unwrap();
        let log_cleanup_policy = config_properties.resolve_log_cleanup_policy().unwrap();
        let log_cleaner_threads = config_properties.log_cleaner_threads.build().unwrap();
        let log_cleaner_dedupe_buffer_size =
            config_properties.log_cleaner_dedupe_buffer_size.build().unwrap();
        let log_cleaner_io_buffer_size =
            config_properties.log_cleaner_io_buffer_size.build().unwrap();
        let log_cleaner_dedupe_buffer_load_factor =
            config_properties.log_cleaner_dedupe_buffer_load_factor.build().unwrap();
        let log_cleaner_io_max_bytes_per_second =
            config_properties.log_cleaner_io_max_bytes_per_second.build().unwrap();
        let log_cleaner_backoff_ms = config_properties.log_cleaner_backoff_ms.build().unwrap();
        let log_cleaner_min_clean_ratio =
            config_properties.log_cleaner_min_clean_ratio.build().unwrap();
        let log_cleaner_enable = config_properties.log_cleaner_enable.build().unwrap();
        let log_cleaner_delete_retention_ms =
            config_properties.log_cleaner_delete_retention_ms.build().unwrap();
        let log_cleaner_min_compaction_lag_ms =
            config_properties.log_cleaner_min_compaction_lag_ms.build().unwrap();
        let log_cleaner_max_compaction_lag_ms =
            config_properties.log_cleaner_max_compaction_lag_ms.build().unwrap();
        let log_index_interval_bytes = config_properties.log_index_interval_bytes.build().unwrap();
        let log_flush_interval_messages =
            config_properties.log_flush_interval_messages.build().unwrap();
        let log_delete_delay_ms = config_properties.log_delete_delay_ms.build().unwrap();
        let log_flush_scheduler_interval_ms =
            config_properties.log_flush_scheduler_interval_ms.build().unwrap();
        let log_flush_interval_ms = config_properties.log_flush_interval_ms.build().unwrap();
        let log_flush_offset_checkpoint_interval_ms =
            config_properties.log_flush_offset_checkpoint_interval_ms.build().unwrap();
        let log_flush_start_offset_checkpoint_interval_ms =
            config_properties.log_flush_start_offset_checkpoint_interval_ms.build().unwrap();
        let log_pre_allocate_enable = config_properties.log_pre_allocate_enable.build().unwrap();
        let log_message_format_version =
            config_properties.resolve_log_message_format_version().unwrap();
        let log_message_timestamp_type =
            config_properties.resolve_log_message_timestamp_type().unwrap();
        let log_message_timestamp_difference_max_ms =
            config_properties.log_message_timestamp_difference_max_ms.build().unwrap();
        let num_recovery_threads_per_data_dir =
            config_properties.num_recovery_threads_per_data_dir.build().unwrap();
        let min_in_sync_replicas = config_properties.min_in_sync_replicas.build().unwrap();
        let log_message_down_conversion_enable =
            config_properties.log_message_down_conversion_enable.build().unwrap();
        Self {
            log_dirs,
            log_segment_bytes,
            log_roll_time_millis,
            log_roll_time_jitter_millis,
            log_retention_time_millis,
            log_cleanup_interval_ms,
            log_cleanup_policy,
            log_cleaner_threads,
            log_cleaner_dedupe_buffer_size,
            log_cleaner_io_buffer_size,
            log_cleaner_dedupe_buffer_load_factor,
            log_cleaner_io_max_bytes_per_second,
            log_cleaner_backoff_ms,
            log_cleaner_min_clean_ratio,
            log_cleaner_enable,
            log_cleaner_delete_retention_ms,
            log_cleaner_min_compaction_lag_ms,
            log_cleaner_max_compaction_lag_ms,
            log_index_interval_bytes,
            log_flush_interval_messages,
            log_delete_delay_ms,
            log_flush_scheduler_interval_ms,
            log_flush_interval_ms,
            log_flush_offset_checkpoint_interval_ms,
            log_flush_start_offset_checkpoint_interval_ms,
            log_pre_allocate_enable,
            log_message_format_version,
            log_message_timestamp_type,
            log_message_timestamp_difference_max_ms,
            num_recovery_threads_per_data_dir,
            min_in_sync_replicas,
            log_message_down_conversion_enable,
        }
    }
}
