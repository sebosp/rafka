//! Kafka Config - Log Configuration

use super::quota::PRODUCER_QUOTA_BYTES_PER_SECOND_DEFAULT_PROP;
use super::{ConfigSet, KafkaConfigError};
use crate::common::config_def::{ConfigDef, ConfigDefImportance};
use crate::common::record::legacy_record;
use enum_iterator::IntoEnumIterator;
use std::fmt;
use std::str::FromStr;
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
pub const LOG_CLEANER_THREADS_PROP: &str = "log.cleaner.threads";
pub const NUM_RECOVERY_THREADS_PER_DATA_DIR_PROP: &str = "num.recovery.threads.per.data.dir";
pub const LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP: &str = "log.cleaner.dedupe.buffer.size";
pub const LOG_CLEANER_DEDUPE_BUFFER_LOAD_FACTOR_PROP: &str = "log.cleaner.io.buffer.load.factor";
pub const LOG_CLEANER_IO_BUFFER_SIZE_PROP: &str = "log.cleaner.io.buffer.size";
pub const LOG_FLUSH_SCHEDULER_INTERVAL_MS_PROP: &str = "log.flush.scheduler.interval.ms";
pub const LOG_FLUSH_INTERVAL_MS_PROP: &str = "log.flush.interval.ms";
pub const LOG_FLUSH_OFFSET_CHECKPOINT_INTERVAL_MS_PROP: &str =
    "log.flush.offset.checkpoint.interval.ms";
pub const LOG_FLUSH_START_OFFSET_CHECKPOINT_INTERVAL_MS_PROP: &str =
    "log.flush.start.offset.checkpoint.interval.ms";

#[derive(Debug, IntoEnumIterator)]
pub enum LogConfigKey {
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
    LogCleanerThreads,
    NumRecoveryThreadsPerDataDir,
    LogCleanerDedupeBufferSize,
    LogCleanerIoBufferSize,
    LogCleanerDedupeBufferLoadFactor,
    LogFlushSchedulerIntervalMs,
    LogFlushIntervalMs,
    LogFlushOffsetCheckpointIntervalMs,
    LogFlushStartOffsetCheckpointIntervalMs,
}

impl fmt::Display for LogConfigKey {
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
            Self::LogCleanerThreads => write!(f, "{}", LOG_CLEANER_THREADS_PROP),
            Self::NumRecoveryThreadsPerDataDir => {
                write!(f, "{}", NUM_RECOVERY_THREADS_PER_DATA_DIR_PROP)
            },
            Self::LogCleanerDedupeBufferSize => {
                write!(f, "{}", LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP)
            },
            Self::LogCleanerIoBufferSize => write!(f, "{}", LOG_CLEANER_IO_BUFFER_SIZE_PROP),
            Self::LogCleanerDedupeBufferLoadFactor => {
                write!(f, "{}", LOG_CLEANER_DEDUPE_BUFFER_LOAD_FACTOR_PROP)
            },
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
        }
    }
}

impl FromStr for LogConfigKey {
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
            LOG_CLEANER_THREADS_PROP => Ok(Self::LogCleanerThreads),
            NUM_RECOVERY_THREADS_PER_DATA_DIR_PROP => Ok(Self::NumRecoveryThreadsPerDataDir),
            LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP => Ok(Self::LogCleanerDedupeBufferSize),
            LOG_CLEANER_IO_BUFFER_SIZE_PROP => Ok(Self::LogCleanerIoBufferSize),
            LOG_CLEANER_DEDUPE_BUFFER_LOAD_FACTOR_PROP => {
                Ok(Self::LogCleanerDedupeBufferLoadFactor)
            },
            LOG_FLUSH_SCHEDULER_INTERVAL_MS_PROP => Ok(Self::LogFlushSchedulerIntervalMs),
            LOG_FLUSH_INTERVAL_MS_PROP => Ok(Self::LogFlushIntervalMs),
            LOG_FLUSH_OFFSET_CHECKPOINT_INTERVAL_MS_PROP => {
                Ok(Self::LogFlushOffsetCheckpointIntervalMs)
            },
            LOG_FLUSH_START_OFFSET_CHECKPOINT_INTERVAL_MS_PROP => {
                Ok(Self::LogFlushStartOffsetCheckpointIntervalMs)
            },
            _ => Err(KafkaConfigError::UnknownKey(input.to_string())),
        }
    }
}

#[derive(Debug)]
pub struct LogConfigProperties {
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
    log_cleaner_threads: ConfigDef<i32>,
    num_recovery_threads_per_data_dir: ConfigDef<i32>,
    log_cleaner_dedupe_buffer_size: ConfigDef<i64>,
    log_cleaner_io_buffer_size: ConfigDef<i32>,
    log_cleaner_dedupe_buffer_load_factor: ConfigDef<f64>,
    log_flush_scheduler_interval_ms: ConfigDef<i64>,
    log_flush_interval_ms: ConfigDef<i64>,
    log_flush_offset_checkpoint_interval_ms: ConfigDef<i32>,
    log_flush_start_offset_checkpoint_interval_ms: ConfigDef<i32>,
}
impl Default for LogConfigProperties {
    fn default() -> Self {
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
            log_cleaner_threads: ConfigDef::default()
                .with_key(LOG_CLEANER_THREADS_PROP)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(String::from("The number of background threads to use for log cleaning"))
                .with_default(1)
                .with_validator(Box::new(|data| {
                    // Safe to unwrap, we have a default
                    ConfigDef::at_least(data, &0, LOG_CLEANER_THREADS_PROP)
                })),
            num_recovery_threads_per_data_dir: ConfigDef::default()
                .with_key(NUM_RECOVERY_THREADS_PER_DATA_DIR_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(String::from(
                    "The number of threads per data directory to be used for log recovery at \
                     startup and flushing at shutdown",
                ))
                .with_default(1),
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
            Self::ConfigKey::NumRecoveryThreadsPerDataDir => {
                self.num_recovery_threads_per_data_dir.try_set_parsed_value(property_value)?
            },
        };
        Ok(())
    }
}
#[derive(Debug, PartialEq, Clone)]
pub struct LogConfig {}
impl Default for LogConfig {}
impl LogConfig {}
