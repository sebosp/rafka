//! Core Kafka Config
//! core/src/main/scala/kafka/server/KafkaConfig.scala
//! Changes:
//! - No SSL, no SASL
//! - RAFKA NOTE: Using serde_json doesn't work very well because for example
//! ADVERTISED_LISTENERS are variable keys that need to be decomposed into actual listeners

use crate::cluster::end_point::EndPoint;
use crate::common::config::topic_config;
use crate::common::config_def::{ConfigDef, ConfigDefImportance};
use crate::common::record::legacy_record;
use crate::common::record::records;
use crate::server::client_quota_manager;
use crate::utils::core_utils;
use fs_err::File;
use std::collections::HashMap;
use std::io::{self, BufReader};
use std::num;
use std::str::FromStr;
use thiserror::Error;
use tracing::{debug, warn};

// General section
pub const BROKER_ID_GENERATION_ENABLED_PROP: &str = "broker.id.generation.enable";
pub const RESERVED_BROKER_MAX_ID_PROP: &str = "reserved.broker.max.id";
pub const BROKER_ID_PROP: &str = "broker.id";
pub const MESSAGE_MAX_BYTES_PROP: &str = "message.max.bytes";

// Log section
pub const LOG_DIRS_PROP: &str = "log.dirs";
pub const LOG_DIR_PROP: &str = "log.dir";
pub const LOG_SEGMENT_BYTES_PROP: &str = "log.segment.bytes";
pub const LOG_ROLL_TIME_MILLIS_PROP: &str = "log.roll.ms";
pub const LOG_ROLL_TIME_HOURS_PROP: &str = "log.roll.hours";
pub const LOG_ROLL_TIME_JITTER_MILLIS_PROP: &str = "log.roll.jitter.ms";
pub const LOG_ROLL_TIME_JITTER_HOURS_PROP: &str = "log.roll.jitter.hours";
pub const LOG_RETENTION_TIME_MILLIS_PROP: &str = "log.retention.ms";
pub const LOG_RETENTION_TIME_MINUTES_PROP: &str = "log.retention.minutes";
pub const LOG_RETENTION_TIME_HOURS_PROP: &str = "log.retention.hours";
pub const LOG_CLEANER_THREADS_PROP: &str = "log.cleaner.threads";
pub const LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP: &str = "log.cleaner.dedupe.buffer.size";
pub const LOG_CLEANER_DEDUPE_BUFFER_LOAD_FACTOR_PROP: &str = "log.cleaner.io.buffer.load.factor";
pub const LOG_CLEANER_IO_BUFFER_SIZE_PROP: &str = "log.cleaner.io.buffer.size";
pub const LOG_FLUSH_SCHEDULER_INTERVAL_MS_PROP: &str = "log.flush.scheduler.interval.ms";
pub const LOG_FLUSH_INTERVAL_MS_PROP: &str = "log.flush.interval.ms";
pub const LOG_FLUSH_OFFSET_CHECKPOINT_INTERVAL_MS_PROP: &str =
    "log.flush.offset.checkpoint.interval.ms";
pub const LOG_FLUSH_START_OFFSET_CHECKPOINT_INTERVAL_MS_PROP: &str =
    "log.flush.start.offset.checkpoint.interval.ms";
pub const NUM_RECOVERY_THREADS_PER_DATA_DIR_PROP: &str = "num.recovery.threads.per.data.dir";

// Socket server section
pub const PORT_PROP: &str = "port";
pub const HOST_NAME_PROP: &str = "host.name";
pub const LISTENERS_PROP: &str = "listeners";
pub const ADVERTISED_HOST_NAME_PROP: &str = "advertised.host.name";
pub const ADVERTISED_PORT_PROP: &str = "advertised.port";
pub const ADVERTISED_LISTENERS_PROP: &str = "advertised.listeners";
pub const MAX_CONNECTIONS_PROP: &str = "max.connections";
// Zookeeper section
pub const ZOOKEEPER_CONNECT_PROP: &str = "zookeeper.connect";
pub const ZOOKEEPER_SESSION_TIMEOUT_PROP: &str = "zookeeper.session.timeout.ms";
pub const ZOOKEEPER_CONNECTION_TIMEOUT_PROP: &str = "zookeeper.connection.timeout.ms";
pub const ZOOKEEPER_MAX_IN_FLIGHT_REQUESTS: &str = "zookeeper.max.in.flight.requests";

// Quota section
pub const CONSUMER_QUOTA_BYTES_PER_SECOND_DEFAULT_PROP: &str = "quota.consumer.default";
pub const PRODUCER_QUOTA_BYTES_PER_SECOND_DEFAULT_PROP: &str = "quota.producer.default";
pub const QUOTA_WINDOW_SIZE_SECONDS_PROP: &str = "quota.window.size.seconds";

// RAFKA TODO: Since ConfigDef was moved, maybe this ConfigError should be moved there and be
// generalized?

// A Helper Enum to aid with the miriad of properties that could be forgotten to be matched.
#[derive(Debug)]
pub enum KafkaConfigKey {
    BrokerIdGenerationEnable,
    ReservedBrokerMaxId,
    BrokerId,
    MessageMaxBytes,
    Port,
    HostName,
    Listeners,
    AdvertisedHostName,
    AdvertisedPort,
    AdvertisedListeners,
    ZkConnect,
    ZkSessionTimeoutMs,
    ZkConnectionTimeoutMs,
    LogDir,
    LogDirs,
    LogSegmentBytes,
    ZkMaxInFlightRequests,
    ConsumerQuotaBytesPerSecondDefault,
    ProducerQuotaBytesPerSecondDefault,
    QuotaWindowSizeSeconds,
    LogRollTimeMillis,
    LogRollTimeHours,
    LogRollTimeJitterMillis,
    LogRollTimeJitterHours,
    LogRetentionTimeMillis,
    LogRetentionTimeMinutes,
    LogRetentionTimeHours,
    LogCleanerThreads,
    LogCleanerDedupeBufferSize,
    LogCleanerDedupeBufferLoadFactor,
    LogCleanerIoBufferSize,
    LogFlushSchedulerIntervalMs,
    LogFlushIntervalMs,
    LogFlushOffsetCheckpointIntervalMs,
    LogFlushStartOffsetCheckpointIntervalMs,
    NumRecoveryThreadsPerDataDir,
}

impl FromStr for KafkaConfigKey {
    type Err = KafkaConfigError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            BROKER_ID_GENERATION_ENABLE_PROP => Ok(Self::BrokerIdGenerationEnable),
            RESERVED_BROKER_MAX_ID_PROP => Ok(Self::ReservedBrokerMaxId),
            BROKER_ID_PROP => Ok(Self::BrokerId),
            MESSAGE_MAX_BYTES_PROP => Ok(Self::MessageMaxBytes),
            PORT_PROP => Ok(Self::Port),
            HOST_NAME_PROP => Ok(Self::HostName),
            LISTENERS_PROP => Ok(Self::Listeners),
            ADVERTISED_HOST_NAME_PROP => Ok(Self::AdvertisedHostName),
            ADVERTISED_PORT_PROP => Ok(Self::AdvertisedPort),
            ADVERTISED_LISTENERS_PROP => Ok(Self::AdvertisedListeners),
            ZOOKEEPER_CONNECT_PROP => Ok(Self::ZkConnect),
            ZOOKEEPER_SESSION_TIMEOUT_PROP => Ok(Self::ZkSessionTimeoutMs),
            ZOOKEEPER_CONNECTION_TIMEOUT_PROP => Ok(Self::ZkConnectionTimeoutMs),
            LOG_DIR_PROP => Ok(Self::LogDir),
            LOG_DIRS_PROP => Ok(Self::LogDirs),
            LOG_SEGMENT_BYTES_PROP => Ok(Self::LogSegmentBytes),
            ZK_MAX_IN_FLIGHT_REQUESTS_PROP => Ok(Self::ZkMaxInFlightRequests),
            CONSUMER_QUOTA_BYTES_PER_SECOND_DEFAULT_PROP => {
                Ok(Self::ConsumerQuotaBytesPerSecondDefault)
            },
            PRODUCER_QUOTA_BYTES_PER_SECOND_DEFAULT_PROP => {
                Ok(Self::ProducerQuotaBytesPerSecondDefault)
            },
            QUOTA_WINDOW_SIZE_SECONDS_PROP => Ok(Self::QuotaWindowSizeSeconds),
            LOG_ROLL_TIME_MILLIS_PROP => Ok(Self::LogRollTimeMillis),
            LOG_ROLL_TIME_HOURS_PROP => Ok(Self::LogRollTimeHours),
            LOG_ROLL_TIME_JITTER_MILLIS_PROP => Ok(Self::LogRollTimeJitterMillis),
            LOG_ROLL_TIME_JITTER_HOURS_PROP => Ok(Self::LogRollTimeJitterHours),
            LOG_RETENTION_TIME_MILLIS_PROP => Ok(Self::LogRetentionTimeMillis),
            LOG_RETENTION_TIME_MINUTES_PROP => Ok(Self::LogRetentionTimeMinutes),
            LOG_RETENTION_TIME_HOURS_PROP => Ok(Self::LogRetentionTimeHours),
            LOG_CLEANER_THREADS_PROP => Ok(Self::LogCleanerThreads),
            LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP => Ok(Self::LogCleanerDedupeBufferSize),
            LOG_CLEANER_DEDUPE_BUFFER_LOAD_FACTOR_PROP => {
                Ok(Self::LogCleanerDedupeBufferLoadFactor)
            },
            LOG_CLEANER_IO_BUFFER_SIZE_PROP => Ok(Self::LogCleanerIoBufferSize),
            LOG_FLUSH_SCHEDULER_INTERVAL_MS_PROP => Ok(Self::LogFlushSchedulerIntervalMs),
            LOG_FLUSH_INTERVAL_MS_PROP => Ok(Self::LogFlushIntervalMs),
            LOG_FLUSH_OFFSET_CHECKPOINT_INTERVAL_MS_PROP => {
                Ok(Self::LogFlushOffsetCheckpointIntervalMs)
            },
            LOG_FLUSH_START_OFFSET_CHECKPOINT_INTERVAL_MS_PROP => {
                Ok(Self::LogFlushStartOffsetCheckpointIntervalMs)
            },
            NUM_RECOVERY_THREADS_PER_DATA_DIR_PROP => Ok(Self::NumRecoveryThreadsPerDataDir),
            _ => Err(KafkaConfigError::UnknownKey(input.to_string())),
        }
    }
}

/// `KafkaConfigError` is a custom error that is returned when properties are invalid, unknown,
/// missing or the config file is not readable.
#[derive(Error, Debug)]
pub enum KafkaConfigError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Property error: {0}")]
    Property(#[from] java_properties::PropertiesError),
    #[error("ParseInt error: {0}")]
    ParseInt(#[from] num::ParseIntError),
    #[error("ParseBool error: {0}")]
    ParseBool(#[from] std::str::ParseBoolError),
    // We try to call parse::<T> on everything, even String -> String, which doesn't make sense.
    // TODO: Cleanup
    #[error("Infalible String Error {0:?}")]
    Infallible(#[from] std::convert::Infallible),
    #[error("Missing Key error: {0:?}")]
    MissingKey(String),
    #[error("Invalid Value: {0}")]
    InvalidValue(String),
    #[error("Unknown Key: {0}")]
    UnknownKey(String),
    #[error("Duplicate Key: {0}")]
    DuplicateKey(String),
    #[error("Attempt to compare a value that is not provided and has no default: {0}")]
    ComparisonOnNone(String),
    #[error("ListenerMisconfig")]
    ListenerMisconfig(String),
}

/// This implementation is only for testing, for example any I/O error is considered equal
impl PartialEq for KafkaConfigError {
    fn eq(&self, rhs: &Self) -> bool {
        match self {
            Self::Io(_) => matches!(rhs, Self::Io(_)),
            Self::Property(lhs) => {
                // TODO: create a method that expects a string and returns the java_properties
                matches!(rhs, Self::Property(rhs) if lhs.line_number() == rhs.line_number())
            },
            Self::ParseInt(lhs) => matches!(rhs, Self::ParseInt(rhs) if lhs == rhs),
            Self::ParseBool(lhs) => matches!(rhs, Self::ParseBool(rhs) if lhs == rhs),
            Self::Infallible(lhs) => matches!(rhs, Self::Infallible(rhs) if lhs == rhs),
            Self::MissingKey(lhs) => matches!(rhs, Self::MissingKey(rhs) if lhs == rhs),
            Self::InvalidValue(lhs) => matches!(rhs, Self::InvalidValue(rhs) if lhs == rhs),
            Self::UnknownKey(lhs) => matches!(rhs, Self::UnknownKey(rhs) if lhs == rhs),
            Self::DuplicateKey(lhs) => matches!(rhs, Self::DuplicateKey(rhs) if lhs == rhs),
            Self::ComparisonOnNone(lhs) => matches!(rhs, Self::ComparisonOnNone(rhs) if lhs == rhs),
            Self::ListenerMisconfig(lhs) => {
                matches!(rhs, Self::ListenerMisconfig(rhs) if lhs == rhs)
            },
        }
    }
}

#[derive(Debug)]
pub struct KafkaConfigProperties {
    broker_id_generation_enable: ConfigDef<bool>,
    reserved_broker_max_id: ConfigDef<i32>,
    broker_id: ConfigDef<i32>,
    message_max_bytes: ConfigDef<usize>,
    port: ConfigDef<i32>,
    host_name: ConfigDef<String>,
    listeners: ConfigDef<String>,
    advertised_host_name: ConfigDef<String>,
    advertised_port: ConfigDef<i32>,
    advertised_listeners: ConfigDef<String>,
    zk_connect: ConfigDef<String>,
    zk_session_timeout_ms: ConfigDef<u32>,
    zk_connection_timeout_ms: ConfigDef<u32>,
    // Singular log.dir
    log_dir: ConfigDef<String>,
    // Multiple comma separated log.dirs, may include spaces after the comma (will be trimmed)
    log_dirs: ConfigDef<String>,
    log_segment_bytes: ConfigDef<usize>,
    zk_max_in_flight_requests: ConfigDef<u32>,
    consumer_quota_bytes_per_second_default: ConfigDef<i64>,
    producer_quota_bytes_per_second_default: ConfigDef<i64>,
    quota_window_size_seconds: ConfigDef<i32>,
    log_roll_time_millis: ConfigDef<i64>,
    log_roll_time_hours: ConfigDef<i32>,
    log_roll_time_jitter_millis: ConfigDef<i64>,
    log_roll_time_jitter_hours: ConfigDef<i32>,
    log_retention_time_millis: ConfigDef<i64>,
    log_retention_time_minutes: ConfigDef<i32>,
    log_retention_time_hours: ConfigDef<i32>,
    log_cleaner_threads: ConfigDef<i32>,
    log_cleaner_dedupe_buffer_size: ConfigDef<i64>,
    log_flush_scheduler_interval_ms: ConfigDef<i64>,
    log_flush_interval_ms: ConfigDef<i64>,
    log_flush_offset_checkpoint_interval_ms: ConfigDef<i32>,
    log_flush_start_offset_checkpoint_interval_ms: ConfigDef<i32>,
    num_recovery_threads_per_data_dir: ConfigDef<i32>,
}

impl Default for KafkaConfigProperties {
    fn default() -> Self {
        Self {
            broker_id_generation_enable: ConfigDef::default()
                .with_key(BROKER_ID_GENERATION_ENABLED_PROP)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(
                    format!("Enable automatic broker id generation on the server. When enabled the value \
                     configured for {} should be reviewed.", RESERVED_BROKER_MAX_ID_PROP)
                )
                .with_default(true),
            reserved_broker_max_id: ConfigDef::default()
                .with_key(RESERVED_BROKER_MAX_ID_PROP)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(format!("Max number that can be used for a {}", BROKER_ID_PROP))
                .with_default(1000)
                .with_validator(Box::new(|data| {
                    // Safe to unwrap, we have a default
                    ConfigDef::at_least(data, &0, RESERVED_BROKER_MAX_ID_PROP)
                })),
            broker_id: ConfigDef::default()
                .with_key(BROKER_ID_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(
                    format!("The broker id for this server. If unset, a unique broker id will be generated. \
                     To avoid conflicts between zookeeper generated broker id's and user configured \
                     broker id's, generated broker ids start from {} + 1.", RESERVED_BROKER_MAX_ID_PROP),
                )
                .with_default(-1),
            message_max_bytes: ConfigDef::default()
                .with_key(MESSAGE_MAX_BYTES_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_default(1024 * 1024 + records::LOG_OVERHEAD)
                .with_doc(
                    format!("{} This can be set per topic with the topic level `{}` config.", topic_config::MAX_MESSAGE_BYTES_DOC, topic_config::MAX_MESSAGE_BYTES_CONFIG)
                )
                .with_validator(Box::new(|data| {
                    // Safe to unwrap, we have a default
                    ConfigDef::at_least(data, &0, MESSAGE_MAX_BYTES_PROP)
                })),
            port: ConfigDef::default()
                .with_key(PORT_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(format!(
                    "DEPRECATED: only used when `listeners` is not set. Use `{}` instead. the port to listen and accept connections on", LISTENERS_PROP
                ))
                .with_default(9092),
            host_name: ConfigDef::default()
                .with_key(HOST_NAME_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(format!(
                    "DEPRECATED: only used when `listeners` is not set. Use `{}` instead. \
                    hostname of broker. If this is set, it will only bind to this address. If this is not set, it will bind to all interfaces", LISTENERS_PROP
                ))
                .with_default(String::from("")),
            listeners: ConfigDef::default()
                .with_key(LISTENERS_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(
                    format!("Listener List - Comma-separated list of URIs we will listen on and the listener names. \
                      NOTE: RAFKA does not implement listener security protocols \
                      Specify hostname as 0.0.0.0 to bind to all interfaces. \
                      Leave hostname empty to bind to default interface. \
                      Examples of legal listener lists: \
                      PLAINTEXT://myhost:9092,SSL://:9091 \
                      CLIENT://0.0.0.0:9092,REPLICATION://localhost:9093"
                )),
            advertised_listeners: ConfigDef::default()
                .with_key(ADVERTISED_LISTENERS_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(format!(
                    "Listeners to publish to ZooKeeper for clients to use, if different than the `{}` config property.\
                    In IaaS environments, this may need to be different from the interface to which the broker binds. \
                    If this is not set, the value for `listeners` will be used. \
                    Unlike `listeners` it is not valid to advertise the 0.0.0.0 meta-address ", LISTENERS_PROP
                )),
            advertised_host_name: ConfigDef::default()
                .with_key(ADVERTISED_HOST_NAME_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(format!(
  "DEPRECATED: only used when `{}` or `{}` are not set. \
  Use `{}` instead. \
  Hostname to publish to ZooKeeper for clients to use. In IaaS environments, this may \
  need to be different from the interface to which the broker binds. If this is not set, \
  it will use the value for `{}` if configured. Otherwise \
  it will use the value returned from gethostname", ADVERTISED_LISTENERS_PROP, LISTENERS_PROP, ADVERTISED_LISTENERS_PROP, HOST_NAME_PROP
                   )),
            advertised_port: ConfigDef::default()
                .with_key(ADVERTISED_PORT_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(format!(
  "DEPRECATED: only used when `{}` or `{}` are not set. \
  Use `{}` instead. \
  The port to publish to ZooKeeper for clients to use. In IaaS environments, this may \
  need to be different from the port to which the broker binds. If this is not set, \
  it will publish the same port that the broker binds to.", ADVERTISED_LISTENERS_PROP, LISTENERS_PROP, ADVERTISED_LISTENERS_PROP
                    )),
            zk_connect: ConfigDef::default()
                .with_key(ZOOKEEPER_CONNECT_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(String::from(r#"
                    Specifies the ZooKeeper connection string in the form <code>hostname:port</code> where host and port are the
                    host and port of a ZooKeeper server. To allow connecting through other ZooKeeper nodes when that ZooKeeper machine is
                    down you can also specify multiple hosts in the form <code>hostname1:port1,hostname2:port2,hostname3:port3</code>.
                    The server can also have a ZooKeeper chroot path as part of its ZooKeeper connection string which puts its data under some path in the global ZooKeeper namespace.
                    For example to give a chroot path of `/chroot/path` you would give the connection string as `hostname1:port1,hostname2:port2,hostname3:port3/chroot/path`.
                    "#
                )),
            zk_session_timeout_ms: ConfigDef::default()
                .with_key(ZOOKEEPER_SESSION_TIMEOUT_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(String::from("Zookeeper session timeout"))
                .with_default(18000),
            zk_connection_timeout_ms: ConfigDef::default()
                .with_key(ZOOKEEPER_CONNECTION_TIMEOUT_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(
                    format!("The max time that the client waits to establish a connection to zookeeper. If \
                     not set, the value in {} is used", ZOOKEEPER_SESSION_TIMEOUT_PROP) // REQ-01
                ),
            zk_max_in_flight_requests: ConfigDef::default()
                .with_importance(ConfigDefImportance::High)
                .with_doc(String::from(
                    "The maximum number of unacknowledged requests the client will send to Zookeeper before blocking."
                ))
                .with_default(10)
                .with_validator(Box::new(|data| {
                    // RAFKA TODO: This doesn't make much sense if it's u32...
                    ConfigDef::at_least(data, &1, ZOOKEEPER_MAX_IN_FLIGHT_REQUESTS)
                    })),
            log_dir: ConfigDef::default()
                .with_key(LOG_DIR_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(
                    format!("The directory in which the log data is kept (supplemental for {} property)", LOG_DIRS_PROP),
                )
                .with_default(String::from("/tmp/kafka-logs")),
            log_dirs: ConfigDef::default()
                .with_key(LOG_DIRS_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(
                    format!("The directories in which the log data is kept. If not set, the value in {} \
                     is used", LOG_DIR_PROP),
                ),
            log_segment_bytes: ConfigDef::default()
                .with_key(LOG_SEGMENT_BYTES_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(String::from("The maximum size of a single log file"))
                .with_default(1 * 1024 * 1024 * 1024)
                .with_validator(Box::new(|data| {
                    // RAFKA TODO: This doesn't make much sense if it's u32...
                    ConfigDef::at_least(data, &legacy_record::RECORD_OVERHEAD_V0, LOG_SEGMENT_BYTES_PROP)
                    })),
            consumer_quota_bytes_per_second_default: ConfigDef::default()
                .with_key(CONSUMER_QUOTA_BYTES_PER_SECOND_DEFAULT_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(String::from(
                    "DEPRECATED: Used only when dynamic default quotas are not configured for <user, <client-id> or <user, client-id> in Zookeeper. \
                    Any consumer distinguished by clientId/consumer group will get throttled if it fetches more bytes than this value per-second"
                ))
                .with_default(client_quota_manager::QUOTA_BYTES_PER_SECOND_DEFAULT)
                .with_validator(Box::new(|data| {
                    // Safe to unwrap, we have a default
                    ConfigDef::at_least(data, &1, CONSUMER_QUOTA_BYTES_PER_SECOND_DEFAULT_PROP)
                })),
            producer_quota_bytes_per_second_default: ConfigDef::default()
                .with_key(PRODUCER_QUOTA_BYTES_PER_SECOND_DEFAULT_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(String::from(
                    "DEPRECATED: Used only when dynamic default quotas are not configured for <user, <client-id> or <user, client-id> in Zookeeper. \
                    Any consumer distinguished by clientId/consumer group will get throttled if it fetches more bytes than this value per-second"
                ))
                .with_default(client_quota_manager::QUOTA_BYTES_PER_SECOND_DEFAULT)
                .with_validator(Box::new(|data| {
                    // Safe to unwrap, we have a default
                    ConfigDef::at_least(data, &1, PRODUCER_QUOTA_BYTES_PER_SECOND_DEFAULT_PROP)
                })),
            quota_window_size_seconds: ConfigDef::default()
                .with_key(QUOTA_WINDOW_SIZE_SECONDS_PROP)
                .with_importance(ConfigDefImportance::Low)
                .with_doc(String::from(
                    "The time span of each sample for client quotas"
                ))
                .with_default(client_quota_manager::QUOTA_WINDOW_SIZE_SECONDS_DEFAULT)
                .with_validator(Box::new(|data| {
                    // Safe to unwrap, we have a default
                    ConfigDef::at_least(data, &1, PRODUCER_QUOTA_BYTES_PER_SECOND_DEFAULT_PROP)
                })),
            log_roll_time_millis: ConfigDef::default()
                .with_key(LOG_ROLL_TIME_MILLIS_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_validator(Box::new(|data| {
                    // Safe to unwrap, we have a default
                    ConfigDef::at_least(data, &0, PRODUCER_QUOTA_BYTES_PER_SECOND_DEFAULT_PROP)
                }))
                .with_doc(format!(
                        "The maximum time before a new log segment is rolled out (in milliseconds). If not set, the value in {} is used", LOG_ROLL_TIME_HOURS_PROP
                )),
            log_roll_time_hours: ConfigDef::default()
                .with_key(LOG_ROLL_TIME_HOURS_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(format!(
                        "The maximum time before a new log segment is rolled out (in hours), secondary to {} property", LOG_ROLL_TIME_MILLIS_PROP
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
                        "The maximum jitter to subtract from logRollTimeMillis (in milliseconds). If not set, the value in {} is used", LOG_ROLL_TIME_JITTER_HOURS_PROP
                )),
            log_roll_time_jitter_hours: ConfigDef::default()
                .with_key(LOG_ROLL_TIME_JITTER_HOURS_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(format!(
                         "The maximum jitter to subtract from logRollTimeMillis (in hours), secondary to {} property", LOG_ROLL_TIME_JITTER_MILLIS_PROP
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
                        "The number of milliseconds to keep a log file before deleting it (in milliseconds), If not set, the value in {} is used. If set to -1, no time limit is applied.", LOG_RETENTION_TIME_MINUTES_PROP
                )),
            log_retention_time_minutes: ConfigDef::default()
                .with_key(LOG_RETENTION_TIME_MINUTES_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(format!(
                        "The number of minutes to keep a log file before deleting it (in minutes), secondary to {} property. If not set, the value in {} is used", LOG_RETENTION_TIME_MILLIS_PROP, LOG_RETENTION_TIME_HOURS_PROP
                )),
            log_retention_time_hours: ConfigDef::default()
                .with_key(LOG_RETENTION_TIME_HOURS_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(format!(
                    "The number of hours to keep a log file before deleting it (in hours), tertiary to {} property", LOG_RETENTION_TIME_MILLIS_PROP
                    ))
                .with_default(24 * 7),
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
                .with_doc(String::from("The total memory used for log deduplication across all cleaner threads"))
                .with_default(128 * 1024 * 1024),
            log_flush_scheduler_interval_ms: ConfigDef::default()
                .with_key(LOG_FLUSH_SCHEDULER_INTERVAL_MS_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(String::from("The frequency in ms that the log flusher checks whether any log needs to be flushed to disk"))
                .with_default(i64::MAX),
            log_flush_interval_ms: ConfigDef::default()
                .with_key(LOG_FLUSH_INTERVAL_MS_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(format!(
                        "The maximum time in ms that a message in any topic is kept in memory before flushed to disk. If not set, the value in {} is used", LOG_FLUSH_SCHEDULER_INTERVAL_MS_PROP
                ))
                .with_default(i64::MAX),
            log_flush_offset_checkpoint_interval_ms: ConfigDef::default()
                .with_key(LOG_FLUSH_OFFSET_CHECKPOINT_INTERVAL_MS_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(String::from("The frequency with which we update the persistent record of the last flush which acts as the log recovery point"))
                .with_default(60000)
                .with_validator(Box::new(|data| {
                    // Safe to unwrap, we have a default
                    ConfigDef::at_least(data, &0, LOG_CLEANER_THREADS_PROP)
                })),
            log_flush_start_offset_checkpoint_interval_ms: ConfigDef::default()
                .with_key(LOG_FLUSH_START_OFFSET_CHECKPOINT_INTERVAL_MS_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(String::from("The frequency with which we update the persistent record of log start offset"))
                .with_default(60000)
                .with_validator(Box::new(|data| {
                    // Safe to unwrap, we have a default
                    ConfigDef::at_least(data, &0, LOG_CLEANER_THREADS_PROP)
                })),
            num_recovery_threads_per_data_dir: ConfigDef::default()
                .with_key(NUM_RECOVERY_THREADS_PER_DATA_DIR_PROP)
                .with_doc(String::from(
                        "The number of threads per data directory to be used for log recovery at startup and flushing at shutdown"
                ))
                .with_default(1),
        }
    }
}

impl KafkaConfigProperties {
    /// `try_from_config_property` transforms a string value from the config into our actual types
    pub fn try_set_property(
        &mut self,
        property_name: &str,
        property_value: &str,
    ) -> Result<(), KafkaConfigError> {
        let kafka_config_key = KafkaConfigKey::from_str(property_name)?;
        match kafka_config_key {
            KafkaConfigKey::BrokerIdGenerationEnable => {
                self.broker_id_generation_enable.try_set_parsed_value(property_value)?
            },
            KafkaConfigKey::ReservedBrokerMaxId => {
                self.reserved_broker_max_id.try_set_parsed_value(property_value)?
            },
            KafkaConfigKey::BrokerId => self.broker_id.try_set_parsed_value(property_value)?,
            KafkaConfigKey::MessageMaxBytes => {
                self.message_max_bytes.try_set_parsed_value(property_value)?
            },
            KafkaConfigKey::Port => self.port.try_set_parsed_value(property_value)?,
            KafkaConfigKey::HostName => self.host_name.try_set_parsed_value(property_value)?,
            KafkaConfigKey::Listeners => self.listeners.try_set_parsed_value(property_value)?,
            KafkaConfigKey::AdvertisedHostName => {
                self.advertised_host_name.try_set_parsed_value(property_value)?
            },
            KafkaConfigKey::AdvertisedPort => {
                self.advertised_port.try_set_parsed_value(property_value)?
            },
            KafkaConfigKey::AdvertisedListeners => {
                self.advertised_listeners.try_set_parsed_value(property_value)?
            },
            KafkaConfigKey::ZkConnect => self.zk_connect.try_set_parsed_value(property_value)?,
            KafkaConfigKey::ZkSessionTimeoutMs => {
                self.zk_session_timeout_ms.try_set_parsed_value(property_value)?
            },
            KafkaConfigKey::ZkConnectionTimeoutMs => {
                self.zk_connection_timeout_ms.try_set_parsed_value(property_value)?
            },
            KafkaConfigKey::LogDir => self.log_dir.try_set_parsed_value(property_value)?,
            KafkaConfigKey::LogDirs => self.log_dirs.try_set_parsed_value(property_value)?,
            KafkaConfigKey::LogSegmentBytes => {
                self.log_segment_bytes.try_set_parsed_value(property_value)?
            },
            KafkaConfigKey::ConsumerQuotaBytesPerSecondDefault => {
                self.consumer_quota_bytes_per_second_default.try_set_parsed_value(property_value)?
            },
            KafkaConfigKey::ProducerQuotaBytesPerSecondDefault => {
                self.producer_quota_bytes_per_second_default.try_set_parsed_value(property_value)?
            },
            KafkaConfigKey::QuotaWindowSizeSeconds => {
                self.quota_window_size_seconds.try_set_parsed_value(property_value)?
            },
            KafkaConfigKey::LogRollTimeMillis => {
                self.log_roll_time_millis.try_set_parsed_value(property_value)?
            },
            KafkaConfigKey::LogRollTimeHours => {
                self.log_roll_time_hours.try_set_parsed_value(property_value)?
            },
            KafkaConfigKey::LogRollTimeJitterMillis => {
                self.log_roll_time_jitter_millis.try_set_parsed_value(property_value)?
            },
            KafkaConfigKey::LogRollTimeJitterHours => {
                self.log_roll_time_jitter_hours.try_set_parsed_value(property_value)?
            },
            KafkaConfigKey::LogRetentionTimeMillis => {
                self.log_retention_time_millis.try_set_parsed_value(property_value)?
            },
            KafkaConfigKey::LogRetentionTimeMinutes => {
                self.log_retention_time_minutes.try_set_parsed_value(property_value)?
            },
            KafkaConfigKey::LogRetentionTimeHours => {
                self.log_retention_time_hours.try_set_parsed_value(property_value)?
            },
            KafkaConfigKey::LogCleanerThreads => {
                self.log_cleaner_threads.try_set_parsed_value(property_value)?
            },
            KafkaConfigKey::LogCleanerDedupeBufferSize => {
                self.log_cleaner_dedupe_buffer_size.try_set_parsed_value(property_value)?
            },
            KafkaConfigKey::LogFlushSchedulerIntervalMs => {
                self.log_flush_scheduler_interval_ms.try_set_parsed_value(property_value)?
            },
            KafkaConfigKey::LogFlushIntervalMs => {
                self.log_flush_interval_ms.try_set_parsed_value(property_value)?
            },
            KafkaConfigKey::LogFlushOffsetCheckpointIntervalMs => {
                self.log_flush_offset_checkpoint_interval_ms.try_set_parsed_value(property_value)?
            },
            KafkaConfigKey::LogFlushStartOffsetCheckpointIntervalMs => self
                .log_flush_start_offset_checkpoint_interval_ms
                .try_set_parsed_value(property_value)?,
            KafkaConfigKey::NumRecoveryThreadsPerDataDir => {
                self.num_recovery_threads_per_data_dir.try_set_parsed_value(property_value)?
            },
        };
        Ok(())
    }

    /// `config_names` returns a list of config keys used by KafkaConfigProperties
    pub fn config_names() -> Vec<String> {
        // TODO: This should be derivable somehow too.
        vec![
            ZOOKEEPER_CONNECT_PROP.to_string(),
            ZOOKEEPER_SESSION_TIMEOUT_PROP.to_string(),
            ZOOKEEPER_CONNECTION_TIMEOUT_PROP.to_string(),
            LOG_DIR_PROP.to_string(),
            LOG_DIRS_PROP.to_string(),
            BROKER_ID_GENERATION_ENABLED_PROP.to_string(),
            RESERVED_BROKER_MAX_ID_PROP.to_string(),
            BROKER_ID_PROP.to_string(),
            CONSUMER_QUOTA_BYTES_PER_SECOND_DEFAULT_PROP.to_string(),
            PRODUCER_QUOTA_BYTES_PER_SECOND_DEFAULT_PROP.to_string(),
            QUOTA_WINDOW_SIZE_SECONDS_PROP.to_string(),
            LOG_ROLL_TIME_MILLIS_PROP.to_string(),
        ]
    }

    /// Transforms from a HashMap of configs into a KafkaConfigProperties object
    /// This may return KafkaConfigError::UnknownKey errors
    pub fn from_properties_hashmap(
        input_config: HashMap<String, String>,
    ) -> Result<Self, KafkaConfigError> {
        let mut config_builder = Self::default();
        for (property, property_value) in &input_config {
            debug!("from_properties_hashmap: {} = {}", property, property_value);
            config_builder.try_set_property(property, property_value)?;
        }
        Ok(config_builder)
    }

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

    /// If user did not define advertised listeners, we'll use host:port, if they were not set
    /// either we set listeners
    pub fn resolve_advertised_listeners(&mut self) -> Result<Vec<EndPoint>, KafkaConfigError> {
        if let Some(advertised_listeners) = self.advertised_listeners.get_value() {
            core_utils::listener_list_to_end_points(&advertised_listeners)
        } else {
            warn!(
                "The properties for advertised.host.name and advertised.port have been DEPRECATED \
                 and are not used in this poc, reverting to {}",
                LISTENERS_PROP
            );
            self.resolve_listeners()
        }
    }

    /// If the user did not define listeners but did define host or port, let's use them in backward
    /// compatible way If none of those are defined, we default to PLAINTEXT://:9092
    pub fn resolve_listeners(&mut self) -> Result<Vec<EndPoint>, KafkaConfigError> {
        match self.listeners.get_value() {
            Some(val) => core_utils::listener_list_to_end_points(val),
            None => match (self.host_name.get_value(), self.port.get_value()) {
                (Some(host_name), Some(port)) => core_utils::listener_list_to_end_points(&format!(
                    "PLAINTEXT://{}:{}",
                    host_name, port
                )),
                (Some(host_name), None) => Err(KafkaConfigError::MissingKey(PORT_PROP.to_string())),
                (None, Some(port)) => Err(KafkaConfigError::MissingKey(HOST_NAME_PROP.to_string())),
                (None, None) => {
                    Err(KafkaConfigError::MissingKey(format!("{}, {}", HOST_NAME_PROP, PORT_PROP)))
                },
            },
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

    pub fn resolve_log_retention_time_millis(&mut self) -> Result<i64, KafkaConfigError> {
        let millis_in_minute = 60 * 1000;
        let millis_in_hour = 60 * millis_in_minute;

        let millis: i64 = match self.log_retention_time_millis.get_value() {
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

    /// `build` validates and resolves dependant properties from a KafkaConfigProperties into a
    /// KafkaConfig
    pub fn build(&mut self) -> Result<KafkaConfig, KafkaConfigError> {
        let zk_session_timeout_ms = self.zk_session_timeout_ms.build()?;
        // Satisties REQ-01, if zk_connection_timeout_ms is unset the value of
        // zk_connection_timeout_ms will be used.
        self.zk_connection_timeout_ms.resolve(&self.zk_session_timeout_ms);
        let zk_connection_timeout_ms = self.zk_connection_timeout_ms.build()?;
        let log_dirs = self.resolve_log_dirs()?;
        let log_segment_bytes = self.log_segment_bytes.build()?;
        let reserved_broker_max_id = self.reserved_broker_max_id.build()?;
        let broker_id = self.broker_id.build()?;
        let broker_id_generation_enable = self.broker_id_generation_enable.build()?;
        let zk_connect = self.zk_connect.build()?;
        let zk_max_in_flight_requests = self.zk_max_in_flight_requests.build()?;
        let consumer_quota_bytes_per_second_default =
            self.consumer_quota_bytes_per_second_default.build()?;
        let quota_window_size_seconds = self.quota_window_size_seconds.build()?;
        let num_recovery_threads_per_data_dir = self.num_recovery_threads_per_data_dir.build()?;
        let advertised_listeners = self.resolve_advertised_listeners()?;
        let producer_quota_bytes_per_second_default =
            self.producer_quota_bytes_per_second_default.build()?;
        let log_roll_time_millis = self.resolve_log_roll_time_millis()?;
        let log_roll_time_hours = self.log_roll_time_hours.build()?;
        let log_roll_time_jitter_millis = self.resolve_log_roll_time_jitter_millis()?;
        let log_roll_time_jitter_hours = self.log_roll_time_jitter_hours.build()?;
        let log_retention_time_millis = self.resolve_log_retention_time_millis()?;
        let log_retention_time_minutes = self.log_retention_time_minutes.build()?;
        let log_retention_time_hours = self.log_retention_time_hours.build()?;
        let log_cleaner_threads = self.log_cleaner_threads.build()?;
        let log_cleaner_dedupe_buffer_size = self.log_cleaner_dedupe_buffer_size.build()?;
        let log_flush_scheduler_interval_ms = self.log_flush_scheduler_interval_ms.build()?;
        let log_flush_interval_ms = self.log_flush_interval_ms.build()?;
        let log_flush_offset_checkpoint_interval_ms =
            self.log_flush_offset_checkpoint_interval_ms.build()?;
        let log_flush_start_offset_checkpoint_interval_ms =
            self.log_flush_start_offset_checkpoint_interval_ms.build()?;
        let kafka_config = KafkaConfig {
            zk_connect,
            zk_session_timeout_ms,
            zk_connection_timeout_ms,
            zk_max_in_flight_requests,
            log_dirs,
            log_segment_bytes,
            reserved_broker_max_id,
            broker_id_generation_enable,
            broker_id,
            consumer_quota_bytes_per_second_default,
            quota_window_size_seconds,
            num_recovery_threads_per_data_dir,
            advertised_listeners,
            producer_quota_bytes_per_second_default,
            log_roll_time_millis,
            log_roll_time_hours,
            log_roll_time_jitter_millis,
            log_roll_time_jitter_hours,
            log_retention_time_millis,
            log_retention_time_minutes,
            log_retention_time_hours,
            log_cleaner_threads,
            log_cleaner_dedupe_buffer_size,
            log_flush_scheduler_interval_ms,
            log_flush_interval_ms,
            log_flush_offset_checkpoint_interval_ms,
            log_flush_start_offset_checkpoint_interval_ms,
        };
        kafka_config.validate_values()
    }
}
#[derive(Debug, PartialEq, Clone)]
pub struct KafkaConfig {
    pub broker_id_generation_enable: bool,
    pub reserved_broker_max_id: i32,
    pub broker_id: i32,
    pub message_max_bytes: usize,
    pub port: i32,
    pub host_name: String,
    pub listeners: String,
    pub advertised_host_name: String,
    pub advertised_port: i32,
    pub advertised_listeners: Vec<EndPoint>,
    pub zk_connect: String,
    pub zk_session_timeout_ms: u32,
    pub zk_connection_timeout_ms: u32,
    pub log_dirs: Vec<String>,
    pub log_segment_bytes: usize,
    pub zk_max_in_flight_requests: u32,
    pub consumer_quota_bytes_per_second_default: i64,
    pub producer_quota_bytes_per_second_default: i64,
    pub quota_window_size_seconds: i32,
    pub log_roll_time_millis: i64,
    pub log_roll_time_hours: i32,
    pub log_roll_time_jitter_millis: i64,
    pub log_roll_time_jitter_hours: i32,
    pub log_retention_time_millis: i64,
    pub log_retention_time_minutes: i32,
    pub log_retention_time_hours: i32,
    pub log_cleaner_threads: i32,
    pub log_cleaner_dedupe_buffer_size: i64,
    pub log_flush_scheduler_interval_ms: i64,
    pub log_flush_interval_ms: i64,
    pub log_flush_offset_checkpoint_interval_ms: i32,
    pub log_flush_start_offset_checkpoint_interval_ms: i32,
    pub num_recovery_threads_per_data_dir: i32,
}

impl KafkaConfig {
    /// `get_kafka_config` Reads the kafka config.
    pub fn get_kafka_config(filename: &str) -> Result<Self, KafkaConfigError> {
        debug!("read_config_from: Reading {}", filename);
        let mut config_file_content = File::open(&filename)?;
        let input_config = java_properties::read(BufReader::new(&mut config_file_content))?;
        KafkaConfigProperties::from_properties_hashmap(input_config)?.build()
    }

    pub fn validate_values(self) -> Result<Self, KafkaConfigError> {
        if self.broker_id_generation_enable {
            if self.broker_id < -1 || self.broker_id > self.reserved_broker_max_id {
                return Err(KafkaConfigError::InvalidValue(format!(
                    "{}: '{}' must be equal or greater than -1 and not greater than {}",
                    BROKER_ID_PROP, self.broker_id, RESERVED_BROKER_MAX_ID_PROP
                )));
            }
        } else if self.broker_id < 0 {
            return Err(KafkaConfigError::InvalidValue(format!(
                "{}: '{}' must be equal or greater than 0",
                BROKER_ID_PROP, self.broker_id
            )));
        }
        Ok(self)
    }
}

impl Default for KafkaConfig {
    fn default() -> Self {
        // Somehow this should only be allowed for testing...
        let mut config_properties = KafkaConfigProperties::default();
        let broker_id_generation_enable =
            config_properties.broker_id_generation_enable.build().unwrap();
        let reserved_broker_max_id = config_properties.reserved_broker_max_id.build().unwrap();
        let broker_id = config_properties.broker_id.build().unwrap();
        let message_max_bytes = config_properties.message_max_bytes.build().unwrap();
        let port = config_properties.port.build().unwrap();
        let host_name = config_properties.host_name.build().unwrap();
        let listeners = config_properties.listeners.build().unwrap();
        let advertised_host_name = config_properties.advertised_host_name.build().unwrap();
        let advertised_port = config_properties.advertised_port.build().unwrap();
        let zk_session_timeout_ms = config_properties.zk_session_timeout_ms.build().unwrap();
        config_properties
            .zk_connection_timeout_ms
            .resolve(&config_properties.zk_session_timeout_ms);
        let zk_connection_timeout_ms = config_properties.zk_connection_timeout_ms.build().unwrap();
        let log_dirs = config_properties.resolve_log_dirs().unwrap();
        let log_segment_bytes = config_properties.log_segment_bytes.build().unwrap();
        let zk_connect = String::from("UNSET");
        let zk_max_in_flight_requests =
            config_properties.zk_max_in_flight_requests.build().unwrap();
        let consumer_quota_bytes_per_second_default =
            config_properties.consumer_quota_bytes_per_second_default.build().unwrap();
        let quota_window_size_seconds =
            config_properties.quota_window_size_seconds.build().unwrap();
        let advertised_listeners = config_properties.resolve_advertised_listeners().unwrap();
        let producer_quota_bytes_per_second_default =
            config_properties.producer_quota_bytes_per_second_default.build().unwrap();
        let log_roll_time_millis = config_properties.resolve_log_roll_time_millis().unwrap();
        let log_roll_time_hours = config_properties.log_roll_time_hours.build().unwrap();
        let log_roll_time_jitter_millis =
            config_properties.resolve_log_roll_time_jitter_millis().unwrap();
        let log_roll_time_jitter_hours =
            config_properties.log_roll_time_jitter_hours.build().unwrap();
        let log_retention_time_millis =
            config_properties.resolve_log_retention_time_millis().unwrap();
        let log_retention_time_minutes =
            config_properties.log_retention_time_minutes.build().unwrap();
        let log_retention_time_hours = config_properties.log_retention_time_hours.build().unwrap();
        let log_cleaner_threads = config_properties.log_cleaner_threads.build().unwrap();
        let log_cleaner_dedupe_buffer_size =
            config_properties.log_cleaner_dedupe_buffer_size.build().unwrap();
        let log_flush_scheduler_interval_ms =
            config_properties.log_flush_scheduler_interval_ms.build().unwrap();
        let log_flush_interval_ms = config_properties.log_flush_interval_ms.build().unwrap();
        let log_flush_scheduler_interval_ms =
            config_properties.log_flush_scheduler_interval_ms.build().unwrap();
        let log_flush_start_offset_checkpoint_interval_ms =
            config_properties.log_flush_start_offset_checkpoint_interval_ms.build().unwrap();
        let num_recovery_threads_per_data_dir =
            config_properties.num_recovery_threads_per_data_dir.build().unwrap();
        Self {
            broker_id_generation_enable,
            reserved_broker_max_id,
            broker_id,
            message_max_bytes,
            port,
            host_name,
            listeners,
            advertised_host_name,
            advertised_port,
            advertised_listeners,
            zk_connect,
            zk_session_timeout_ms,
            zk_connection_timeout_ms,
            log_dirs,
            log_segment_bytes,
            zk_max_in_flight_requests,
            consumer_quota_bytes_per_second_default,
            producer_quota_bytes_per_second_default,
            quota_window_size_seconds,
            log_roll_time_millis,
            log_roll_time_hours,
            log_roll_time_jitter_millis,
            log_roll_time_jitter_hours,
            log_retention_time_millis,
            log_retention_time_minutes,
            log_retention_time_hours,
            log_cleaner_threads,
            log_cleaner_dedupe_buffer_size,
            log_flush_scheduler_interval_ms,
            log_flush_interval_ms,
            log_flush_scheduler_interval_ms,
            log_flush_start_offset_checkpoint_interval_ms,
            num_recovery_threads_per_data_dir,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_gets_config_from_hashmap() {
        let empty_config: HashMap<String, String> = HashMap::new();
        let config_error = KafkaConfigProperties::from_properties_hashmap(empty_config)
            .unwrap()
            .build()
            .unwrap_err();
        assert_eq!(config_error, KafkaConfigError::MissingKey(ZOOKEEPER_CONNECT_PROP.to_string()));
        let mut unknown_key_config: HashMap<String, String> = HashMap::new();
        unknown_key_config.insert(String::from("not.a.known.key"), String::from("127.0.0.1:2181"));
        assert_eq!(
            KafkaConfigProperties::from_properties_hashmap(unknown_key_config),
            Err(KafkaConfigError::UnknownKey(String::from("not.a.known.key")))
        );
        let mut missing_key_config: HashMap<String, String> = HashMap::new();
        missing_key_config
            .insert(String::from("zookeeper.session.timeout.ms"), String::from("1000"));
        let config_error = KafkaConfigProperties::from_properties_hashmap(missing_key_config)
            .unwrap()
            .build()
            .unwrap_err();
        assert_eq!(config_error, KafkaConfigError::MissingKey(ZOOKEEPER_CONNECT_PROP.to_string()));
        let mut full_config: HashMap<String, String> = HashMap::new();
        full_config.insert(String::from(ZOOKEEPER_CONNECT_PROP), String::from("127.0.0.1:2181"));
        full_config.insert(String::from(ZOOKEEPER_SESSION_TIMEOUT_PROP), String::from("1000"));
        full_config.insert(String::from(ZOOKEEPER_CONNECTION_TIMEOUT_PROP), String::from("1000"));
        full_config.insert(String::from(LOG_DIRS_PROP), String::from("/some-dir/logs"));
        assert!(KafkaConfigProperties::from_properties_hashmap(full_config).is_ok());
        let mut multiple_log_dir_properties: HashMap<String, String> = HashMap::new();
        multiple_log_dir_properties
            .insert(String::from(ZOOKEEPER_CONNECT_PROP), String::from("127.0.0.1:2181"));
        multiple_log_dir_properties
            .insert(String::from(LOG_DIR_PROP), String::from("/single/log/dir"));
        multiple_log_dir_properties
            .insert(String::from(LOG_DIRS_PROP), String::from("/some-1/logs, /some-2-logs"));
        let config_builder =
            KafkaConfigProperties::from_properties_hashmap(multiple_log_dir_properties);
        assert!(config_builder.is_ok());
        let mut config_builder = config_builder.unwrap();
        let config = config_builder.build().unwrap();
        assert_eq!(config.log_dirs, vec![
            String::from("/some-1/logs"),
            String::from("/some-2-logs")
        ]);
        let mut invalid_broker_id: HashMap<String, String> = HashMap::new();
        invalid_broker_id
            .insert(String::from(ZOOKEEPER_CONNECT_PROP), String::from("127.0.0.1:2181"));
        invalid_broker_id.insert(String::from(BROKER_ID_PROP), String::from("-2"));
        let config_error = KafkaConfigProperties::from_properties_hashmap(invalid_broker_id)
            .unwrap()
            .build()
            .unwrap_err();
        assert_eq!(
            config_error,
            KafkaConfigError::InvalidValue(format!(
                "{}: '-2' must be equal or greater than -1 and not greater than {}",
                BROKER_ID_PROP, RESERVED_BROKER_MAX_ID_PROP
            ))
        );
    }
}
