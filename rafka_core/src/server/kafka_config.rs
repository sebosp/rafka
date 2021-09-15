/// Core Kafka Config
/// core/src/main/scala/kafka/server/KafkaConfig.scala
/// Changes:
/// - No SSL, no SASL
/// - RAFKA NOTE: Using serde_json doesn't work very well because for example
/// ADVERTISED_LISTENERS are variable keys that need to be decomposed into actual listeners
use crate::common::config_def::{ConfigDef, ConfigDefImportance};
use crate::server::client_quota_manager;
use fs_err::File;
use std::collections::HashMap;
use std::io::{self, BufReader};
use std::num;
use thiserror::Error;
use tracing::debug;

// Log section
pub const LOG_DIRS_PROP: &str = "log.dirs";
pub const LOG_DIR_PROP: &str = "log.dir";
pub const LOG_SEGMENT_BYTES_PROP: &str = "log.segment.bytes";
pub const LOG_ROLL_TIME_MILLIS_PROP: &str = "log.roll.ms"; // RAFKA TODO: Missing associated ConfigDef
pub const LOG_ROLL_TIME_HOURS_PROP: &str = "log.roll.hours"; // RAFKA TODO: Missing associated ConfigDef
pub const LOG_ROLL_TIME_JITTER_MILLIS_PROP: &str = "log.roll.jitter.ms"; // RAFKA TODO: Missing associated ConfigDef
pub const LOG_ROLL_TIME_JITTER_HOURS_PROP: &str = "log.roll.jitter.hours"; // RAFKA TODO: Missing associated ConfigDef
pub const LOG_RETENTION_TIME_MILLIS_PROP: &str = "log.retention.ms"; // RAFKA TODO: Missing associated ConfigDef
pub const LOG_RETENTION_TIME_MINUTES_PROP: &str = "log.retention.minutes"; // RAFKA TODO: Missing associated ConfigDef
pub const LOG_RETENTION_TIME_HOURS_PROP: &str = "log.retention.hours"; // RAFKA TODO: Missing associated ConfigDef
pub const LOG_FLUSH_SCHEDULER_INTERVAL_MS_PROP: &str = "log.flush.scheduler.interval.ms"; // RAFKA TODO: Missing associated ConfigDef
pub const LOG_FLUSH_INTERVAL_MS_PROP: &str = "log.flush.interval.ms"; // RAFKA TODO: Missing associated ConfigDef
pub const NUM_RECOVERY_THREADS_PER_DATA_DIR_PROP: &str = "num.recovery.threads.per.data.dir";

// General section
pub const BROKER_ID_GENERATION_ENABLED_PROP: &str = "broker.id.generation.enable";
pub const RESERVED_BROKER_MAX_ID_PROP: &str = "reserved.broker.max.id";
pub const BROKER_ID_PROP: &str = "broker.id";
// Socket server section
pub const LISTENERS_PROP: &str = "listeners";
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
        }
    }
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KafkaConfigProperties {
    #[serde(rename = "zookeeper.connect")]
    zk_connect: ConfigDef<String>,
    #[serde(rename = "zookeeper.session.timeout.ms")]
    zk_session_timeout_ms: ConfigDef<u32>,
    #[serde(rename = "zookeeper.connection.timeout.ms")]
    zk_connection_timeout_ms: ConfigDef<u32>,
    // Singular log.dir
    #[serde(rename = "log.dir")]
    log_dir: ConfigDef<String>,
    // Multiple comma separated log.dirs, may include spaces after the comma (will be trimmed)
    #[serde(rename = "log.dirs")]
    log_dirs: ConfigDef<String>,
    #[serde(rename = "broker.id.generation.enable")]
    broker_id_generation_enable: ConfigDef<bool>,
    #[serde(rename = "reserved.broker.max.id")]
    reserved_broker_max_id: ConfigDef<i32>,
    #[serde(rename = "broker.id")]
    broker_id: ConfigDef<i32>,
    #[serde(rename = "zookeeper.max.in.flight.requests")]
    zk_max_in_flight_requests: ConfigDef<u32>,
    #[serde(rename = "advertised.listeners")]
    advertised_listeners: ConfigDef<String>,
    #[serde(rename = "quota.consumer.default")]
    consumer_quota_bytes_per_second_default: ConfigDef<i64>,
    #[serde(rename = "quota.producer.default")]
    producer_quota_bytes_per_second_default: ConfigDef<i64>,
    #[serde(rename = "quota.window.size.seconds")]
    quota_window_size_seconds: ConfigDef<i32>,
    #[serde(rename = "log.roll.ms")]
    log_roll_time_millis_prop: ConfigDef<i64>,
    #[serde(rename = "log.roll.hours")]
    log_roll_time_hours_prop: ConfigDef<i32>,
    #[serde(rename = "log.roll.jitter.ms")]
    log_roll_time_jitter_millis_prop: ConfigDef<i64>,
    #[serde(rename = "log.roll.jitter.hours")]
    log_roll_time_jitter_hours_prop: ConfigDef<i32>,
    #[serde(rename = "log.retention.ms")]
    log_retention_time_millis_prop: ConfigDef<i64>,
    #[serde(rename = "log.retention.minutes")]
    log_retention_time_minutes_prop: ConfigDef<i32>,
    #[serde(rename = "log.retention.hours")]
    log_retention_time_hours_prop: ConfigDef<i32>,
    #[serde(rename = "log.flush.scheduler.interval.ms")]
    log_flush_scheduler_interval_ms_prop: ConfigDef<i64>,
    #[serde(rename = "log.flush.interval.ms")]
    log_flush_interval_ms_prop: ConfigDef<i64>,
    #[serde(rename = "num.recovery.threads.per.data.dir")]
    num_recovery_threads_per_data_dir_prop: ConfigDef<i32>,
}

impl Default for KafkaConfigProperties {
    fn default() -> Self {
        Self {
            zk_connect: ConfigDef::default()
                .with_key(ZOOKEEPER_CONNECT_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(String::from(r#"
                    Specifies the ZooKeeper connection string in the form <code>hostname:port</code> where host and port are the
                    host and port of a ZooKeeper server. To allow connecting through other ZooKeeper nodes when that ZooKeeper machine is
                    down you can also specify multiple hosts in the form <code>hostname1:port1,hostname2:port2,hostname3:port3</code>.
                    The server can also have a ZooKeeper chroot path as part of its ZooKeeper connection string which puts its data under some path in the global ZooKeeper namespace.
                    For example to give a chroot path of <code>/chroot/path</code> you would give the connection string as <code>hostname1:port1,hostname2:port2,hostname3:port3/chroot/path</code>.
                    "#
                )),
            zk_session_timeout_ms: ConfigDef::default()
                .with_key(ZOOKEEPER_SESSION_TIMEOUT_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(String::from("Zookeeper session timeout"))
                .with_default(String::from("18000")),
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
                .with_default(String::from("10")),
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
            broker_id_generation_enable: ConfigDef::default()
                .with_key(BROKER_ID_GENERATION_ENABLED_PROP)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(
                    format!("Enable automatic broker id generation on the server. When enabled the value \
                     configured for {} should be reviewed.", RESERVED_BROKER_MAX_ID_PROP)
                )
                .with_default(String::from("true")),
            reserved_broker_max_id: ConfigDef::default()
                .with_key(RESERVED_BROKER_MAX_ID_PROP)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(format!("Max number that can be used for a {}", BROKER_ID_PROP))
                .with_default(String::from("1000")),
            broker_id: ConfigDef::default()
                .with_key(BROKER_ID_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(
                    format!("The broker id for this server. If unset, a unique broker id will be generated. \
                     To avoid conflicts between zookeeper generated broker id's and user configured \
                     broker id's, generated broker ids start from {} + 1.", RESERVED_BROKER_MAX_ID_PROP),
                )
                .with_default(String::from("-1")),
            advertised_listeners: ConfigDef::default()
                .with_key(ADVERTISED_LISTENERS_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(String::from(
                    "Listeners to publish to ZooKeeper for clients to use, if different than the `listeners` config property.\
                    In IaaS environments, this may need to be different from the interface to which the broker binds. \
                    If this is not set, the value for `listeners` will be used. \
                    Unlike `listeners` it is not valid to advertise the 0.0.0.0 meta-address "
                ))
                .with_default(String::from("-1")),
            consumer_quota_bytes_per_second_default: ConfigDef::default()
                .with_key(CONSUMER_QUOTA_BYTES_PER_SECOND_DEFAULT_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(String::from(
                    "DEPRECATED: Used only when dynamic default quotas are not configured for <user, <client-id> or <user, client-id> in Zookeeper. \
                    Any consumer distinguished by clientId/consumer group will get throttled if it fetches more bytes than this value per-second"
                ))
                .with_default(client_quota_manager::QUOTA_BYTES_PER_SECOND_DEFAULT.to_string()),
            producer_quota_bytes_per_second_default: ConfigDef::default()
                .with_key(PRODUCER_QUOTA_BYTES_PER_SECOND_DEFAULT_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(String::from(
                    "DEPRECATED: Used only when dynamic default quotas are not configured for <user, <client-id> or <user, client-id> in Zookeeper. \
                    Any consumer distinguished by clientId/consumer group will get throttled if it fetches more bytes than this value per-second"
                ))
                .with_default(client_quota_manager::QUOTA_BYTES_PER_SECOND_DEFAULT.to_string()),
            quota_window_size_seconds: ConfigDef::default()
                .with_key(QUOTA_WINDOW_SIZE_SECONDS_PROP)
                .with_importance(ConfigDefImportance::Low)
                .with_doc(String::from(
                    "The time span of each sample for client quotas"
                ))
                .with_default(client_quota_manager::QUOTA_WINDOW_SIZE_SECONDS_DEFAULT.to_string()),
            log_roll_time_millis_prop: ConfigDef::default()
                .with_key(LOG_ROLL_TIME_MILLIS_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(format!(
                        "The maximum time before a new log segment is rolled out (in milliseconds). If not set, the value in {} is used", LOG_ROLL_TIME_HOURS_PROP
                )),
            log_roll_time_hours_prop: ConfigDef::default()
                .with_key(LOG_ROLL_TIME_HOURS_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(format!(
                        "The maximum time before a new log segment is rolled out (in hours), secondary to {} property", LOG_ROLL_TIME_MILLIS_PROP
                ))
                .with_default(String::from("168")), // 24 * 7 // RAFKA TODO: Make this lazy_static!
            log_roll_time_jitter_millis_prop: ConfigDef::default()
                .with_key(LOG_ROLL_TIME_JITTER_MILLIS_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(format!(
                        "The maximum jitter to subtract from logRollTimeMillis (in milliseconds). If not set, the value in {} is used", LOG_ROLL_TIME_JITTER_HOURS_PROP
                )),
            log_roll_time_jitter_hours_prop: ConfigDef::default()
                .with_key(LOG_ROLL_TIME_JITTER_HOURS_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(format!(
                         "The maximum jitter to subtract from logRollTimeMillis (in hours), secondary to {} property", LOG_ROLL_TIME_JITTER_MILLIS_PROP
                )),
            log_retention_time_millis_prop: ConfigDef::default()
                .with_key(LOG_RETENTION_TIME_MILLIS_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(format!(
                        "The number of milliseconds to keep a log file before deleting it (in milliseconds), If not set, the value in {} is used. If set to -1, no time limit is applied.", LOG_RETENTION_TIME_MINUTES_PROP
                )),
            log_retention_time_minutes_prop: ConfigDef::default()
                .with_key(LOG_RETENTION_TIME_MINUTES_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(format!(
                        "The number of minutes to keep a log file before deleting it (in minutes), secondary to {} property. If not set, the value in {} is used", LOG_RETENTION_TIME_MILLIS_PROP, LOG_RETENTION_TIME_HOURS_PROP
                )),
            log_retention_time_hours_prop: ConfigDef::default()
                .with_key(LOG_RETENTION_TIME_HOURS_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(format!(
                    "The number of hours to keep a log file before deleting it (in hours), tertiary to {} property", LOG_RETENTION_TIME_MILLIS_PROP
                    ))
                .with_default(String::from("168")), // 24 * 7 // RAFKA TODO: Make this lazy_static!
            log_flush_scheduler_interval_ms_prop: ConfigDef::default()
                .with_key(LOG_FLUSH_SCHEDULER_INTERVAL_MS_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(String::from("The frequency in ms that the log flusher checks whether any log needs to be flushed to disk"))
                .with_default(u64::MAX.to_string()),
            log_flush_interval_ms_prop: ConfigDef::default()
                .with_key(LOG_FLUSH_INTERVAL_MS_PROP)
                .with_doc(format!(
                        "The maximum time in ms that a message in any topic is kept in memory before flushed to disk. If not set, the value in {} is used", LOG_FLUSH_SCHEDULER_INTERVAL_MS_PROP
                ))
                .with_default(u64::MAX.to_string()),
            num_recovery_threads_per_data_dir_prop: ConfigDef::default()
                .with_key(NUM_RECOVERY_THREADS_PER_DATA_DIR_PROP)
                .with_doc(String::from(
                        "The number of threads per data directory to be used for log recovery at startup and flushing at shutdown"
                ))
                .with_default(String::from("1")),
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
        match property_name {
            ZOOKEEPER_CONNECT_PROP => self.zk_connect.try_set_parsed_value(property_value)?,
            ZOOKEEPER_SESSION_TIMEOUT_PROP => {
                self.zk_session_timeout_ms.try_set_parsed_value(property_value)?
            },
            ZOOKEEPER_CONNECTION_TIMEOUT_PROP => {
                self.zk_connection_timeout_ms.try_set_parsed_value(property_value)?
            },
            LOG_DIR_PROP => self.log_dir.try_set_parsed_value(property_value)?,
            LOG_DIRS_PROP => self.log_dirs.try_set_parsed_value(property_value)?,
            BROKER_ID_GENERATION_ENABLED_PROP => {
                self.broker_id_generation_enable.try_set_parsed_value(property_value)?
            },
            RESERVED_BROKER_MAX_ID_PROP => {
                self.reserved_broker_max_id.try_set_parsed_value(property_value)?
            },
            BROKER_ID_PROP => self.broker_id.try_set_parsed_value(property_value)?,
            CONSUMER_QUOTA_BYTES_PER_SECOND_DEFAULT_PROP => {
                self.consumer_quota_bytes_per_second_default.try_set_parsed_value(property_value)?
            },
            PRODUCER_QUOTA_BYTES_PER_SECOND_DEFAULT_PROP => {
                self.producer_quota_bytes_per_second_default.try_set_parsed_value(property_value)?
            },
            QUOTA_WINDOW_SIZE_SECONDS_PROP => {
                self.quota_window_size_seconds.try_set_parsed_value(property_value)?
            },
            ADVERTISED_LISTENERS_PROP => {
                self.advertised_listeners.try_set_parsed_value(property_value)?
            },
            _ => return Err(KafkaConfigError::UnknownKey(property_name.to_string())),
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

    /// `resolve_zk_session_timeout_ms` Returns the session timeout when connecting to zookeeper
    fn resolve_zk_session_timeout_ms(&mut self) -> Result<u32, KafkaConfigError> {
        // NOTE: zk_session_timeout_ms has a default, so it is never None
        match self.zk_session_timeout_ms.get_value() {
            Some(val) => Ok(*val),
            None => {
                // TODO: Find a way to make it not-compile of the value is None and the default is
                // not None
                panic!(
                    "zk_session_timeout_ms has a default but found its value to be None, bug in \
                     parsing defaults"
                );
            },
        }
    }

    /// `resolve_zk_connection_timeout_ms` Satisties REQ-01, if zk_connection_timeout_ms is unset
    /// the value of zk_connection_timeout_ms will be used.
    fn resolve_zk_connection_timeout_ms(&mut self) -> Result<u32, KafkaConfigError> {
        if let Some(val) = self.zk_connection_timeout_ms.get_value() {
            Ok(*val)
        } else {
            debug!(
                "Unspecified property {} will use {:?} from {}",
                ZOOKEEPER_CONNECTION_TIMEOUT_PROP,
                self.zk_session_timeout_ms.get_value(),
                ZOOKEEPER_SESSION_TIMEOUT_PROP
            );
            // Fallback to the zookeeper.connection.timeout.ms value
            let zk_session_timeout_ms = self.resolve_zk_session_timeout_ms()?;
            self.zk_connection_timeout_ms.set_value(zk_session_timeout_ms);
            Ok(*self.zk_session_timeout_ms.get_value().unwrap())
        }
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

    fn resolve_zk_connect(&mut self) -> Result<String, KafkaConfigError> {
        if let Some(zk_connect) = &self.zk_connect.get_value() {
            Ok(zk_connect.to_string())
        } else {
            Err(KafkaConfigError::MissingKey(ZOOKEEPER_CONNECT_PROP.to_string()))
        }
    }

    fn resolve_reserved_broker_max_id(&mut self) -> Result<i32, KafkaConfigError> {
        if let Some(reserved_broker_max_id) = self.reserved_broker_max_id.get_value() {
            if *reserved_broker_max_id < 0 {
                Err(KafkaConfigError::InvalidValue(format!(
                    "{}: '{}' should be at least 0",
                    RESERVED_BROKER_MAX_ID_PROP, *reserved_broker_max_id
                )))
            } else {
                Ok(*reserved_broker_max_id)
            }
        } else {
            panic!(
                "reserved_broker_max_id has a default but found its value to be None, bug in \
                 parsing defaults"
            );
        }
    }

    fn resolve_broker_id(&mut self) -> Result<i32, KafkaConfigError> {
        if let Some(broker_id) = self.broker_id.get_value() {
            Ok(*broker_id)
        } else {
            panic!(
                "broker_id has a default but found its value to be None, bug in parsing defaults"
            );
        }
    }

    fn resolve_broker_id_generation_enable(&mut self) -> Result<bool, KafkaConfigError> {
        if let Some(val) = self.broker_id_generation_enable.get_value() {
            Ok(*val)
        } else {
            Err(KafkaConfigError::MissingKey(BROKER_ID_GENERATION_ENABLED_PROP.to_string()))
        }
    }

    fn resolve_zk_max_in_flight_requests(&mut self) -> Result<u32, KafkaConfigError> {
        // at least 0
        if let Some(zk_max_in_flight_requests) = self.zk_max_in_flight_requests.get_value() {
            if *zk_max_in_flight_requests < 1 {
                // RAFKA TODO: This doesn't make much sense if it's u32...
                Err(KafkaConfigError::InvalidValue(format!(
                    "{}: '{}' should be at least 0",
                    ZOOKEEPER_MAX_IN_FLIGHT_REQUESTS, *zk_max_in_flight_requests
                )))
            } else {
                Ok(*zk_max_in_flight_requests)
            }
        } else {
            panic!(
                "zk_max_in_flight_requests has a default but found its value to be None, bug in \
                 parsing defaults"
            );
        }
    }

    fn resolve_consumer_quota_bytes_per_second_default(&mut self) -> Result<i64, KafkaConfigError> {
        if let Some(val) = self.consumer_quota_bytes_per_second_default.get_value() {
            if *val < 1 {
                Err(KafkaConfigError::InvalidValue(format!(
                    "{}: '{}' should be at least 1",
                    CONSUMER_QUOTA_BYTES_PER_SECOND_DEFAULT_PROP, *val
                )))
            } else {
                Ok(*val)
            }
        } else {
            panic!(
                "consumer_quota_bytes_per_second_default has a default but found its value to be \
                 None, bug in parsing defaults"
            );
        }
    }

    fn resolve_quota_window_size_seconds(&mut self) -> Result<i32, KafkaConfigError> {
        if let Some(val) = self.quota_window_size_seconds.get_value() {
            if *val < 1 {
                Err(KafkaConfigError::InvalidValue(format!(
                    "{}: '{}' should be at least 1",
                    QUOTA_WINDOW_SIZE_SECONDS_PROP, *val
                )))
            } else {
                Ok(*val)
            }
        } else {
            panic!(
                "quota_window_size_seconds has a default but found its value to be None, bug in \
                 parsing defaults"
            );
        }
    }

    /// `build` validates and resolves dependant properties from a KafkaConfigProperties into a
    /// KafkaConfig
    pub fn build(&mut self) -> Result<KafkaConfig, KafkaConfigError> {
        let zk_session_timeout_ms = self.resolve_zk_session_timeout_ms()?;
        let zk_connection_timeout_ms = self.resolve_zk_connection_timeout_ms()?;
        let log_dirs = self.resolve_log_dirs()?;
        let reserved_broker_max_id = self.resolve_reserved_broker_max_id()?;
        let broker_id = self.resolve_broker_id()?;
        let broker_id_generation_enable = self.resolve_broker_id_generation_enable()?;
        let zk_connect = self.resolve_zk_connect()?;
        let zk_max_in_flight_requests = self.resolve_zk_max_in_flight_requests()?;
        let consumer_quota_bytes_per_second_default =
            self.resolve_consumer_quota_bytes_per_second_default()?;
        let quota_window_size_seconds = self.resolve_quota_window_size_seconds()?;
        let num_recovery_threads_per_data_dir = self.resolve_num_recovery_threads_per_data_dir()?;
        let kafka_config = KafkaConfig {
            zk_connect,
            zk_session_timeout_ms,
            zk_connection_timeout_ms,
            zk_max_in_flight_requests,
            log_dirs,
            reserved_broker_max_id,
            broker_id_generation_enable,
            broker_id,
            consumer_quota_bytes_per_second_default,
            quota_window_size_seconds,
            num_recovery_threads_per_data_dir,
        };
        kafka_config.validate_values()
    }
}
#[derive(Debug, PartialEq, Clone)]
pub struct KafkaConfig {
    pub zk_connect: String,
    pub zk_session_timeout_ms: u32,
    // pub zk_sync_time_ms: u32,
    pub zk_connection_timeout_ms: u32,
    pub zk_max_in_flight_requests: u32,
    pub log_dirs: Vec<String>,
    pub reserved_broker_max_id: i32,
    pub broker_id_generation_enable: bool,
    pub broker_id: i32,
    pub consumer_quota_bytes_per_second_default: i64,
    pub quota_window_size_seconds: i32,
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
        let zk_session_timeout_ms = config_properties.resolve_zk_session_timeout_ms().unwrap();
        let zk_connection_timeout_ms =
            config_properties.resolve_zk_connection_timeout_ms().unwrap();
        let log_dirs = config_properties.resolve_log_dirs().unwrap();
        let reserved_broker_max_id = config_properties.resolve_reserved_broker_max_id().unwrap();
        let broker_id = config_properties.resolve_broker_id().unwrap();
        let broker_id_generation_enable =
            config_properties.resolve_broker_id_generation_enable().unwrap();
        let zk_connect = String::from("UNSET");
        let zk_max_in_flight_requests =
            config_properties.resolve_zk_max_in_flight_requests().unwrap();
        let consumer_quota_bytes_per_second_default =
            config_properties.resolve_consumer_quota_bytes_per_second_default().unwrap();
        let quota_window_size_seconds =
            config_properties.resolve_quota_window_size_seconds().unwrap();
        Self {
            zk_connect,
            zk_session_timeout_ms,
            zk_connection_timeout_ms,
            zk_max_in_flight_requests,
            log_dirs,
            reserved_broker_max_id,
            broker_id_generation_enable,
            broker_id,
            consumer_quota_bytes_per_second_default,
            quota_window_size_seconds,
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
