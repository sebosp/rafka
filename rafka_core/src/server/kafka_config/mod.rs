//! Core Kafka Config
//! core/src/main/scala/kafka/server/KafkaConfig.scala
//! Changes:
//! - No SSL, no SASL
//! - RAFKA NOTE: ADVERTISED_LISTENERS are variable keys that need to be decomposed into actual
//!   listeners Thus using serde_json need to be tweaked properly
//! TODO:
//! - The amount of properties is too big to handle sanely, adding, parsing, checking, too error
//! prone, perhaps splitting them by cathegories is the next step, that would allow us to split
//! into smaller files and specialize them there.

pub mod general;
pub mod log;
pub mod quota;
pub mod socket_server;
pub mod transaction_management;

use self::general::{GeneralConfig, GeneralConfigKey, GeneralConfigProperties};
use self::log::{LogConfig, LogConfigKey, LogConfigProperties};
use self::quota::{QuotaConfig, QuotaConfigKey, QuotaConfigProperties};
use self::transaction_management::{
    TransactionConfig, TransactionConfigKey, TransactionConfigProperties,
};
use crate::common::config_def::{ConfigDef, ConfigDefImportance};
use enum_iterator::IntoEnumIterator;
use fs_err::File;
use std::collections::HashMap;
use std::fmt::Display;
use std::io::{self, BufReader};
use std::num;
use std::str::FromStr;
use thiserror::Error;
use tracing::debug;

use self::socket_server::{SocketConfig, SocketConfigKey, SocketConfigProperties};

// Zookeeper section
pub const ZOOKEEPER_CONNECT_PROP: &str = "zookeeper.connect";
pub const ZOOKEEPER_SESSION_TIMEOUT_PROP: &str = "zookeeper.session.timeout.ms";
pub const ZOOKEEPER_CONNECTION_TIMEOUT_PROP: &str = "zookeeper.connection.timeout.ms";
pub const ZOOKEEPER_MAX_IN_FLIGHT_REQUESTS: &str = "zookeeper.max.in.flight.requests";

// Transaction management section
pub const TRANSACTIONAL_ID_EXPIRATION_MS_PROP: &str = "transactional.id.expiration.ms";

// A Helper Enum to aid with the miriad of properties that could be forgotten to be matched.
#[derive(Debug)]
pub enum KafkaConfigKey {
    General(GeneralConfigKey),
    Socket(SocketConfigKey),
    ZkConnect,
    ZkSessionTimeoutMs,
    ZkConnectionTimeoutMs,
    // ZkMaxInFlightRequests,
    Log(LogConfigKey),
    Transaction(TransactionConfigKey),
    Quota(QuotaConfigKey),
}

// impl fmt::Display for *ConfigKey {
// vim from enum to match: /^    \(.*\),/\= "Self::" . submatch(1) . "=> write!(f, \"{}\","
// . Uppercase(submatch(1)) . "_PROP),"/
//}
impl FromStr for KafkaConfigKey {
    type Err = KafkaConfigError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if let Ok(val) = GeneralConfigKey::from_str(input) {
            return Ok(Self::General(val));
        }
        if let Ok(val) = LogConfigKey::from_str(input) {
            return Ok(Self::Log(val));
        }
        if let Ok(val) = QuotaConfigKey::from_str(input) {
            return Ok(Self::Quota(val));
        }
        if let Ok(val) = SocketConfigKey::from_str(input) {
            return Ok(Self::Socket(val));
        }
        if let Ok(val) = TransactionConfigKey::from_str(input) {
            return Ok(Self::Transaction(val));
        }
        // vim from enum to match: /^    \(.*\),/\= "         " . Uppercase(submatch(1)) . "_PROP =>
        // Ok(Self::" .submatch(1) . "),"/
        match input {
            ZOOKEEPER_CONNECT_PROP => Ok(Self::ZkConnect),
            ZOOKEEPER_SESSION_TIMEOUT_PROP => Ok(Self::ZkSessionTimeoutMs),
            ZOOKEEPER_CONNECTION_TIMEOUT_PROP => Ok(Self::ZkConnectionTimeoutMs),
            // ZK_MAX_IN_FLIGHT_REQUESTS_PROP => Ok(Self::ZkMaxInFlightRequests),
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
    #[error("ParseFloat error: {0}")]
    ParseFloat(#[from] num::ParseFloatError),
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
            Self::ParseFloat(lhs) => matches!(rhs, Self::ParseFloat(rhs) if lhs == rhs),
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

/// A set of functions that the different configuration sets must implement, including building,
/// parsing, returning keys, etc.
pub trait ConfigSet {
    type ConfigKey;
    type ConfigType;
    /// `try_from_config_property` transforms a string value from the config into our actual types
    fn try_set_property(
        &mut self,
        property_name: &str,
        property_value: &str,
    ) -> Result<(), KafkaConfigError>;
    /// `build` validates and resolves dependant properties from a KafkaConfigProperties into a
    /// KafkaConfig. NOTE: This doesn't consume self, as a ConfigKey may be re-updated on-the-fly
    /// without the need to restart, for example via zookeeper
    fn build(&mut self) -> Result<Self::ConfigType, KafkaConfigError>;
    /// `config_names` returns a list of config keys used
    fn config_names() -> Vec<String>
    where
        Self::ConfigKey: IntoEnumIterator + Display,
    {
        Self::ConfigKey::into_enum_iter().map(|val| val.to_string()).collect()
    }
}

#[derive(Debug)]
pub struct KafkaConfigProperties {
    general: GeneralConfigProperties,
    socket: SocketConfigProperties,
    zk_connect: ConfigDef<String>,
    zk_session_timeout_ms: ConfigDef<u32>,
    zk_connection_timeout_ms: ConfigDef<u32>,
    zk_max_in_flight_requests: ConfigDef<u32>,
    log: LogConfigProperties,
    transaction: TransactionConfigProperties,
    quota: QuotaConfigProperties,
}

impl Default for KafkaConfigProperties {
    fn default() -> Self {
        Self {
            general: GeneralConfigProperties::default(),
            socket: SocketConfigProperties::default(),
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
            log: LogConfigProperties::default(),
            transaction: TransactionConfigProperties::default(),
            quota: QuotaConfigProperties::default(),
        }
    }
}

impl ConfigSet for KafkaConfigProperties {
    type ConfigKey = KafkaConfigKey;
    type ConfigType = KafkaConfig;

    fn try_set_property(
        &mut self,
        property_name: &str,
        property_value: &str,
    ) -> Result<(), KafkaConfigError> {
        let kafka_config_key = Self::ConfigKey::from_str(property_name)?;
        // vim from enum to match: s/^    \(.*\),/\= "            Self::ConfigKey::" . submatch(1) .
        // " => self." . Snakecase(submatch(1)). ".try_set_parsed_value(property_value)?,"/
        match kafka_config_key {
            KafkaConfigKey::General(val) => {
                self.general.try_set_property(property_name, property_value)?
            },
            KafkaConfigKey::Socket(val) => {
                self.socket.try_set_property(property_name, property_value)?
            },
            KafkaConfigKey::ZkConnect => self.zk_connect.try_set_parsed_value(property_value)?,
            KafkaConfigKey::ZkSessionTimeoutMs => {
                self.zk_session_timeout_ms.try_set_parsed_value(property_value)?
            },
            KafkaConfigKey::ZkConnectionTimeoutMs => {
                self.zk_connection_timeout_ms.try_set_parsed_value(property_value)?
            },
            KafkaConfigKey::Log(val) => self.log.try_set_property(property_name, property_value)?,
            KafkaConfigKey::Transaction(val) => {
                self.transaction.try_set_property(property_name, property_value)?
            },
            KafkaConfigKey::Quota(val) => {
                self.quota.try_set_property(property_name, property_value)?
            },
        };
        Ok(())
    }

    /// `config_names` returns a list of config keys used by KafkaConfigProperties
    fn config_names() -> Vec<String> {
        let res = vec![];
        res.append(&mut GeneralConfigProperties::config_names());
        res.append(&mut SocketConfigProperties::config_names());
        res.append(&mut LogConfigProperties::config_names());
        res.append(&mut QuotaConfigProperties::config_names());
        res.append(&mut TransactionConfigProperties::config_names());
        // TODO: This should be derivable somehow too.
        res.append(&mut vec![
            ZOOKEEPER_CONNECT_PROP.to_string(),
            ZOOKEEPER_SESSION_TIMEOUT_PROP.to_string(),
            ZOOKEEPER_CONNECTION_TIMEOUT_PROP.to_string(),
        ]);
        res
    }

    /// `build` validates and resolves dependant properties from a KafkaConfigProperties into a
    /// KafkaConfig
    fn build(&mut self) -> Result<Self::ConfigType, KafkaConfigError> {
        let general = self.general.build()?;
        let socket = self.socket.build()?;
        let log = self.log.build()?;
        let zk_connect = self.zk_connect.build()?;
        let zk_session_timeout_ms = self.zk_session_timeout_ms.build()?;
        // Satisties REQ-01, if zk_connection_timeout_ms is unset the value of
        // zk_connection_timeout_ms will be used.
        // RAFKA NOTE: somehow the zk_session_timeout_ms build needs to be called before this,
        // maybe resolve can do it?
        self.zk_connection_timeout_ms.resolve(&self.zk_session_timeout_ms);
        let zk_connection_timeout_ms = self.zk_connection_timeout_ms.build()?;
        let zk_max_in_flight_requests = self.zk_max_in_flight_requests.build()?;
        let transaction = self.transaction.build()?;
        let quota = self.quota.build()?;
        let kafka_config = Self::ConfigType {
            general,
            socket,
            zk_connect,
            zk_session_timeout_ms,
            zk_connection_timeout_ms,
            zk_max_in_flight_requests,
            log,
            transaction,
            quota,
        };
        kafka_config.validate_values()
    }
}

impl KafkaConfigProperties {
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
}
#[derive(Debug, PartialEq, Clone)]
pub struct KafkaConfig {
    pub general: GeneralConfig,
    pub socket: SocketConfig,
    pub zk_connect: String,
    pub zk_session_timeout_ms: u32,
    pub zk_connection_timeout_ms: u32,
    pub zk_max_in_flight_requests: u32,
    pub log: LogConfig,
    pub transaction: TransactionConfig,
    pub quota: QuotaConfig,
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
        self.general.validate_values()?;
        Ok(self)
    }
}

impl Default for KafkaConfig {
    fn default() -> Self {
        // Somehow this should only be allowed for testing...
        let mut config_properties = KafkaConfigProperties::default();
        let zk_connect = String::from("UNSET");
        let zk_session_timeout_ms = config_properties.zk_session_timeout_ms.build().unwrap();
        config_properties
            .zk_connection_timeout_ms
            .resolve(&config_properties.zk_session_timeout_ms);
        let zk_connection_timeout_ms = config_properties.zk_connection_timeout_ms.build().unwrap();
        let zk_max_in_flight_requests =
            config_properties.zk_max_in_flight_requests.build().unwrap();
        let transaction = config_properties.transaction.build().unwrap();
        Self {
            general: GeneralConfig::default(),
            socket: SocketConfig::default(),
            zk_connect,
            zk_session_timeout_ms,
            zk_connection_timeout_ms,
            zk_max_in_flight_requests,
            log: LogConfig::default(),
            transaction,
            quota: QuotaConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::general::{BROKER_ID_PROP, MESSAGE_MAX_BYTES_PROP, RESERVED_BROKER_MAX_ID_PROP};
    use super::log::{LOG_DIRS_PROP, LOG_DIR_PROP};
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
