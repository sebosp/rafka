//! Core Kafka Config
//! core/src/main/scala/kafka/server/KafkaConfig.scala
//! Changes:
//! - No SSL, no SASL
//! - RAFKA NOTE: ADVERTISED_LISTENERS are variable keys that need to be decomposed into actual
//!   listeners, thus using serde_json need to be tweaked properly

pub mod general;
pub mod log;
pub mod quota;
pub mod replication;
pub mod socket_server;
pub mod transaction_management;
pub mod zookeeper;

use self::general::{GeneralConfig, GeneralConfigProperties};
use self::log::{DefaultLogConfig, DefaultLogConfigProperties};
use self::quota::{QuotaConfig, QuotaConfigProperties};
use self::replication::{ReplicationConfig, ReplicationConfigProperties};
use self::transaction_management::{TransactionConfig, TransactionConfigProperties};
use self::zookeeper::{ZookeeperConfig, ZookeeperConfigProperties};
use crate::common::config::config_exception;
use crate::common::security::auth::security_protocol::SecurityProtocolError;
use fs_err::File;
use std::collections::HashMap;
use std::io::{self, BufReader};
use std::num;
use std::str::FromStr;
use thiserror::Error;
use tracing::{debug, trace};

use self::socket_server::{SocketConfig, SocketConfigKey, SocketConfigProperties};

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
    // RAFKA TODO: Move this to its sub-type Error (i.e. SocketConfigPropertiesError)
    #[error("ListenerMisconfig")]
    ListenerMisconfig(String),
    #[error("Unknown Cleanup Policy")]
    UnknownCleanupPolicy(String),
    #[error("Invalid Broker Compression Codec")]
    InvalidBrokerCompressionCodec(String),
    #[error("Config Exception {0}")]
    ConfigException(#[from] config_exception::ConfigException),
    #[error("Invalid Log Message Timestamp Type")]
    InvalidLogMessageTimestampType(String),
    #[error("Security Protocol Config Error: {0}")]
    SecurityProtocol(#[from] SecurityProtocolError),
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
            Self::UnknownCleanupPolicy(lhs) => {
                matches!(rhs, Self::UnknownCleanupPolicy(rhs) if lhs == rhs)
            },
            Self::InvalidBrokerCompressionCodec(lhs) => {
                matches!(rhs, Self::InvalidBrokerCompressionCodec(rhs) if lhs == rhs)
            },
            Self::ConfigException(lhs) => matches!(rhs, Self::ConfigException(rhs) if lhs == rhs),
            Self::InvalidLogMessageTimestampType(lhs) => {
                matches!(rhs, Self::InvalidLogMessageTimestampType(rhs) if lhs == rhs)
            },
            Self::SecurityProtocol(lhs) => matches!(rhs, Self::SecurityProtocol(rhs) if lhs == rhs),
        }
    }
}

/// A type may implement TrySetProperty which receives a "key" that is tied to a specific field and
/// a value to set it that impls FromStr, usually read from config files, zookeeper, etc
pub trait TrySetProperty {
    /// `config_names` returns a list of config keys managed by a specific Properties
    fn config_names() -> Vec<&'static str>;

    /// `try_from_config_property` transforms a string value from the config into our actual types
    fn try_set_property(
        &mut self,
        property_name: &str,
        property_value: &str,
    ) -> Result<(), KafkaConfigError>;
}

/// A set of methods that the different configuration-sets must implement, including building,
/// validating, etc.
pub trait ConfigSet {
    type ConfigType;
    /// `resolve` dependant properties from a ConfigKey into a ConfigType.
    /// NOTE: This doesn't consume self, as a ConfigKey may be re-used after bootstrap,
    /// on-the-fly without the need to restart, for example via zookeeper
    fn resolve(&mut self) -> Result<Self::ConfigType, KafkaConfigError>;
    /// `build` calls the per-type `resolve`/`build` to transform From<ConfigDef<T>> -> T
    /// Which resolves/fallbacks to other properties and/or uses defaults.
    /// Once value resolution is done, validate_set makes sure that variables are compatible
    /// with each-other
    fn build(&mut self) -> Result<Self::ConfigType, KafkaConfigError> {
        let res = self.resolve()?;
        self.validate_set(&res)?;
        Ok(res)
    }

    /// Transforms from a HashMap of configs into a KafkaConfigProperties object
    /// This may return KafkaConfigError::UnknownKey errors
    fn from_properties_hashmap(
        input_config: HashMap<String, String>,
    ) -> Result<Self, KafkaConfigError>
    where
        Self: Default + TrySetProperty,
    {
        let mut config_builder = Self::default();
        for (property, property_value) in &input_config {
            debug!("from_properties_hashmap: {} = {}", property, property_value);
            config_builder.try_set_property(property, property_value)?;
        }
        Ok(config_builder)
    }
    /// `validate_set` ensures values are compatible with each other and within limits not provided
    /// by the custom Validator types.
    /// The ConfigDef deriver creates a validate_values that calls *per*-field validator
    fn validate_set(&self, _cfg: &Self::ConfigType) -> Result<(), KafkaConfigError> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct KafkaConfigProperties {
    zookeeper: ZookeeperConfigProperties,
    general: GeneralConfigProperties,
    socket: SocketConfigProperties,
    log: DefaultLogConfigProperties,
    transaction: TransactionConfigProperties,
    quota: QuotaConfigProperties,
    replication: ReplicationConfigProperties,
}

impl Default for KafkaConfigProperties {
    fn default() -> Self {
        Self {
            zookeeper: ZookeeperConfigProperties::default(),
            general: GeneralConfigProperties::default(),
            socket: SocketConfigProperties::default(),
            log: DefaultLogConfigProperties::default(),
            transaction: TransactionConfigProperties::default(),
            quota: QuotaConfigProperties::default(),
            replication: ReplicationConfigProperties::default(),
        }
    }
}

impl KafkaConfigProperties {
    /// RAFKA NOTE: We can't impl ConfigSet here because our KafkaConfigKey cannot derive
    /// IntoEnumIterator, we could duplicate the code from config_names() everywhere but that looks
    /// like more pain than gain.
    pub fn try_set_property(
        &mut self,
        property_name: &str,
        property_value: &str,
    ) -> Result<(), KafkaConfigError> {
        if ZookeeperConfigProperties::config_names().contains(&property_name) {
            self.zookeeper.try_set_property(property_name, property_value)
        } else if GeneralConfigProperties::config_names().contains(&property_name) {
            self.general.try_set_property(property_name, property_value)
        } else if SocketConfigProperties::config_names().contains(&property_name) {
            self.socket.try_set_property(property_name, property_value)
        } else if DefaultLogConfigProperties::config_names().contains(&property_name) {
            self.log.try_set_property(property_name, property_value)
        } else if TransactionConfigProperties::config_names().contains(&property_name) {
            self.transaction.try_set_property(property_name, property_value)
        } else if QuotaConfigProperties::config_names().contains(&property_name) {
            self.quota.try_set_property(property_name, property_value)
        } else if ReplicationConfigProperties::config_names().contains(&property_name) {
            self.replication.try_set_property(property_name, property_value)
        } else {
            Err(KafkaConfigError::UnknownKey(property_name.to_string()))
        }
    }

    /// `config_names` returns a list of config keys used by KafkaConfigProperties
    pub fn config_names() -> Vec<&'static str> {
        // RAFKA TODO: Add unit tests to make sure all configs are returned here
        let mut res = vec![];
        res.append(&mut ZookeeperConfigProperties::config_names().clone());
        res.append(&mut GeneralConfigProperties::config_names());
        res.append(&mut SocketConfigProperties::config_names());
        res.append(&mut DefaultLogConfigProperties::config_names());
        res.append(&mut QuotaConfigProperties::config_names());
        res.append(&mut TransactionConfigProperties::config_names());
        res
    }

    /// `build` validates and resolves dependant properties from a KafkaConfigProperties into a
    /// KafkaConfig
    pub fn build(&mut self) -> Result<KafkaConfig, KafkaConfigError> {
        trace!("KafkaConfigProperties::build() INIT");
        let zookeeper = self.zookeeper.build()?;
        let general = self.general.build()?;
        let socket = self.socket.build()?;
        let log = self.log.build()?;
        let transaction = self.transaction.build()?;
        let quota = self.quota.build()?;
        let replication = self.replication.build()?;
        let kafka_config =
            KafkaConfig { zookeeper, general, socket, log, transaction, quota, replication };
        trace!("KafkaConfigProperties::build() DONE");
        Ok(kafka_config)
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

    /// `read_config_file` Reads the kafka config.
    pub fn read_config_file(filename: &str) -> Result<Self, KafkaConfigError> {
        debug!("read_config_from: Reading {}", filename);
        let mut config_file_content = File::open(&filename)?;
        let input_config = java_properties::read(BufReader::new(&mut config_file_content))?;
        Ok(KafkaConfigProperties::from_properties_hashmap(input_config)?)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct KafkaConfig {
    pub zookeeper: ZookeeperConfig,
    pub general: GeneralConfig,
    pub socket: SocketConfig,
    pub log: DefaultLogConfig,
    pub transaction: TransactionConfig,
    pub quota: QuotaConfig,
    pub replication: ReplicationConfig,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        // Somehow this should only be allowed for testing...
        Self {
            zookeeper: ZookeeperConfig::default(),
            general: GeneralConfig::default(),
            socket: SocketConfig::default(),
            log: DefaultLogConfig::default(),
            transaction: TransactionConfig::default(),
            quota: QuotaConfig::default(),
            replication: ReplicationConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::general::{BROKER_ID_PROP, RESERVED_BROKER_MAX_ID_PROP};
    use super::log::{LOG_DIRS_PROP, LOG_DIR_PROP};
    use super::zookeeper::*;
    use super::*;

    #[test]
    fn it_gets_config_from_hashmap() {
        let empty_config: HashMap<String, String> = HashMap::new();
        let config_error = KafkaConfigProperties::from_properties_hashmap(empty_config)
            .unwrap()
            .build()
            .unwrap_err();
        assert_eq!(config_error, KafkaConfigError::MissingKey(ZK_CONNECT_PROP.to_string()));
        let mut unknown_key_config: HashMap<String, String> = HashMap::new();
        unknown_key_config.insert(String::from("not.a.known.key"), String::from("127.0.0.1:2181"));
        if let Err(actual_err) = KafkaConfigProperties::from_properties_hashmap(unknown_key_config)
        {
            assert_eq!(actual_err, KafkaConfigError::UnknownKey(String::from("not.a.known.key")));
        }
        let mut missing_key_config: HashMap<String, String> = HashMap::new();
        missing_key_config
            .insert(String::from("zookeeper.session.timeout.ms"), String::from("1000"));
        let config_error = KafkaConfigProperties::from_properties_hashmap(missing_key_config)
            .unwrap()
            .build()
            .unwrap_err();
        assert_eq!(config_error, KafkaConfigError::MissingKey(ZK_CONNECT_PROP.to_string()));
        let mut full_config: HashMap<String, String> = HashMap::new();
        full_config.insert(String::from(ZK_CONNECT_PROP), String::from("127.0.0.1:2181"));
        full_config.insert(String::from(ZK_SESSION_TIMEOUT_PROP), String::from("1000"));
        full_config.insert(String::from(ZK_CONNECTION_TIMEOUT_PROP), String::from("1000"));
        full_config.insert(String::from(LOG_DIRS_PROP), String::from("/some-dir/logs"));
        assert!(KafkaConfigProperties::from_properties_hashmap(full_config).is_ok());
        let mut multiple_log_dir_properties: HashMap<String, String> = HashMap::new();
        multiple_log_dir_properties
            .insert(String::from(ZK_CONNECT_PROP), String::from("127.0.0.1:2181"));
        multiple_log_dir_properties
            .insert(String::from(LOG_DIR_PROP), String::from("/single/log/dir"));
        multiple_log_dir_properties
            .insert(String::from(LOG_DIRS_PROP), String::from("/some-1/logs, /some-2-logs"));
        let config_builder =
            KafkaConfigProperties::from_properties_hashmap(multiple_log_dir_properties);
        assert!(config_builder.is_ok());
        let mut config_builder = config_builder.unwrap();
        let config = config_builder.build().unwrap();
        assert_eq!(config.log.log_dirs, vec![
            String::from("/some-1/logs"),
            String::from("/some-2-logs")
        ]);
        let mut invalid_broker_id: HashMap<String, String> = HashMap::new();
        invalid_broker_id.insert(String::from(ZK_CONNECT_PROP), String::from("127.0.0.1:2181"));
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
