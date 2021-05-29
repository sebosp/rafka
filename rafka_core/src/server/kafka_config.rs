/// Core Kafka Config
/// core/src/main/scala/kafka/server/KafkaConfig.scala
/// Changes:
/// - No SSL for now.
use fs_err::File;
use std::collections::HashMap;
use std::convert::TryInto;
use std::io::{self, BufReader};
use std::num;
use std::str::FromStr;
use thiserror::Error;
use tracing::{debug, error, info};

pub const ZOOKEEPER_CONNECT_PROP: &str = "zookeeper.connect";
pub const ZOOKEEPER_SESSION_TIMEOUT_PROP: &str = "zookeeper.session.timeout.ms";
pub const ZOOKEEPER_CONNECTION_TIMEOUT_PROP: &str = "zookeeper.connection.timeout.ms";
pub const LOG_DIR_PROP: &str = "log.dir";
pub const LOG_DIRS_PROP: &str = "log.dirs";
pub const BROKER_ID_GENERATION_ENABLED_PROP: &str = "broker.id.generation.enable";
pub const RESERVED_BROKER_MAX_ID_PROP: &str = "reserved.broker.max.id";
pub const BROKER_ID_PROP: &str = "broker.id";
pub const ZOOKEEPER_MAX_IN_FLIGHT_REQUESTS: &str = "zookeeper.max.in.flight.requests";

// Unimplemented:
// ZkEnableSecureAcls = false
// ZkSslClientEnable = false
// ZkSslProtocol = "TLSv1.2"
// ZkSslEndpointIdentificationAlgorithm = "HTTPS"
// ZkSslCrlEnable = false
// ZkSslOcspEnable = false

/// `KafkaConfigDefImportance` provides the levels of importance that different java_properties
/// have.
#[derive(Debug, PartialEq)]
pub enum KafkaConfigDefImportance {
    High,
    Medium,
    Low,
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

/// `KafkaConfigDef` defines the configuration properties, how they can be resolved from other
/// values and their defaults This should be later transformed into a derivable from something like
/// DocOpt.
#[derive(Debug, PartialEq)]
pub struct KafkaConfigDef<T> {
    importance: KafkaConfigDefImportance,
    doc: &'static str,
    /// `default` of the value, this would be parsed and transformed into each field type from
    /// KafkaConfig
    default: Option<String>,
    /// Whether or not this variable was provided by the configuration file.
    provided: bool,
    /// The current value, be it the default or overwritten by config
    value: Option<T>,
}

impl<T> KafkaConfigDef<T>
where
    T: FromStr,
    KafkaConfigError: From<<T as FromStr>::Err>,
    <T as FromStr>::Err: std::fmt::Display,
    T: std::fmt::Debug,
{
    pub fn new() -> Self {
        Self {
            importance: KafkaConfigDefImportance::Low,
            doc: "TODO: Missing Docs",
            default: None,
            provided: false,
            value: None,
        }
    }

    pub fn with_importance(mut self, importance: KafkaConfigDefImportance) -> Self {
        self.importance = importance;
        self
    }

    pub fn with_doc(mut self, doc: &'static str) -> Self {
        self.doc = doc;
        self
    }

    pub fn with_default(mut self, default: String) -> Self {
        //  Pre-fill the value with the default
        match default.parse::<T>() {
            Ok(val) => self.value = Some(val),
            Err(err) => {
                error!("Unable to parse default property for {:?}: {}", self, err);
                panic!();
            },
        }
        self.default = Some(default);
        self
    }

    pub fn set_value(&mut self, value: T) {
        self.value = Some(value);
        self.provided = true;
    }

    pub fn try_set_parsed_value(&mut self, value: &str) -> Result<(), KafkaConfigError> {
        match value.parse::<_>() {
            Ok(val) => {
                self.set_value(val);
                Ok(())
            },
            Err(err) => {
                error!("Unable to parse property {:?} : {}. Doc: {}", value, err, self.doc);
                Err(KafkaConfigError::from(err))
            },
        }
    }

    pub fn get_value(&self) -> Option<&T> {
        self.value.as_ref()
    }

    pub fn get_importance(&self) -> KafkaConfigDefImportance {
        self.importance
    }

    pub fn is_provided(&self) -> bool {
        self.provided
    }
}

#[derive(Debug, PartialEq)]
pub struct KafkaConfigProperties {
    zk_connect: KafkaConfigDef<String>,
    zk_session_timeout_ms: KafkaConfigDef<u32>,
    zk_connection_timeout_ms: KafkaConfigDef<u32>,
    // Singular log.dir
    log_dir: KafkaConfigDef<String>,
    // Multiple comma separated log.dirs, may include spaces after the comma (will be trimmed)
    log_dirs: KafkaConfigDef<String>,
    broker_id_generation_enable: KafkaConfigDef<bool>,
    reserved_broker_max_id: KafkaConfigDef<i32>,
    broker_id: KafkaConfigDef<i32>,
    zk_max_in_flight_requests: KafkaConfigDef<u32>,
}

impl Default for KafkaConfigProperties {
    fn default() -> Self {
        Self {
            zk_connect: KafkaConfigDef::new()
                .with_importance(KafkaConfigDefImportance::High)
                .with_doc(r#"
                    Specifies the ZooKeeper connection string in the form <code>hostname:port</code> where host and port are the
                    host and port of a ZooKeeper server. To allow connecting through other ZooKeeper nodes when that ZooKeeper machine is
                    down you can also specify multiple hosts in the form <code>hostname1:port1,hostname2:port2,hostname3:port3</code>.
                    The server can also have a ZooKeeper chroot path as part of its ZooKeeper connection string which puts its data under some path in the global ZooKeeper namespace.
                    For example to give a chroot path of <code>/chroot/path</code> you would give the connection string as <code>hostname1:port1,hostname2:port2,hostname3:port3/chroot/path</code>.
                    "#
                ),
            zk_session_timeout_ms: KafkaConfigDef::new()
                .with_importance(KafkaConfigDefImportance::High)
                .with_doc("Zookeeper session timeout")
                .with_default(String::from("18000")),
            zk_connection_timeout_ms: KafkaConfigDef::new()
                .with_importance(KafkaConfigDefImportance::High)
                .with_doc(
                    "The max time that the client waits to establish a connection to zookeeper. If \
                     not set, the value in zookeeper.session.timeout.ms is used", // REQ-01
                ),
            zk_max_in_flight_requests: KafkaConfigDef::new()
                .with_importance(KafkaConfigDefImportance::High)
                .with_doc(
                    "The maximum number of unacknowledged requests the client will send to Zookeeper before blocking."
                )
                .with_default(String::from("10")),
            log_dir: KafkaConfigDef::new()
                .with_importance(KafkaConfigDefImportance::High)
                .with_doc(
                    "The directory in which the log data is kept (supplemental for log.dirs property)",
                )
                .with_default(String::from("/tmp/kafka-logs")),
            log_dirs: KafkaConfigDef::new()
                .with_importance(KafkaConfigDefImportance::High)
                .with_doc(
                    "The directories in which the log data is kept. If not set, the value in log.dir \
                     is used",
                ),
            broker_id_generation_enable: KafkaConfigDef::new()
                .with_importance(KafkaConfigDefImportance::Medium)
                .with_doc(
                    "Enable automatic broker id generation on the server. When enabled the value \
                     configured for reserved.broker.max.id should be reviewed.",
                )
                .with_default(String::from("true")),
            reserved_broker_max_id: KafkaConfigDef::new()
                .with_importance(KafkaConfigDefImportance::Medium)
                .with_doc("Max number that can be used for a broker.id")
                .with_default(String::from("1000")),
            broker_id: KafkaConfigDef::new()
                .with_importance(KafkaConfigDefImportance::High)
                .with_doc(
                    "The broker id for this server. If unset, a unique broker id will be generated. \
                     To avoid conflicts between zookeeper generated broker id's and user configured \
                     broker id's, generated broker ids start from  + 1.",
                )
                .with_default(String::from("-1")),
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
            _ => return Err(KafkaConfigError::UnknownKey(property_name.to_string())),
        };
        Ok(())
    }

    //   ZOOKEEPER_CONNECT_PROP => Ok(self.zk_connect.get_value()),
    //   ZOOKEEPER_SESSION_TIMEOUT_PROP => Ok(self.zk_session_timeout_ms.get_value()),
    //   ZOOKEEPER_CONNECTION_TIMEOUT_PROP => Ok(self.zk_connection_timeout_ms.get_value()),
    //   LOG_DIR_PROP => Ok(self.log_dir.get_value()),
    //   LOG_DIRS_PROP => Ok(self.log_dirs.get_value()),
    //   BROKER_ID_GENERATION_ENABLED_PROP => Ok(self.broker_id_generation_enable.get_value()),
    //   RESERVED_BROKER_MAX_ID_PROP => Ok(self.reserved_broker_max_id.get_value()),
    //   BROKER_ID_PROP => Ok(self.broker_id.get_value()),
    //
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
            Some(val) => Ok(val),
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
            Ok(val)
        } else {
            debug!(
                "Unspecified property {} will use {:?} from {}",
                ZOOKEEPER_CONNECTION_TIMEOUT_PROP,
                self.zk_session_timeout_ms.get_value(),
                ZOOKEEPER_SESSION_TIMEOUT_PROP
            );
            // Fallback to the zookeeper.connection.timeout.ms value
            self.zk_connection_timeout_ms.set_value(self.resolve_zk_session_timeout_ms()?);
            Ok(self.zk_session_timeout_ms.get_value().unwrap())
        }
    }

    /// `resolve_log_dirs` validates the log.dirs and log.dir combination. Note that the end value
    /// in KafkaConfig has a default, so even if they are un-set, they will be marked as provided
    fn resolve_log_dirs(&mut self) -> Result<Vec<String>, KafkaConfigError> {
        // TODO: Consider checking for valid Paths and return KafkaConfigError for them
        if let Some(log_dirs) = &self.log_dirs.get_value() {
            Ok(log_dirs.clone().split(',').map(|x| x.trim_start().to_string()).collect())
        } else if let Some(log_dir) = &self.log_dir.get_value() {
            Ok(vec![log_dir.clone()])
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
        // at least 0
        if let Some(reserved_broker_max_id) = self.reserved_broker_max_id.get_value() {
            if reserved_broker_max_id < 0 {
                Err(KafkaConfigError::InvalidValue(format!(
                    "{}: '{}' should be at least 0",
                    RESERVED_BROKER_MAX_ID_PROP, reserved_broker_max_id
                )))
            } else {
                Ok(reserved_broker_max_id)
            }
        } else {
            panic!(
                "zk_session_timeout_ms has a default but found its value to be None, bug in \
                 parsing defaults"
            );
        }
    }

    fn resolve_broker_id(&mut self) -> Result<i32, KafkaConfigError> {
        todo!()
    }

    fn resolve_broker_id_generation_enable(&mut self) -> Result<bool, KafkaConfigError> {
        if let Some(val) = self.broker_id_generation_enable.get_value() {
            Ok(val)
        } else {
            Err(KafkaConfigError::MissingKey(BROKER_ID_GENERATION_ENABLED_PROP.to_string()))
        }
    }

    fn resolve_zk_max_in_flight_requests(&mut self) -> Result<u32, KafkaConfigError> {
        // at least 0
        if let Some(zk_max_in_flight_requests) = self.zk_max_in_flight_requests.get_value() {
            if zk_max_in_flight_requests < 1 {
                Err(KafkaConfigError::InvalidValue(format!(
                    "{}: '{}' should be at least 0",
                    ZOOKEEPER_MAX_IN_FLIGHT_REQUESTS, zk_max_in_flight_requests
                )))
            } else {
                Ok(zk_max_in_flight_requests)
            }
        } else {
            panic!(
                "zk_max_in_flight_requests has a default but found its value to be None, bug in \
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
        let mut kafka_config = KafkaConfig {
            zk_connect,
            zk_session_timeout_ms,
            zk_connection_timeout_ms,
            zk_max_in_flight_requests,
            log_dirs,
            broker_id_generation_enable,
            reserved_broker_max_id,
            broker_id,
        };
        kafka_config.validate_values()
    }
}
#[derive(Debug, PartialEq, Default, Clone)]
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
        if self.broker_id < -1 || self.broker_id > self.reserved_broker_max_id.try_into().unwrap() {
            Err(KafkaConfigError::InvalidValue(format!(
                "{}: '{}' must be equal or greater than -1 and not greater than {}",
                self.broker_id, BROKER_ID_PROP, RESERVED_BROKER_MAX_ID_PROP
            )))
        } else if self.broker_id < 0 {
            Err(KafkaConfigError::InvalidValue(format!(
                "{}: '{}' must be equal or greater than 0",
                BROKER_ID_PROP, self.broker_id
            )))
        } else {
            Ok(self)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_gets_config_from_hashmap() {
        let empty_config: HashMap<String, String> = HashMap::new();
        if let Err(empty_config_builder) =
            KafkaConfigProperties::from_properties_hashmap(empty_config).unwrap().build()
        {
            assert_eq!(
                empty_config_builder,
                KafkaConfigError::MissingKey(ZOOKEEPER_CONNECT_PROP.to_string())
            );
        } else {
            panic!("Expected Err result on empty_config");
        }
        let mut unknown_key_config: HashMap<String, String> = HashMap::new();
        unknown_key_config.insert(String::from("not.a.known.key"), String::from("127.0.0.1:2181"));
        assert_eq!(
            KafkaConfigProperties::from_properties_hashmap(unknown_key_config),
            Err(KafkaConfigError::UnknownKey(String::from("not.a.known.key")))
        );
        let mut missing_key_config: HashMap<String, String> = HashMap::new();
        missing_key_config
            .insert(String::from("zookeeper.session.timeout.ms"), String::from("1000"));
        if let Err(empty_config_builder) =
            KafkaConfigProperties::from_properties_hashmap(missing_key_config).unwrap().build()
        {
            assert_eq!(
                empty_config_builder,
                KafkaConfigError::MissingKey(ZOOKEEPER_CONNECT_PROP.to_string())
            );
        } else {
            panic!("Expected Err result on missing_key");
        }
        let mut full_config: HashMap<String, String> = HashMap::new();
        full_config.insert(String::from(ZOOKEEPER_CONNECT_PROP), String::from("127.0.0.1:2181"));
        full_config.insert(String::from(ZOOKEEPER_SESSION_TIMEOUT_PROP), String::from("1000"));
        full_config.insert(String::from(ZOOKEEPER_CONNECTION_TIMEOUT_PROP), String::from("1000"));
        full_config.insert(String::from(LOG_DIRS_PROP), String::from("/some-dir/logs"));
        assert!(KafkaConfigProperties::from_properties_hashmap(full_config).is_ok());
    }
}
