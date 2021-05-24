/// Core Kafka Config
/// core/src/main/scala/kafka/server/KafkaConfig.scala
/// Changes:
/// - No SSL for now.
use fs_err::File;
use std::collections::HashMap;
use std::io::{self, BufReader};
use std::num;
use thiserror::Error;
use tracing::{debug, error, info};

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
    #[error("Parse error: {0}")]
    ParseInt(#[from] num::ParseIntError),
    #[error("Missing Key error: {0:?}")]
    MissingKeys(Vec<String>),
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
            Self::MissingKeys(lhs) => matches!(rhs, Self::MissingKeys(rhs) if lhs == rhs),
            Self::InvalidValue(lhs) => matches!(rhs, Self::InvalidValue(rhs) if lhs == rhs),
            Self::UnknownKey(lhs) => matches!(rhs, Self::UnknownKey(rhs) if lhs == rhs),
            Self::DuplicateKey(lhs) => matches!(rhs, Self::DuplicateKey(rhs) if lhs == rhs),
            Self::ParseInt(lhs) => matches!(rhs, Self::ParseInt(rhs) if lhs == rhs),
        }
    }
}

/// `KafkaConfigDef` defines the configuration properties, how they can be resolved from other
/// values and their defaults This should be later transformed into a derivable from something like
/// DocOpt.
#[derive(Debug)]
pub struct KafkaConfigDef {
    key: String,
    importance: KafkaConfigDefImportance,
    doc: String,
    /// `default` of the value, this would be parsed and transformed into each field type from
    /// KafkaConfig
    default: Option<String>,
    provided: bool,
}

/// `gen_kafka_config_definition` returns the configuration properties HashMap.
fn gen_kafka_config_definition() -> HashMap<String, KafkaConfigDef> {
    let mut res = HashMap::new();
    res.insert(
        String::from("zookeeper.connect"),
        KafkaConfigDef{
            key: String::from("zk_connect"),
            importance: KafkaConfigDefImportance::High,
            doc: String::from(r#"
            Specifies the ZooKeeper connection string in the form <code>hostname:port</code> where host and port are the
            host and port of a ZooKeeper server. To allow connecting through other ZooKeeper nodes when that ZooKeeper machine is
            down you can also specify multiple hosts in the form <code>hostname1:port1,hostname2:port2,hostname3:port3</code>.\n
            The server can also have a ZooKeeper chroot path as part of its ZooKeeper connection string which puts its data under some path in the global ZooKeeper namespace.
            For example to give a chroot path of <code>/chroot/path</code> you would give the connection string as <code>hostname1:port1,hostname2:port2,hostname3:port3/chroot/path</code>.
            "#),
            default: None,
            provided: false,
        }
    );
    res.insert(String::from("zookeeper.session.timeout.ms"), KafkaConfigDef {
        key: String::from("zk_session_timeout_ms"),
        importance: KafkaConfigDefImportance::High,
        doc: String::from("Zookeeper session timeout"),
        // val ZkSessionTimeoutMs = 18000
        default: Some(String::from("18000")),
        provided: false,
    });
    res.insert(String::from("zookeeper.connection.timeout.ms"), KafkaConfigDef {
        key: String::from("zk_connection_timeout_ms"),
        importance: KafkaConfigDefImportance::High,
        doc: String::from(
            "The max time that the client waits to establish a connection to zookeeper. If not \
             set, the value in zookeeper.session.timeout.ms is used", // REQ-01
        ),
        default: None,
        provided: false,
    });
    // res.insert(String::from("zookeeper.sync.time.ms"), KafkaConfigDef {
    // key: String::from("zk_sync_time_ms"),
    // importance: KafkaConfigDefImportance::Low,
    // doc: String::from("How far a ZK follower can be behind a ZK leader"),
    // val ZkSyncTimeMs = 2000
    // default: Some(String::from("2000")),
    // provided: false,
    // });
    res.insert(String::from("log.dir"), KafkaConfigDef {
        key: String::from("log_dirs"),
        importance: KafkaConfigDefImportance::High,
        doc: String::from(
            "The directory in which the log data is kept (supplemental for log.dirs property)",
        ),
        // val LogDir = "/tmp/kafka-logs"
        default: Some(String::from("/tmp/kafka-logs")),
        provided: false,
    });
    res.insert(String::from("log.dirs"), KafkaConfigDef {
        key: String::from("log_dirs"),
        importance: KafkaConfigDefImportance::High,
        doc: String::from(
            "The directories in which the log data is kept. If not set, the value in log.dir is \
             used",
        ),
        default: None,
        provided: false,
    });
    res
}

#[derive(Debug)]
pub struct KafkaConfigBuilder {
    pub zk_connect: Option<String>,
    pub zk_session_timeout_ms: Option<u32>,
    pub zk_sync_time_ms: Option<u32>,
    pub zk_connection_timeout_ms: Option<u32>,
    pub zk_max_in_flight_requests: Option<u32>,
    pub log_dirs: Option<String>,
    pub log_dir: Option<String>,
    config_definition: HashMap<String, KafkaConfigDef>,
}

impl Default for KafkaConfigBuilder {
    fn default() -> Self {
        let mut config_builder = KafkaConfigBuilder {
            zk_connect: None,
            zk_session_timeout_ms: None,
            zk_sync_time_ms: None,
            zk_connection_timeout_ms: None,
            zk_max_in_flight_requests: None,
            log_dirs: None,
            log_dir: None,
            config_definition: gen_kafka_config_definition(),
        };
        config_builder.set_defaults_from_config_definition().unwrap();
        config_builder
    }
}

impl PartialEq for KafkaConfigBuilder {
    fn eq(&self, rhs: &Self) -> bool {
        self.zk_connect == rhs.zk_connect
            && self.zk_session_timeout_ms == rhs.zk_session_timeout_ms
            && self.zk_sync_time_ms == rhs.zk_sync_time_ms
            && self.zk_connection_timeout_ms == rhs.zk_connection_timeout_ms
            && self.zk_max_in_flight_requests == rhs.zk_max_in_flight_requests
            && self.log_dirs == rhs.log_dirs
            && self.log_dir == rhs.log_dir
    }
}

impl KafkaConfigBuilder {
    /// `set_config_key_as_provided` sets one of the java properties as provided, this happens when
    /// the variable is resolved by using the value of another variable.
    pub fn set_config_key_as_provided(&mut self, key: &str) {
        if let Some(config_def) = self.config_definition.get_mut(key) {
            config_def.provided = true;
        } else {
            // Help with typos
            panic!("KafkaConfigBuilder::set_config_key_as_provided No such key {}", key);
        }
    }

    /// `try_from_property_to_u32` transform a string property into a destination field from
    /// KafkaConfig fileds
    pub fn try_from_property_to_u32(
        &mut self,
        property_name: &str,
        property_value: &str,
    ) -> Result<u32, KafkaConfigError> {
        match self.config_definition.get_mut(property_name) {
            Some(property_definition) => match property_value.parse::<u32>() {
                Ok(val) => {
                    property_definition.provided = true;
                    Ok(val)
                },
                Err(err) => {
                    error!(
                        "Unable to parse property {} to u32 number: {}. Doc: {}",
                        property_value, err, property_definition.doc
                    );
                    Err(KafkaConfigError::from(err))
                },
            },
            None => {
                error!("Unknown/Unhandled Configuration Key: {}", property_name);
                Err(KafkaConfigError::UnknownKey(property_name.to_string()))
            },
        }
    }

    /// `parse_to_field_name` gets a property string and a property value as strings (usually from
    /// configuration files) and sets these values in the builder struct
    fn parse_to_field_name(&mut self, property: &str, value: &str) -> Result<(), KafkaConfigError> {
        match property {
            "zookeeper.connect" => self.zk_connect = Some(value.to_string()),
            "zookeeper.session.timeout.ms" => {
                self.zk_session_timeout_ms = Some(self.try_from_property_to_u32(property, value)?);
            },
            "zookeeper.connection.timeout.ms" => {
                self.zk_connection_timeout_ms =
                    Some(self.try_from_property_to_u32(property, value)?);
            },
            "log.dirs" => self.log_dirs = Some(value.to_string()),
            "log.dir" => self.log_dir = Some(value.to_string()),
            _ => return Err(KafkaConfigError::UnknownKey(property.to_string())),
        }
        self.set_config_key_as_provided(property);
        Ok(())
    }

    /// Iterates over the Config Definition `default` field and sets each value on the builder
    pub fn set_defaults_from_config_definition(&mut self) -> Result<(), KafkaConfigError> {
        // TODO: Second call to gen_kafka_config_definition is done to avoid borrow checks, maybe
        // find a way around it
        for (property, property_definition) in gen_kafka_config_definition() {
            if let Some(property_value) = &property_definition.default {
                debug!("set_default: {} = {}", property, property_value);
                self.parse_to_field_name(&property, &property_value)?;
            }
        }
        Ok(())
    }

    /// Transforms from a HashMap of configs into a KafkaConfigBuilder object
    /// This may return KafkaConfigError::UnknownKey errors
    pub fn from_properties_hashmap(
        input_config: HashMap<String, String>,
    ) -> Result<Self, KafkaConfigError> {
        let mut config_builder = KafkaConfigBuilder::default();
        for (property, property_value) in &input_config {
            debug!("from_properties_hashmap: {} = {}", property, property_value);
            config_builder.parse_to_field_name(property, property_value)?;
        }
        Ok(config_builder)
    }

    fn resolve_zk_session_timeout_ms(&mut self, kafka_config: &mut KafkaConfig) {
        // NOTE: zk_session_timeout_ms has a default, so it is never None
        if let Some(zk_session_timeout_ms) = self.zk_session_timeout_ms {
            kafka_config.zk_session_timeout_ms = zk_session_timeout_ms;
            self.set_config_key_as_provided("zookeeper.session.timeout.ms");
        }
    }

    /// `resolve_zk_connection_timeout_ms` Satisties REQ-01, if zk_connection_timeout_ms is unset
    /// the value of zk_connection_timeout_ms will be used.
    fn resolve_zk_connection_timeout_ms(
        &mut self,
        kafka_config: &mut KafkaConfig,
    ) -> Result<(), KafkaConfigError> {
        if let Some(zk_connection_timeout_ms) = self.zk_connection_timeout_ms {
            kafka_config.zk_connection_timeout_ms = zk_connection_timeout_ms;
            return Ok(());
        } else {
            debug!(
                "Unspecified property zk_connection_timeout_ms will use {:?} from \
                 zookeeper.session.timeout.ms",
                self.zk_session_timeout_ms
            );
            self.set_config_key_as_provided("zookeeper.connection.timeout.ms");
            self.zk_connection_timeout_ms = self.zk_session_timeout_ms;
        };
        Ok(())
    }

    /// `resolve_log_dirs` validates the log.dirs and log.dir combination. Note that the end value
    /// in KafkaConfig has a default, so even if they are un-set, they will be marked as provided
    fn resolve_log_dirs(&mut self, kafka_config: &mut KafkaConfig) -> Result<(), KafkaConfigError> {
        // TODO: Consider checking for valid Paths and return KafkaConfigError for them
        if let Some(log_dirs) = &self.log_dirs {
            kafka_config.log_dirs =
                log_dirs.clone().split(',').map(|x| x.trim_start().to_string()).collect();
        } else if let Some(log_dir) = &self.log_dir {
            kafka_config.log_dirs = vec![log_dir.clone()];
        }
        self.set_config_key_as_provided("log.dirs");
        self.set_config_key_as_provided("log.dir");
        Ok(())
    }

    /// `build` validates and resolves dependant properties from a KafkaConfigBuilder into a
    /// KafkaConfig
    pub fn build(&mut self) -> Result<KafkaConfig, KafkaConfigError> {
        let mut kafka_config = KafkaConfig::default();
        self.resolve_zk_session_timeout_ms(&mut kafka_config);
        self.resolve_zk_connection_timeout_ms(&mut kafka_config)?;
        self.resolve_log_dirs(&mut kafka_config)?;
        if let Some(zk_connect) = &self.zk_connect {
            kafka_config.zk_connect = zk_connect.to_string();
        }
        let mut missing_keys: Vec<String> = vec![];
        for (property, property_def) in &self.config_definition {
            if KafkaConfigDefImportance::High == property_def.importance && !property_def.provided {
                missing_keys.push(property.to_string());
            }
        }
        if !missing_keys.is_empty() {
            return Err(KafkaConfigError::MissingKeys(missing_keys));
        }
        Ok(kafka_config)
    }
}
#[derive(Debug, PartialEq, Default, Clone)]
pub struct KafkaConfig {
    pub zk_connect: String,
    pub zk_session_timeout_ms: u32,
    pub zk_sync_time_ms: u32,
    pub zk_connection_timeout_ms: u32,
    pub zk_max_in_flight_requests: u32,
    pub log_dirs: Vec<String>,
}

#[macro_export]
macro_rules! from_property_u32 {
    ( $input_config:expr, $config_definition:expr, $property:expr, $value:expr) => {{
        match $config_definition.get_mut($property) {
            Some(property_definition) => match $value.parse::<u32>() {
                Ok(val) => {
                    property_definition.provided = true;
                    Ok(val)
                },
                Err(err) => {
                    error!(
                        "Unable to parse property {} to u32 number: {}. Doc: {}",
                        $value, err, property_definition.doc
                    );
                    Err(KafkaConfigError::ParseInt(err))
                },
            },
            None => {
                error!("Unknown/Unhandled Configuration Key: {}", $property);
                Err(KafkaConfigError::UnknownKey($property.to_string()))
            },
        }
    }};
}

impl KafkaConfig {
    /// `get_kafka_config` Reads the kafka config.
    pub fn get_kafka_config(filename: &str) -> Result<Self, KafkaConfigError> {
        debug!("read_config_from: Reading {}", filename);
        let mut config_file_content = File::open(&filename)?;
        let input_config = java_properties::read(BufReader::new(&mut config_file_content))?;
        debug!("read_config_from: Converting to HashMap");
        KafkaConfigBuilder::from_properties_hashmap(input_config)?.build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_gets_config_from_hashmap() {
        // Property(java_properties::PropertiesError),
        // ParseInt(num::ParseIntError),
        // MissingKeys(String),
        // InvalidValue(String),
        // UnknownKey(String),
        let all_required_keys: Vec<String> = vec![String::from("zookeeper.connect")];
        let empty_config: HashMap<String, String> = HashMap::new();
        if let Err(empty_config_builder) =
            KafkaConfigBuilder::from_properties_hashmap(empty_config).unwrap().build()
        {
            assert!(matches!(
                empty_config_builder,
                KafkaConfigError::MissingKeys(req_keys) if req_keys == all_required_keys
            ));
        } else {
            panic!("Expected Err result on empty_config");
        }
        let mut unknown_key_config: HashMap<String, String> = HashMap::new();
        unknown_key_config.insert(String::from("not.a.known.key"), String::from("127.0.0.1:2181"));
        assert_eq!(
            KafkaConfigBuilder::from_properties_hashmap(unknown_key_config),
            Err(KafkaConfigError::UnknownKey(String::from("not.a.known.key")))
        );
        let mut missing_key_config: HashMap<String, String> = HashMap::new();
        missing_key_config
            .insert(String::from("zookeeper.session.timeout.ms"), String::from("1000"));
        let missing_keys_builder =
            KafkaConfigBuilder::from_properties_hashmap(missing_key_config).unwrap().build();
        if let Err(KafkaConfigError::MissingKeys(mut missing_keys)) = missing_keys_builder {
            assert_eq!(missing_keys.sort(), vec![String::from("zookeeper.connect")].sort());
        } else {
            panic!("Expected Err result on missing_keys");
        }
        let mut full_config: HashMap<String, String> = HashMap::new();
        full_config.insert(String::from("zookeeper.connect"), String::from("127.0.0.1:2181"));
        full_config.insert(String::from("zookeeper.session.timeout.ms"), String::from("1000"));
        full_config.insert(String::from("zookeeper.connection.timeout.ms"), String::from("1000"));
        full_config.insert(String::from("log.dirs"), String::from("/some-dir/logs"));
        assert!(KafkaConfigBuilder::from_properties_hashmap(full_config).is_ok());
    }
}
