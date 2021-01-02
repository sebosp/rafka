/// Core Kafka Config
/// core/src/main/scala/kafka/server/KafkaConfig.scala
/// Changes:
/// - No SSL for now.
use java_properties::read;
use std::collections::HashMap;
use std::error;
use std::fmt;
use std::fs::File;
use std::io::{self, BufReader};
use std::num;
use tracing::{debug, error};

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
#[derive(Debug)]
pub enum KafkaConfigError {
    Io(io::Error),
    Property(java_properties::PropertiesError),
    ParseInt(num::ParseIntError),
    MissingKeys(Vec<String>),
    InvalidValue(String),
    UnknownKey(String),
    DuplicateKey(String),
}

impl From<num::ParseIntError> for KafkaConfigError {
    fn from(err: num::ParseIntError) -> KafkaConfigError {
        KafkaConfigError::ParseInt(err)
    }
}

impl From<io::Error> for KafkaConfigError {
    fn from(err: io::Error) -> KafkaConfigError {
        KafkaConfigError::Io(err)
    }
}

impl From<java_properties::PropertiesError> for KafkaConfigError {
    fn from(err: java_properties::PropertiesError) -> KafkaConfigError {
        KafkaConfigError::Property(err)
    }
}

impl error::Error for KafkaConfigError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            KafkaConfigError::Io(ref err) => Some(err),
            KafkaConfigError::Property(ref err) => Some(err),
            KafkaConfigError::ParseInt(ref err) => Some(err),
            _ => None,
        }
    }
}
impl fmt::Display for KafkaConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            KafkaConfigError::Io(ref err) => write!(f, "IO error: {}", err),
            KafkaConfigError::ParseInt(ref err) => write!(f, "Parse error: {}", err),
            KafkaConfigError::Property(ref err) => write!(f, "Property error: {}", err),
            KafkaConfigError::MissingKeys(ref err) => write!(f, "Missing Key error: {:?}", err),
            KafkaConfigError::InvalidValue(ref err) => write!(f, "Invalid Value: {}", err),
            KafkaConfigError::UnknownKey(ref err) => write!(f, "Unknown Key: {}", err),
            KafkaConfigError::DuplicateKey(ref err) => write!(f, "Duplicate Key: {}", err),
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
        default: None,
        provided: false,
    });
    res.insert(String::from("zookeeper.connection.timeout.ms"), KafkaConfigDef {
        key: String::from("zk_connection_timeout_ms"),
        importance: KafkaConfigDefImportance::High,
        doc: String::from(
            "The max time that the client waits to establish a connection to zookeeper. If not \
             set, the value in zookeeper.session.timeout.ms is used", // REQ-01
        ),
        default: Some(String::from("zk_session_timeout_ms")),
        provided: false,
    });
    res.insert(String::from("zookeeper.sync.time.ms"), KafkaConfigDef {
        key: String::from("zk_sync_time_ms"),
        importance: KafkaConfigDefImportance::Low,
        doc: String::from("How far a ZK follower can be behind a ZK leader"),
        default: None,
        provided: false,
    });
    res.insert(String::from("log.dir"), KafkaConfigDef {
        key: String::from("log_dirs"),
        importance: KafkaConfigDefImportance::High,
        doc: String::from(
            "The directory in which the log data is kept (supplemental for log.dirs property)",
        ),
        default: None,
        provided: false,
    });
    res.insert(String::from("log.dirs"), KafkaConfigDef {
        key: String::from("log_dirs"),
        importance: KafkaConfigDefImportance::High,
        doc: String::from(
            "The directories in which the log data is kept. If not set, the value in log.dir is \
             used",
        ),
        default: Some(String::from("log.dir")),
        provided: false,
    });
    res
}

#[derive(Debug, Default)]
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

impl KafkaConfigBuilder {
    pub fn set_config_key_as_provided(
        &mut self,
        key: &'static str,
    ) -> Result<(), KafkaConfigError> {
        if let Some(config_def) = self.config_definition.get_mut(key) {
            config_def.provided = true;
        }
    }

    /// `try_from_property_to_u32` transform a string property into a destination field from
    /// KafkaConfig fileds
    pub fn try_from_property_to_u32(
        &mut self,
        property_name: &String,
        property_value: &String,
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

    /// Transforms from a HashMap of configs into a KafkaConfigBuilder object
    pub fn from_properties_hashmap(
        self,
        input_config: HashMap<String, String>,
    ) -> Result<Self, KafkaConfigError> {
        let mut config_builder = KafkaConfigBuilder::default();
        let mut zk_connection_timeout_ms: Option<u32> = None;
        for (property, property_value) in &input_config {
            debug!("from_properties_hashmap: {} = {}", property, property_value);
            match property.as_str() {
                "zookeeper.connect" => config_builder.zk_connect = Some(property_value.clone()),
                "zookeeper.session.timeout.ms" => {
                    config_builder.zk_session_timeout_ms =
                        Some(config_builder.try_from_property_to_u32(property, property_value)?);
                },
                "zookeeper.connection.timeout.ms" => {
                    config_builder.zk_connection_timeout_ms =
                        Some(config_builder.try_from_property_to_u32(property, property_value)?);
                },
                "log.dirs" => {
                    config_builder.log_dirs =
                        Some(property_value.clone().split(',').map(|x| x.to_string()).collect())
                },
                "log.dir" => config_builder.log_dir = Some(property_value.clone()),
                _ => return Err(KafkaConfigError::UnknownKey(property.to_string())),
            }
        }
        Ok(config_builder)
    }

    /// `resolve_zk_connection_timeout_ms` Satisties REQ-01, if
    fn resolve_zk_connection_timeout_ms(&mut self) -> Result<(), KafkaConfigError> {
        if self.zk_connection_timeout_ms.is_some() {
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

    /// `build` validates and resolves dependant properties from a KafkaConfigBuilder into a
    /// KafkaConfig
    pub fn build(&mut self) -> Result<KafkaConfig, KafkaConfigError> {
        let mut kafka_config = KafkaConfig::default();
        self.resolve_zk_connection_timeout_ms();
        let mut missing_keys: Vec<String> = vec![];
        for (property, property_def) in &self.config_definition {
            if KafkaConfigDefImportance::High == property_def.importance && !property_def.provided {
                missing_keys.push(property.to_string());
            }
        }
        if missing_keys.len() > 0 {
            return Err(KafkaConfigError::MissingKeys(missing_keys));
        }
        Ok(kafka_config)
    }
}
#[derive(Debug)]
pub struct KafkaConfig {
    pub zk_connect: String,
    pub zk_session_timeout_ms: u32,
    pub zk_sync_time_ms: u32,
    pub zk_connection_timeout_ms: u32,
    pub zk_max_in_flight_requests: u32,
    pub log_dirs: Vec<String>,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        KafkaConfig {
            zk_session_timeout_ms: 18000u32,
            zk_sync_time_ms: 2000u32,
            zk_connection_timeout_ms: 18000u32,
            zk_max_in_flight_requests: 10u32,
            zk_connect: String::from(""),
            log_dirs: vec![String::from("/tmp/kafka-logs")],
            config_definition: gen_kafka_config_definition(),
        }
    }
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
    /// `read_config_from` is the main entry point for configuration.
    pub fn read_config_from(
        filename: &String,
    ) -> Result<HashMap<String, String>, KafkaConfigError> {
        let mut config_file_content = File::open(&filename)?;
        read(BufReader::new(config_file_content)).map_err(|err| KafkaConfigError::Property(err))
    }

    /// `get_kafka_config` Reads the kafka config.
    pub fn get_kafka_config(filename: &String) -> Result<Self, KafkaConfigError> {
        let input_config = KafkaConfig::read_config_from(filename)?;
        debug!("read_config_from: {}", filename);
        KafkaConfig::from_properties_hashmap(input_config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_gets_config_from_hashmap() {
        let mut kafka_config = KafkaConfig::default();
        // Property(java_properties::PropertiesError),
        // ParseInt(num::ParseIntError),
        // MissingKeys(String),
        // InvalidValue(String),
        // UnknownKey(String),
        let mut unknown_key_config: HashMap<String, String> = HashMap::new();
        unknown_key_config.insert(String::from("not.a.known.key"), String::from("127.0.0.1:2181"));
        assert_eq!(
            KafkaConfig::from_properties_hashmap(unknown_key_config),
            KafkaConfigError::UnknownKey(String::from("not.a.known.key"))
        );
        let mut missing_key_config: HashMap<String, String> = HashMap::new();
        missing_key_config
            .insert(String::from("zookeeeper.session.timeout.ms"), String::from("1000"));
        assert_eq!(
            KafkaConfig::from_properties_hashmap(missing_key_config),
            KafkaConfigError::MissingKeys(vec![String::from("zookeeper.connect")])
        );
        let mut full_config: HashMap<String, String> = HashMap::new();
        full_config.insert(String::from("zookeeper.connect"), String::from("127.0.0.1:2181"));
        full_config.insert(String::from("zookeeper.session.timeout.ms"), String::from("1000"));
        full_config.insert(String::from("zookeeper.connection.timeout.ms"), String::from("1000"));
        assert!(KafkaConfig::from_properties_hashmap(full_config).is_ok());
    }
}
