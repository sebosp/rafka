/// Core Kafka Config
/// core/src/main/scala/kafka/server/KafkaConfig.scala
/// Changes:
/// - No SSL for now.
use java_properties::read;
use std::collections::HashMap;
use std::error;
use std::fmt;
use std::fs::File;
use std::io::{self, BufReader, Write};
use std::num;
use tracing::{debug, error, info};

// Unimplemented:
// ZkEnableSecureAcls = false
// ZkSslClientEnable = false
// ZkSslProtocol = "TLSv1.2"
// ZkSslEndpointIdentificationAlgorithm = "HTTPS"
// ZkSslCrlEnable = false
// ZkSslOcspEnable = false

#[derive(Debug)]
pub enum KafkaConfigDefImportance {
    High,
    Medium,
    Low,
}

#[derive(Debug)]
pub enum KafkaConfigError {
    Io(io::Error),
    Property(java_properties::PropertiesError),
    ParseInt(num::ParseIntError),
    MissingKey(String),
    InvalidValue(String),
    UnknownKey(String),
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
            KafkaConfigError::MissingKey(ref err) => write!(f, "Missing Key error: {}", err),
            KafkaConfigError::InvalidValue(ref err) => write!(f, "Invalid Value: {}", err),
            KafkaConfigError::UnknownKey(ref err) => write!(f, "Unknown Key: {}", err),
        }
    }
}

#[derive(Debug)]
pub struct KafkaConfigDef {
    key: String,
    importance: KafkaConfigDefImportance,
    doc: String,
    default: Option<String>,
    provided: bool,
}

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

pub struct KafkaConfig {
    pub zk_connect: String,
    pub zk_session_timeout_ms: u32,
    pub zk_sync_time_ms: u32,
    pub zk_connection_timeout_ms: u32,
    pub zk_max_in_flight_requests: u32,
    pub log_dirs: Vec<String>,
    // config_definition: HashMap<String, KafkaConfigDef>,
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

    /// Transforms from a HashMap of configs into a KafkaConfig object
    pub fn from_properties_hashmap(
        input_config: HashMap<String, String>,
    ) -> Result<Self, KafkaConfigError> {
        let mut kafka_config = KafkaConfig::default();
        let mut config_definition = gen_kafka_config_definition();
        let mut zk_connection_timeout_ms: Option<u32> = None;
        for (property, property_value) in &input_config {
            debug!("from_properties_hashmap: {} = {}", property, property_value);
            match property.as_str() {
                "zookeeper.connect" => kafka_config.zk_connect = property_value.clone(),
                "zookeeper.session.timeout.ms" => {
                    kafka_config.zk_session_timeout_ms = from_property_u32!(
                        input_config,
                        config_definition,
                        property,
                        property_value
                    )?;
                },
                "zookeeper.connection.timeout.ms" => {
                    kafka_config.zk_connection_timeout_ms = from_property_u32!(
                        input_config,
                        config_definition,
                        property,
                        property_value
                    )?;
                },
                "log.dirs" => {
                    kafka_config.log_dirs =
                        property_value.clone().split(',').map(|x| x.to_string()).collect()
                },
                "log.dir" => kafka_config.log_dirs.push(property_value.clone()),
                _ => return Err(format!("Unknown config key: {}", property)),
            }
        }
        kafka_config.zk_connection_timeout_ms = match zk_connection_timeout_ms {
            Some(val) => val,
            None => {
                debug!("Unspecified property zk_connection_timeout_ms will use default blah blah");
                // Satisties REQ-01
                kafka_config.zk_session_timeout_ms
            },
        };

        Ok(kafka_config)
    }

    pub fn from_hash_u32(
        input_config: &HashMap<String, String>,
        config_definition: HashMap<String, KafkaConfigDef>,
        property_name: String,
        config_name: String,
    ) -> Option<u32> {

    }
}
