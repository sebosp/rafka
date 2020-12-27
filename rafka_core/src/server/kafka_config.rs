/// Core Kafka Config
/// core/src/main/scala/kafka/server/KafkaConfig.scala
/// Changes:
/// - No SSL for now.
use java_properties::read;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
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
    ( $kafka_config:expr, $input_config:expr, $config_definition:expr, $property:expr, $value:expr, $field:ident ) => {{
        match $config_definition.get_mut($property) {
            Some(property_definition) => match $value.parse::<u32>() {
                Ok(val) => {
                    $kafka_config.$field = val;
                    property_definition.provided = true;
                },
                Err(err) => {
                    error!(
                        "Unable to parse property {} to u32 number: {}. Doc: {}",
                        $value, err, property_definition.doc
                    );
                },
            },
            None => {
                error!("Unknown/Unhandled Configuration Key: {}", $property);
            },
        };
    }};
}

impl KafkaConfig {
    /// `read_config_from` is the main entry point for configuration.
    pub fn read_config_from(
        filename: &String,
    ) -> Result<HashMap<String, String>, java_properties::PropertiesError> {
        let mut config_file_content = File::open(&filename)?;
        read(BufReader::new(config_file_content))
    }

    /// `get_kafka_config` Reads the kafka config.
    pub fn get_kafka_config(filename: &String) -> Result<Self, String> {
        // TODO: Create a ConfigError struct and return it instead of the String
        let input_config = match KafkaConfig::read_config_from(filename) {
            Err(err) => return Err(err.to_string()),
            Ok(val) => val,
        };
        debug!("read_config_from: {}", filename);
        KafkaConfig::from_properties_hashmap(input_config)
    }

    /// Transforms from a HashMap of configs into a KafkaConfig object
    pub fn from_properties_hashmap(input_config: HashMap<String, String>) -> Result<Self, String> {
        let mut kafka_config = KafkaConfig::default();
        let mut config_definition = gen_kafka_config_definition();
        let mut zk_connection_timeout_ms: Option<u32> = None;
        for (property, property_value) in &input_config {
            debug!("from_properties_hashmap: {} = {}", property, property_value);
            match property.as_str() {
                "zookeeper.connect" => kafka_config.zk_connect = property_value.clone(),
                "zookeeper.session.timeout.ms" => from_property_u32!(
                    kafka_config,
                    input_config,
                    config_definition,
                    property,
                    property_value,
                    zk_session_timeout_ms
                ),
                "zookeeper.connection.timeout.ms" => from_property_u32!(
                    kafka_config,
                    input_config,
                    config_definition,
                    property,
                    property_value,
                    zk_connection_timeout_ms
                ),
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
