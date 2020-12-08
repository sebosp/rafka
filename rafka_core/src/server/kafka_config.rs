/// Core Kafka Config
/// core/src/main/scala/kafka/server/KafkaConfig.scala
/// Changes:
/// - No SSL for now.
use java_properties::read;
use std::collections::HashMap;
use tracing::debug;
use std::fs::File;
use std::io::BufReader;

// Unimplemented:
// ZkEnableSecureAcls = false
// ZkSslClientEnable = false
// ZkSslProtocol = "TLSv1.2"
// ZkSslEndpointIdentificationAlgorithm = "HTTPS"
// ZkSslCrlEnable = false
// ZkSslOcspEnable = false

pub enum KafkaConfigDefImportance{
    High,
    Medium,
    Low,
}

pub struct KafkaConfigDef {
    importance: KafkaConfigDefImportance,
    doc: String,
}

fn kafka_config_params() -> HashMap<String, KafkaConfigDef> {
    let mut res = HashMap::new();
    res.insert(
        String::from("zookeeper.connect"),
        KafkaConfigDef{
            importance: KafkaConfigDefImportance::High,
            doc: String::from(r#"
            Specifies the ZooKeeper connection string in the form <code>hostname:port</code> where host and port are the
            host and port of a ZooKeeper server. To allow connecting through other ZooKeeper nodes when that ZooKeeper machine is
            down you can also specify multiple hosts in the form <code>hostname1:port1,hostname2:port2,hostname3:port3</code>.\n
            The server can also have a ZooKeeper chroot path as part of its ZooKeeper connection string which puts its data under some path in the global ZooKeeper namespace.
            For example to give a chroot path of <code>/chroot/path</code> you would give the connection string as <code>hostname1:port1,hostname2:port2,hostname3:port3/chroot/path</code>.
            "#),
        }
    );
    res.insert(
        String::from("ZkSessionTimeoutMsProp"),
        KafkaConfigDef{
            importance: KafkaConfigDefImportance::High,
            doc: String::from("Zookeeper session timeout"),
        }
    );
    res
}

pub struct KafkaConfig {
    zk_session_timeout_ms: u32,
    zk_sync_time_ms: u32,
    zk_max_in_flight_requests: u32,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        KafkaConfig {
            zk_session_timeout_ms: 18000u32,
            zk_sync_time_ms: 2000u32,
            zk_max_in_flight_requests: 10u32,
        }
    }
}

impl KafkaConfig {
    pub fn read_config_from(filename: String) -> Result<Self, java_properties::PropertiesError> {
        let mut config_file_content = File::open(&filename)?;
        let mut kafka_config =  KafkaConfig::default();
        let config_hash = read(BufReader::new(config_file_content))?;
        for (property, value) in &config_hash {
            debug!("read_config_from: {} {} = {}", filename, property, value);
            match property.as_str() {
                "zookeeper.connect" => 
                _ => return Err(java_properties::PropertiesError{ description: String::from("Unknown config key: {}", property));
            }
        }
 /** ********* Zookeeper Configuration ***********
  val ZkConnectProp = "zookeeper.connect"
  val ZkSessionTimeoutMsProp = "zookeeper.session.timeout.ms"
  val ZkConnectionTimeoutMsProp = "zookeeper.connection.timeout.ms"
  val ZkSyncTimeMsProp = "zookeeper.sync.time.ms"
  val ZkMaxInFlightRequestsProp = "zookeeper.max.in.flight.requests"
  */

        Ok(kafka_config)
    }
}
