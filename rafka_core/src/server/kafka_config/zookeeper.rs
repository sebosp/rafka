//! Kafka Config - Zookeeper Configuration
use super::{ConfigSet, KafkaConfigError, TrySetProperty};
use crate::common::config_def::{ConfigDef, ConfigDefImportance};
use const_format::concatcp;
use rafka_derive::ConfigDef;
use std::str::FromStr;
use tracing::trace;

// Config Keys
pub const ZK_CONNECT_PROP: &str = "zookeeper.connect";
pub const ZK_SESSION_TIMEOUT_PROP: &str = "zookeeper.session.timeout.ms";
pub const ZK_CONNECTION_TIMEOUT_PROP: &str = "zookeeper.connection.timeout.ms";
pub const ZK_MAX_IN_FLIGHT_REQUESTS_PROP: &str = "zookeeper.max.in.flight.requests";

// Documentation
pub const ZK_CONNECT_DOC: &str =
    "Specifies the ZooKeeper connection string in the form <code>hostname:port</code>where host \
     and port are the host and port of a ZooKeeper server. To allow connecting through other \
     ZooKeeper nodes when that ZooKeeper machine is down you can also specify multiple hosts in \
     the form <code>hostname1:port1,hostname2:port2,hostname3:port3</code>. The server can also \
     have a ZooKeeper chroot path as part of its ZooKeeper connection string which puts its data \
     under some path in the global ZooKeeper namespace. For example to give a chroot path of \
     `/chroot/path` you would give the connection string as \
     `hostname1:port1,hostname2:port2,hostname3:port3/chroot/path`.";
pub const ZK_SESSION_TIMEOUT_DOC: &str = "Zookeeper session timeout";
pub const ZK_CONNECTION_TIMEOUT_DOC: &str = concatcp!(
    "The max time that the client waits to establish a connection to zookeeper. If not set, the \
     value in ",
    ZK_SESSION_TIMEOUT_PROP,
    " is used"
);
pub const ZK_MAX_IN_FLIGHT_REQUESTS_DOC: &str = "The maximum number of unacknowledged requests \
                                                 the client will send to Zookeeper before \
                                                 blocking.";

#[derive(Debug, ConfigDef)]
pub struct ZookeeperConfigProperties {
    #[config_def(
        key = ZK_CONNECT_PROP,
        importance = High,
        doc = ZK_CONNECT_DOC,
    )]
    zk_connect: ConfigDef<String>,

    #[config_def(
        key = ZK_SESSION_TIMEOUT_PROP,
        importance = High,
        doc = ZK_SESSION_TIMEOUT_DOC,
        default = 18000,
    )]
    zk_session_timeout_ms: ConfigDef<u32>,

    #[config_def(
        key = ZK_CONNECTION_TIMEOUT_PROP,
        importance = High,
        doc = ZK_CONNECTION_TIMEOUT_DOC,
    )]
    zk_connection_timeout_ms: ConfigDef<u32>,

    #[config_def(
        key = ZK_MAX_IN_FLIGHT_REQUESTS_PROP,
        importance = High,
        doc = ZK_MAX_IN_FLIGHT_REQUESTS_DOC,
        default = 100,
        with_validator_fn,
    )]
    zk_max_in_flight_requests: ConfigDef<u32>,
}

impl ZookeeperConfigProperties {
    pub fn validate_zk_max_in_flight_requests(&self) -> Result<(), KafkaConfigError> {
        self.zk_max_in_flight_requests.validate_at_least(1)
    }
}

impl ConfigSet for ZookeeperConfigProperties {
    type ConfigType = ZookeeperConfig;

    fn resolve(&mut self) -> Result<Self::ConfigType, KafkaConfigError> {
        trace!("ZookeeperConfigProperties::resolve()");
        let zk_connect = self.build_zk_connect()?;
        let zk_session_timeout_ms = self.build_zk_session_timeout_ms()?;
        // Satisties REQ-01, if zk_connection_timeout_ms is unset the value of
        // zk_connection_timeout_ms will be used.
        // RAFKA NOTE: somehow the zk_session_timeout_ms build needs to be called before this,
        // maybe resolve can do it?
        self.zk_connection_timeout_ms.or_set_fallback(&self.zk_session_timeout_ms)?;
        let zk_connection_timeout_ms = self.build_zk_connection_timeout_ms()?;
        let zk_max_in_flight_requests = self.build_zk_max_in_flight_requests()?;
        Ok(Self::ConfigType {
            zk_connect,
            zk_session_timeout_ms,
            zk_connection_timeout_ms,
            zk_max_in_flight_requests,
        })
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct ZookeeperConfig {
    pub zk_connect: String,
    pub zk_session_timeout_ms: u32,
    pub zk_connection_timeout_ms: u32,
    pub zk_max_in_flight_requests: u32,
}

impl Default for ZookeeperConfig {
    fn default() -> Self {
        let mut config_properties = ZookeeperConfigProperties::default();
        config_properties.build().unwrap()
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    pub fn default_config_for_test() -> ZookeeperConfig {
        let mut config_properties = ZookeeperConfigProperties::default();
        config_properties.try_set_property("zookeeper.connect", "UNSET").unwrap();
        config_properties.build().unwrap()
    }

    pub fn default_props_for_test() -> ZookeeperConfigProperties {
        let mut config_properties = ZookeeperConfigProperties::default();
        config_properties.try_set_property("zookeeper.connect", "UNSET").unwrap();
        config_properties
    }
}
