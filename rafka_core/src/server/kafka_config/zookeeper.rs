//! Kafka Config - Zookeeper Configuration
use super::{ConfigSet, KafkaConfigError};
use crate::common::config_def::{ConfigDef, ConfigDefImportance};
use enum_iterator::IntoEnumIterator;
use std::fmt;
use std::str::FromStr;
use tracing::trace;

pub const ZK_CONNECT_PROP: &str = "zookeeper.connect";
pub const ZK_SESSION_TIMEOUT_PROP: &str = "zookeeper.session.timeout.ms";
pub const ZK_CONNECTION_TIMEOUT_PROP: &str = "zookeeper.connection.timeout.ms";
pub const ZK_MAX_IN_FLIGHT_REQUESTS_PROP: &str = "zookeeper.max.in.flight.requests";

#[derive(Debug, IntoEnumIterator)]
pub enum ZookeeperConfigKey {
    ZkConnect,
    ZkSessionTimeoutMs,
    ZkConnectionTimeoutMs,
    ZkMaxInFlightRequests,
}

impl fmt::Display for ZookeeperConfigKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZkConnect => write!(f, "{}", ZK_CONNECT_PROP),
            Self::ZkSessionTimeoutMs => write!(f, "{}", ZK_SESSION_TIMEOUT_PROP),
            Self::ZkConnectionTimeoutMs => write!(f, "{}", ZK_CONNECTION_TIMEOUT_PROP),
            Self::ZkMaxInFlightRequests => write!(f, "{}", ZK_MAX_IN_FLIGHT_REQUESTS_PROP),
        }
    }
}

impl FromStr for ZookeeperConfigKey {
    type Err = KafkaConfigError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            ZK_CONNECT_PROP => Ok(Self::ZkConnect),
            ZK_SESSION_TIMEOUT_PROP => Ok(Self::ZkSessionTimeoutMs),
            ZK_CONNECTION_TIMEOUT_PROP => Ok(Self::ZkConnectionTimeoutMs),
            ZK_MAX_IN_FLIGHT_REQUESTS_PROP => Ok(Self::ZkMaxInFlightRequests),
            _ => Err(KafkaConfigError::UnknownKey(input.to_string())),
        }
    }
}

#[derive(Debug)]
pub struct ZookeeperConfigProperties {
    zk_connect: ConfigDef<String>,
    zk_session_timeout_ms: ConfigDef<u32>,
    zk_connection_timeout_ms: ConfigDef<u32>,
    zk_max_in_flight_requests: ConfigDef<u32>,
}

impl Default for ZookeeperConfigProperties {
    fn default() -> Self {
        Self {
            zk_connect: ConfigDef::default()
                .with_key(ZK_CONNECT_PROP)
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
                .with_key(ZK_SESSION_TIMEOUT_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(String::from("Zookeeper session timeout"))
                .with_default(18000),
            zk_connection_timeout_ms: ConfigDef::default()
                .with_key(ZK_CONNECTION_TIMEOUT_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(
                    format!("The max time that the client waits to establish a connection to zookeeper. If \
                     not set, the value in {} is used", ZK_SESSION_TIMEOUT_PROP) // REQ-01
                ),
            zk_max_in_flight_requests: ConfigDef::default()
                .with_importance(ConfigDefImportance::High)
                .with_doc(String::from(
                    "The maximum number of unacknowledged requests the client will send to Zookeeper before blocking."
                ))
                .with_default(10)
                .with_validator(Box::new(|data| {
                    // RAFKA TODO: This doesn't make much sense if it's u32...
                    ConfigDef::at_least(data, &1, ZK_MAX_IN_FLIGHT_REQUESTS_PROP)
                    })),
        }
    }
}

impl ConfigSet for ZookeeperConfigProperties {
    type ConfigKey = ZookeeperConfigKey;
    type ConfigType = ZookeeperConfig;

    fn try_set_property(
        &mut self,
        property_name: &str,
        property_value: &str,
    ) -> Result<(), KafkaConfigError> {
        let kafka_config_key = Self::ConfigKey::from_str(property_name)?;
        // vim from enum to match: s/^    \(.*\),/\= "            Self::ConfigKey::" . submatch(1) .
        // " => self." . Snakecase(submatch(1)). ".try_set_parsed_value(property_value)?,"/
        match kafka_config_key {
            Self::ConfigKey::ZkConnect => self.zk_connect.try_set_parsed_value(property_value)?,
            Self::ConfigKey::ZkSessionTimeoutMs => {
                self.zk_session_timeout_ms.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::ZkConnectionTimeoutMs => {
                self.zk_connection_timeout_ms.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::ZkMaxInFlightRequests => {
                self.zk_max_in_flight_requests.try_set_parsed_value(property_value)?
            },
        };
        Ok(())
    }

    fn resolve(&mut self) -> Result<Self::ConfigType, KafkaConfigError> {
        trace!("ZookeeperConfigProperties::resolve()");
        let zk_connect = self.zk_connect.build()?;
        let zk_session_timeout_ms = self.zk_session_timeout_ms.build()?;
        // Satisties REQ-01, if zk_connection_timeout_ms is unset the value of
        // zk_connection_timeout_ms will be used.
        // RAFKA NOTE: somehow the zk_session_timeout_ms build needs to be called before this,
        // maybe resolve can do it?
        self.zk_connection_timeout_ms.get_or_fallback(&self.zk_session_timeout_ms)?;
        let zk_connection_timeout_ms = self.zk_connection_timeout_ms.build()?;
        let zk_max_in_flight_requests = self.zk_max_in_flight_requests.build()?;
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
        let zk_connect = String::from("UNSET");
        let zk_session_timeout_ms = config_properties.zk_session_timeout_ms.build().unwrap();
        config_properties
            .zk_connection_timeout_ms
            .resolve(&config_properties.zk_session_timeout_ms)
            .unwrap();
        let zk_connection_timeout_ms = config_properties.zk_connection_timeout_ms.build().unwrap();
        let zk_max_in_flight_requests =
            config_properties.zk_max_in_flight_requests.build().unwrap();
        Self {
            zk_connect,
            zk_session_timeout_ms,
            zk_connection_timeout_ms,
            zk_max_in_flight_requests,
        }
    }
}
