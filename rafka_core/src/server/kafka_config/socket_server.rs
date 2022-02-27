//! Kafka Config - Socket Server Configuration
use super::{ConfigSet, KafkaConfigError, TrySetProperty};
use crate::cluster::end_point::EndPoint;
use crate::common::config_def::{ConfigDef, ConfigDefImportance};
use crate::utils::core_utils;
use const_format::concatcp;
use enum_iterator::IntoEnumIterator;
use rafka_derive::ConfigDef;
use std::fmt;
use std::str::FromStr;
use tracing::trace;

// Config Keys
pub const PORT_PROP: &str = "port";
pub const HOST_NAME_PROP: &str = "host.name";
pub const LISTENERS_PROP: &str = "listeners";
pub const ADVERTISED_HOST_NAME_PROP: &str = "advertised.host.name";
pub const ADVERTISED_PORT_PROP: &str = "advertised.port";
pub const ADVERTISED_LISTENERS_PROP: &str = "advertised.listeners";
pub const MAX_CONNECTIONS_PROP: &str = "max.connections";

// Documentation
pub const PORT_DOC: &str = concatcp!(
    "DEPRECATED: only used when `listeners` is not set. Use `",
    LISTENERS_PROP,
    "` instead. the port to listen and accept connections on"
);
pub const HOST_NAME_DOC: &str = concatcp!(
    "DEPRECATED: only used when `listeners` is not set. Use `",
    LISTENERS_PROP,
    "` instead. hostname of broker. If this is set, it will only bind to this address. If this is \
     not set, it will bind to all interfaces"
);
pub const LISTENERS_DOC: &str =
    "Listener List - Comma-separated list of URIs we will listen on and the listener names. NOTE: \
     RAFKA does not implement listener security protocols Specify hostname as 0.0.0.0 to bind to \
     all interfaces. Leave hostname empty to bind to default interface. Examples of legal \
     listener lists: PLAINTEXT://myhost:9092,SSL://:9091 \
     CLIENT://0.0.0.0:9092,REPLICATION://localhost:9093";
pub const ADVERTISED_LISTENERS_DOC: &str = concatcp!(
    "Listeners to publish to ZooKeeper for clients to use, if different than the `",
    LISTENERS_PROP,
    "` config property.In IaaS environments, this may need to be different from the interface to \
     which the broker binds. If this is not set, the value for `listeners` will be used. Unlike \
     `listeners` it is not valid to advertise the 0.0.0.0 meta-address "
);
pub const ADVERTISED_HOST_NAME_DOC: &str = concatcp!(
    "DEPRECATED: only used when `",
    ADVERTISED_LISTENERS_PROP,
    "` or `",
    LISTENERS_PROP,
    "` are not set. Use `",
    ADVERTISED_LISTENERS_PROP,
    "` instead. Hostname to publish to ZooKeeper for clients to use. In IaaS environments, this \
     may need to be different from the interface to which the broker binds. If this is not set, \
     it will use the value for `",
    HOST_NAME_PROP,
    "` if configured. Otherwise it will use the value returned from gethostname"
);
pub const ADVERTISED_PORT_DOC: &str = concatcp!(
    "DEPRECATED: only used when `",
    ADVERTISED_LISTENERS_PROP,
    "` or `",
    LISTENERS_PROP,
    "` are not set. Use `",
    ADVERTISED_LISTENERS_PROP,
    "` instead. The port to publish to ZooKeeper for clients to use. In IaaS environments, this \
     may need to be different from the port to which the broker binds. If this is not set, it \
     will publish the same port that the broker binds to."
);

#[derive(Debug, IntoEnumIterator)]
pub enum SocketConfigKey {
    Port,
    HostName,
    Listeners,
    AdvertisedHostName,
    AdvertisedPort,
    AdvertisedListeners,
}
impl fmt::Display for SocketConfigKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Port => write!(f, "{}", PORT_PROP),
            Self::HostName => write!(f, "{}", HOST_NAME_PROP),
            Self::Listeners => write!(f, "{}", LISTENERS_PROP),
            Self::AdvertisedHostName => write!(f, "{}", ADVERTISED_HOST_NAME_PROP),
            Self::AdvertisedPort => write!(f, "{}", ADVERTISED_PORT_PROP),
            Self::AdvertisedListeners => write!(f, "{}", ADVERTISED_LISTENERS_PROP),
        }
    }
}
impl FromStr for SocketConfigKey {
    type Err = KafkaConfigError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            PORT_PROP => Ok(Self::Port),
            HOST_NAME_PROP => Ok(Self::HostName),
            LISTENERS_PROP => Ok(Self::Listeners),
            ADVERTISED_HOST_NAME_PROP => Ok(Self::AdvertisedHostName),
            ADVERTISED_PORT_PROP => Ok(Self::AdvertisedPort),
            ADVERTISED_LISTENERS_PROP => Ok(Self::AdvertisedListeners),
            _ => Err(KafkaConfigError::UnknownKey(input.to_string())),
        }
    }
}

#[derive(Debug, ConfigDef)]
pub struct SocketConfigProperties {
    #[config_def(
        key = PORT_PROP,
        importance = High,
        doc = PORT_DOC,
        default = 9092,
    )]
    port: ConfigDef<i32>,

    #[config_def(
        key = HOST_NAME_PROP,
        importance = High,
        doc = HOST_NAME_DOC,
        default = "",
    )]
    host_name: ConfigDef<String>,

    #[config_def(
        key = LISTENERS_PROP,
        importance = High,
        doc = LISTENERS_DOC,
        no_default_resolver,
        no_default_builder,
    )]
    listeners: ConfigDef<String>,

    #[config_def(
        key = ADVERTISED_HOST_NAME_PROP,
        importance = High,
        doc = ADVERTISED_HOST_NAME_DOC,
    )]
    advertised_host_name: ConfigDef<String>,

    #[config_def(
        key = ADVERTISED_PORT_PROP,
        importance = High,
        doc = ADVERTISED_PORT_DOC,
    )]
    advertised_port: ConfigDef<i32>,

    #[config_def(
        key = ADVERTISED_LISTENERS_PROP,
        importance = High,
        doc = ADVERTISED_LISTENERS_DOC,
        no_default_resolver,
        no_default_builder,
    )]
    advertised_listeners: ConfigDef<String>,
}

impl ConfigSet for SocketConfigProperties {
    type ConfigType = SocketConfig;

    fn resolve(&mut self) -> Result<Self::ConfigType, KafkaConfigError> {
        trace!("SocketConfigProperties::resolve()");
        let port = self.build_port()?;
        let host_name = self.build_host_name()?;
        let listeners = self.build_listeners()?;
        self.advertised_host_name.or_set_fallback(&self.host_name)?;
        self.advertised_port.or_set_fallback(&self.port)?;
        let advertised_host_name = self.build_advertised_host_name()?;
        let advertised_port = self.build_advertised_port()?;
        let advertised_listeners = self.build_advertised_listeners()?;
        Ok(Self::ConfigType {
            port,
            host_name,
            listeners,
            advertised_host_name,
            advertised_port,
            advertised_listeners,
        })
    }
}

impl SocketConfigProperties {
    /// If user did not define advertised listeners, we'll use host:port, if they were not set
    /// either we set listeners
    pub fn resolve_advertised_listeners(&mut self) -> Result<Vec<EndPoint>, KafkaConfigError> {
        if let Some(advertised_listeners) = self.advertised_listeners.get_value() {
            core_utils::listener_list_to_end_points(&advertised_listeners)
        } else if let (Some(advertised_host_name), Some(advertised_port)) =
            (self.advertised_host_name.get_value(), self.advertised_port.get_value())
        {
            core_utils::listener_list_to_end_points(&format!(
                "PLAINTEXT://{}:{}",
                advertised_host_name, advertised_port
            ))
        } else {
            self.resolve_listeners()
        }
    }

    /// If the user did not define listeners but did define host or port, let's use them in backward
    /// compatible way If none of those are defined, we default to PLAINTEXT://:9092
    pub fn resolve_listeners(&mut self) -> Result<Vec<EndPoint>, KafkaConfigError> {
        match self.listeners.get_value() {
            Some(val) => core_utils::listener_list_to_end_points(val),
            None => match (self.host_name.get_value(), self.port.get_value()) {
                (Some(host_name), Some(port)) => core_utils::listener_list_to_end_points(&format!(
                    "PLAINTEXT://{}:{}",
                    host_name, port
                )),
                (Some(_host_name), None) => {
                    Err(KafkaConfigError::MissingKey(PORT_PROP.to_string()))
                },
                (None, Some(_port)) => {
                    Err(KafkaConfigError::MissingKey(HOST_NAME_PROP.to_string()))
                },
                (None, None) => {
                    Err(KafkaConfigError::MissingKey(format!("{}, {}", HOST_NAME_PROP, PORT_PROP)))
                },
            },
        }
    }

    pub fn build_listeners(&mut self) -> Result<Vec<EndPoint>, KafkaConfigError> {
        // there's no validator.
        self.resolve_listeners()
    }

    pub fn build_advertised_listeners(&mut self) -> Result<Vec<EndPoint>, KafkaConfigError> {
        // there's no validator.
        self.resolve_advertised_listeners()
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct SocketConfig {
    pub port: i32,
    pub host_name: String,
    pub listeners: Vec<EndPoint>,
    pub advertised_host_name: String,
    pub advertised_port: i32,
    pub advertised_listeners: Vec<EndPoint>,
}

impl Default for SocketConfig {
    fn default() -> Self {
        trace!("SocketConfig::default()");
        let mut config_properties = SocketConfigProperties::default();
        config_properties.build().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_resolves_advertised_defaults() {
        let host = "fake-host";
        let port = 9999;

        let mut conf_props = SocketConfigProperties::default();
        conf_props.try_set_property("host.name", host).unwrap();
        conf_props.try_set_property("port", &port.to_string()).unwrap();

        let conf = conf_props.build().unwrap();
        assert_eq!(conf.advertised_listeners[0].host, host);
        assert_eq!(conf.advertised_listeners[0].port, port);
    }

    #[test]
    fn it_resolves_advertised_configured() {
        let host = "routable-host";
        let port = 1234;

        let mut conf_props = SocketConfigProperties::default();
        conf_props.try_set_property("advertised.host.name", host).unwrap();
        conf_props.try_set_property("advertised.port", &port.to_string()).unwrap();

        let conf = conf_props.build().unwrap();
        assert_eq!(conf.advertised_listeners[0].host, host);
        assert_eq!(conf.advertised_listeners[0].port, port);
    }

    #[test]
    fn it_resolves_advertised_port_default() {
        let advertised_host = "routable-host";
        let port = 9999;

        let mut conf_props = SocketConfigProperties::default();
        conf_props.try_set_property("advertised.host.name", advertised_host).unwrap();
        conf_props.try_set_property("port", &port.to_string()).unwrap();

        let conf = conf_props.build().unwrap();
        assert_eq!(conf.advertised_listeners[0].host, advertised_host);
        assert_eq!(conf.advertised_listeners[0].port, port);
    }

    #[test]
    fn it_resolves_advertised_hostname_default() {
        let host = "routable-host";
        let advertised_port = 9999;

        let mut conf_props = SocketConfigProperties::default();
        conf_props.try_set_property("host.name", host).unwrap();
        conf_props.try_set_property("advertised.port", &advertised_port.to_string()).unwrap();

        let conf = conf_props.build().unwrap();
        assert_eq!(conf.advertised_listeners[0].host, host);
        assert_eq!(conf.advertised_listeners[0].port, advertised_port);
    }

    #[test]
    fn it_identifies_duplicate_listeners() {
        let mut conf_props = SocketConfigProperties::default();

        // listeners with duplicate port
        conf_props
            .try_set_property("listeners", "PLAINTEXT://localhost:9091,TRACE://localhost:9091")
            .unwrap();
        assert!(conf_props.build().is_err());

        // listeners with duplicate proto
        conf_props
            .try_set_property("listeners", "PLAINTEXT://localhost:9091,PLAINTEXT://localhost:9092")
            .unwrap();
        assert!(conf_props.build().is_err());

        let mut conf_props = SocketConfigProperties::default();
        // advertised liteners with uplicate port
        conf_props
            .try_set_property(
                "advertised.listeners",
                "PLAINTEXT://localhost:9091,TRACE://localhost:9091",
            )
            .unwrap();
        assert!(conf_props.build().is_err());
    }

    #[test]
    fn it_identifies_bad_listener_proto() {
        let mut conf_props = SocketConfigProperties::default();
        conf_props.try_set_property("listeners", "BAD://localhost:0").unwrap();
        assert!(conf_props.build().is_err());
    }
}
