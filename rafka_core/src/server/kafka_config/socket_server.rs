//! Kafka Config - Socket Server Configuration
use super::{ConfigSet, KafkaConfigError};
use crate::cluster::end_point::EndPoint;
use crate::common::config_def::{ConfigDef, ConfigDefImportance, PartialConfigDef};
use crate::utils::core_utils;
use enum_iterator::IntoEnumIterator;
use std::fmt;
use std::str::FromStr;
use tracing::trace;

pub const PORT_PROP: &str = "port";
pub const HOST_NAME_PROP: &str = "host.name";
pub const LISTENERS_PROP: &str = "listeners";
pub const ADVERTISED_HOST_NAME_PROP: &str = "advertised.host.name";
pub const ADVERTISED_PORT_PROP: &str = "advertised.port";
pub const ADVERTISED_LISTENERS_PROP: &str = "advertised.listeners";
pub const MAX_CONNECTIONS_PROP: &str = "max.connections";

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

#[derive(Debug)]
pub struct SocketConfigProperties {
    port: ConfigDef<i32>,
    host_name: ConfigDef<String>,
    listeners: PartialConfigDef<String>,
    advertised_host_name: ConfigDef<String>,
    advertised_port: ConfigDef<i32>,
    advertised_listeners: PartialConfigDef<String>,
}
impl Default for SocketConfigProperties {
    fn default() -> Self {
        Self {
            port: ConfigDef::default()
                .with_key(PORT_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(format!(
                    "DEPRECATED: only used when `listeners` is not set. Use `{}` instead. the \
                     port to listen and accept connections on",
                    LISTENERS_PROP
                ))
                .with_default(9092),
            host_name: ConfigDef::default()
                .with_key(HOST_NAME_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(format!(
                    "DEPRECATED: only used when `listeners` is not set. Use `{}` instead. \
                     hostname of broker. If this is set, it will only bind to this address. If \
                     this is not set, it will bind to all interfaces",
                    LISTENERS_PROP
                ))
                .with_default(String::from("")),
            listeners: PartialConfigDef::default()
                .with_key(LISTENERS_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(format!(
                    "Listener List - Comma-separated list of URIs we will listen on and the \
                     listener names. NOTE: RAFKA does not implement listener security protocols \
                     Specify hostname as 0.0.0.0 to bind to all interfaces. Leave hostname empty \
                     to bind to default interface. Examples of legal listener lists: \
                     PLAINTEXT://myhost:9092,SSL://:9091 \
                     CLIENT://0.0.0.0:9092,REPLICATION://localhost:9093"
                )),
            advertised_listeners: PartialConfigDef::default()
                .with_key(ADVERTISED_LISTENERS_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(format!(
                    "Listeners to publish to ZooKeeper for clients to use, if different than the \
                     `{}` config property.In IaaS environments, this may need to be different \
                     from the interface to which the broker binds. If this is not set, the value \
                     for `listeners` will be used. Unlike `listeners` it is not valid to \
                     advertise the 0.0.0.0 meta-address ",
                    LISTENERS_PROP
                )),
            advertised_host_name: ConfigDef::default()
                .with_key(ADVERTISED_HOST_NAME_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(format!(
                    "DEPRECATED: only used when `{}` or `{}` are not set. Use `{}` instead. \
                     Hostname to publish to ZooKeeper for clients to use. In IaaS environments, \
                     this may need to be different from the interface to which the broker binds. \
                     If this is not set, it will use the value for `{}` if configured. Otherwise \
                     it will use the value returned from gethostname",
                    ADVERTISED_LISTENERS_PROP,
                    LISTENERS_PROP,
                    ADVERTISED_LISTENERS_PROP,
                    HOST_NAME_PROP
                )),
            advertised_port: ConfigDef::default()
                .with_key(ADVERTISED_PORT_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(format!(
                    "DEPRECATED: only used when `{}` or `{}` are not set. Use `{}` instead. The \
                     port to publish to ZooKeeper for clients to use. In IaaS environments, this \
                     may need to be different from the port to which the broker binds. If this is \
                     not set, it will publish the same port that the broker binds to.",
                    ADVERTISED_LISTENERS_PROP, LISTENERS_PROP, ADVERTISED_LISTENERS_PROP
                )),
        }
    }
}
impl ConfigSet for SocketConfigProperties {
    type ConfigKey = SocketConfigKey;
    type ConfigType = SocketConfig;

    fn try_set_property(
        &mut self,
        property_name: &str,
        property_value: &str,
    ) -> Result<(), KafkaConfigError> {
        let kafka_config_key = Self::ConfigKey::from_str(property_name)?;
        match kafka_config_key {
            Self::ConfigKey::Port => self.port.try_set_parsed_value(property_value)?,
            Self::ConfigKey::HostName => self.host_name.try_set_parsed_value(property_value)?,
            Self::ConfigKey::Listeners => self.listeners.try_set_parsed_value(property_value)?,
            Self::ConfigKey::AdvertisedHostName => {
                self.advertised_host_name.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::AdvertisedPort => {
                self.advertised_port.try_set_parsed_value(property_value)?
            },
            Self::ConfigKey::AdvertisedListeners => {
                self.advertised_listeners.try_set_parsed_value(property_value)?
            },
        };
        Ok(())
    }

    fn resolve(&mut self) -> Result<Self::ConfigType, KafkaConfigError> {
        trace!("SocketConfigProperties::resolve()");
        let port = self.port.build()?;
        let host_name = self.host_name.build()?;
        let listeners = self.resolve_listeners()?;
        self.advertised_host_name.get_or_fallback(&self.host_name)?;
        self.advertised_port.get_or_fallback(&self.port)?;
        let advertised_host_name = self.advertised_host_name.build()?;
        let advertised_port = self.advertised_port.build()?;
        let advertised_listeners = self.resolve_advertised_listeners()?;
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
        let port = config_properties.port.build().unwrap();
        let host_name = config_properties.host_name.build().unwrap();
        config_properties.advertised_host_name.get_or_fallback(&config_properties.host_name).unwrap();
        config_properties.advertised_port.get_or_fallback(&config_properties.port).unwrap();
        let listeners = config_properties.resolve_listeners().unwrap();
        let advertised_host_name = config_properties.advertised_host_name.build().unwrap();
        let advertised_port = config_properties.advertised_port.build().unwrap();
        let advertised_listeners = config_properties.resolve_advertised_listeners().unwrap();
        Self {
            port,
            host_name,
            listeners,
            advertised_host_name,
            advertised_port,
            advertised_listeners,
        }
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

        // Duplicate port
        conf_props.try_set_property("listeners", "PLAINTEXT://localhost:9091,TRACE://localhost:9091").unwrap();
        assert!(conf_props.build().is_err());
    }
}
