//! Kafka Config - Replication Configuration

use super::{ConfigSet, KafkaConfigError, TrySetProperty};
use crate::common::config_def::{ConfigDef, ConfigDefImportance};
use enum_iterator::IntoEnumIterator;
use std::fmt;
use std::str::FromStr;
use tracing::trace;

// Config Keys
pub const UNCLEAN_LEADER_ELECTION_ENABLE_PROP: &str = "unclean.leader.election.enable";

// Documentation
pub const UNCLEAN_LEADER_ELECTION_ENABLE_DOC: &str =
    "Indicates whether to enable replicas not in the ISR set to be elected as leader as a last \
     resort, even though doing so may result in data loss";

#[derive(Debug, IntoEnumIterator)]
pub enum ReplicationConfigKey {
    UncleanLeaderElectionEnable,
}

impl fmt::Display for ReplicationConfigKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UncleanLeaderElectionEnable => {
                write!(f, "{}", UNCLEAN_LEADER_ELECTION_ENABLE_PROP)
            },
        }
    }
}

impl FromStr for ReplicationConfigKey {
    type Err = KafkaConfigError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            UNCLEAN_LEADER_ELECTION_ENABLE_PROP => Ok(Self::UncleanLeaderElectionEnable),
            _ => Err(KafkaConfigError::UnknownKey(input.to_string())),
        }
    }
}

#[derive(Debug)]
pub struct ReplicationConfigProperties {
    pub unclean_leader_election_enable: ConfigDef<bool>,
}

impl Default for ReplicationConfigProperties {
    fn default() -> Self {
        Self {
            unclean_leader_election_enable: ConfigDef::default()
                .with_key(UNCLEAN_LEADER_ELECTION_ENABLE_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(UNCLEAN_LEADER_ELECTION_ENABLE_DOC)
                .with_default(false),
        }
    }
}

impl TrySetProperty for ReplicationConfigProperties {
    /// `try_from_config_property` transforms a string value from the config into our actual types
    fn try_set_property(
        &mut self,
        property_name: &str,
        property_value: &str,
    ) -> Result<(), KafkaConfigError> {
        let kafka_config_key = ReplicationConfigKey::from_str(property_name)?;
        match kafka_config_key {
            ReplicationConfigKey::UncleanLeaderElectionEnable => {
                self.unclean_leader_election_enable.try_set_parsed_value(property_value)?
            },
        };
        Ok(())
    }
}
impl ConfigSet for ReplicationConfigProperties {
    type ConfigKey = ReplicationConfigKey;
    type ConfigType = ReplicationConfig;

    fn resolve(&mut self) -> Result<Self::ConfigType, KafkaConfigError> {
        trace!("ReplicationConfigProperties::resolve()");
        let unclean_leader_election_enable = self.unclean_leader_election_enable.build()?;
        Ok(Self::ConfigType { unclean_leader_election_enable })
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct ReplicationConfig {
    pub unclean_leader_election_enable: bool,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        let mut config_properties = ReplicationConfigProperties::default();
        config_properties.build().unwrap()
    }
}
