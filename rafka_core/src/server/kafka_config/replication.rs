//! Kafka Config - Replication Configuration

use super::{ConfigSet, KafkaConfigError, TrySetProperty};
use crate::common::config_def::{ConfigDef, ConfigDefImportance};
use rafka_derive::ConfigDef;
use std::str::FromStr;
use tracing::trace;

// Config Keys
pub const UNCLEAN_LEADER_ELECTION_ENABLE_PROP: &str = "unclean.leader.election.enable";

// Documentation
pub const UNCLEAN_LEADER_ELECTION_ENABLE_DOC: &str =
    "Indicates whether to enable replicas not in the ISR set to be elected as leader as a last \
     resort, even though doing so may result in data loss";

#[derive(Debug, ConfigDef)]
pub struct ReplicationConfigProperties {
    #[config_def(
        key = UNCLEAN_LEADER_ELECTION_ENABLE_PROP,
        importance = High,
        doc = UNCLEAN_LEADER_ELECTION_ENABLE_DOC,
        default = false,
    )]
    pub unclean_leader_election_enable: ConfigDef<bool>,
}

impl ConfigSet for ReplicationConfigProperties {
    type ConfigType = ReplicationConfig;

    fn resolve(&mut self) -> Result<Self::ConfigType, KafkaConfigError> {
        trace!("ReplicationConfigProperties::resolve()");
        let unclean_leader_election_enable = self.build_unclean_leader_election_enable()?;
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
