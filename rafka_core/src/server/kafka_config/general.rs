//! Kafka Config - General Configuration

use super::{ConfigSet, KafkaConfigError, TrySetProperty};
use crate::common::config::topic_config;
use crate::common::config_def::{ConfigDef, ConfigDefImportance};
use crate::common::record::records;
use const_format::concatcp;
use rafka_derive::ConfigDef;
use std::str::FromStr;
use tracing::trace;

// Config Keys
pub const BROKER_ID_GENERATION_ENABLE_PROP: &str = "broker.id.generation.enable";
pub const RESERVED_BROKER_MAX_ID_PROP: &str = "reserved.broker.max.id";
pub const BROKER_ID_PROP: &str = "broker.id";
pub const MESSAGE_MAX_BYTES_PROP: &str = "message.max.bytes";

// Documentation
pub const BROKER_ID_GENERATION_ENABLE_DOC: &str = concatcp!(
    "Enable automatic broker id generation on the server. When enabled the value configured for ",
    RESERVED_BROKER_MAX_ID_PROP,
    " should be reviewed."
);
pub const RESERVED_BROKER_MAX_ID_DOC: &str =
    concatcp!("Max number that can be used for a ", BROKER_ID_PROP);
pub const BROKER_ID_DOC: &str = concatcp!(
    "The broker id for this server. If unset, a unique broker id will be generated. To avoid \
     conflicts between zookeeper generated broker id's and user configured broker id's, generated \
     broker ids start from ",
    RESERVED_BROKER_MAX_ID_PROP,
    " + 1."
);

pub const MESSAGE_MAX_BYTES_DOC: &str = concatcp!(
    topic_config::MAX_MESSAGE_BYTES_DOC,
    " This can be set per topic with the topic level `",
    topic_config::MAX_MESSAGE_BYTES_CONFIG,
    "` config."
);

#[derive(Debug, ConfigDef)]
pub struct GeneralConfigProperties {
    #[config_def(
        key = BROKER_ID_GENERATION_ENABLE_PROP,
        importance = Medium,
        doc = BROKER_ID_GENERATION_ENABLE_DOC,
        default = true,
    )]
    pub broker_id_generation_enable: ConfigDef<bool>,
    #[config_def(
        key = RESERVED_BROKER_MAX_ID_PROP,
        importance = Medium,
        doc = RESERVED_BROKER_MAX_ID_DOC,
        default = 1000,
        with_validator_fn,
    )]
    pub reserved_broker_max_id: ConfigDef<i32>,
    #[config_def(
        key = BROKER_ID_PROP,
        importance = High,
        doc = BROKER_ID_DOC,
        default = -1,
    )]
    pub broker_id: ConfigDef<i32>,
    #[config_def(
        key = MESSAGE_MAX_BYTES_PROP,
        importance = High,
        doc = MESSAGE_MAX_BYTES_DOC,
        with_default_fn,
        with_validator_fn,
    )]
    pub message_max_bytes: ConfigDef<usize>,
}

impl GeneralConfigProperties {
    pub fn validate_reserved_broker_max_id(&self) -> Result<(), KafkaConfigError> {
        self.reserved_broker_max_id.validate_at_least(0)
    }

    pub fn validate_message_max_bytes(&self) -> Result<(), KafkaConfigError> {
        self.message_max_bytes.validate_at_least(0)
    }

    pub fn default_message_max_bytes() -> usize {
        1024 * 1024 + records::LOG_OVERHEAD
    }
}

impl ConfigSet for GeneralConfigProperties {
    type ConfigType = GeneralConfig;

    fn resolve(&mut self) -> Result<GeneralConfig, KafkaConfigError> {
        trace!("GeneralConfigProperties::resolve()");
        let broker_id_generation_enable = self.build_broker_id_generation_enable()?;
        let reserved_broker_max_id = self.build_reserved_broker_max_id()?;
        let broker_id = self.build_broker_id()?;
        let message_max_bytes = self.build_message_max_bytes()?;
        Ok(GeneralConfig {
            broker_id_generation_enable,
            reserved_broker_max_id,
            broker_id,
            message_max_bytes,
        })
    }

    fn validate_set(&self, cfg: &Self::ConfigType) -> Result<(), KafkaConfigError> {
        if cfg.broker_id_generation_enable {
            if cfg.broker_id < -1 || cfg.broker_id > cfg.reserved_broker_max_id {
                return Err(KafkaConfigError::InvalidValue(format!(
                    "{}: '{}' must be equal or greater than -1 and not greater than {}",
                    BROKER_ID_PROP, cfg.broker_id, RESERVED_BROKER_MAX_ID_PROP
                )));
            }
        } else if cfg.broker_id < 0 {
            return Err(KafkaConfigError::InvalidValue(format!(
                "{}: '{}' must be equal or greater than 0",
                BROKER_ID_PROP, cfg.broker_id
            )));
        }
        Ok(())
    }
}
#[derive(Debug, PartialEq, Clone)]
pub struct GeneralConfig {
    pub broker_id_generation_enable: bool,
    pub reserved_broker_max_id: i32,
    pub broker_id: i32,
    pub message_max_bytes: usize,
}

impl Default for GeneralConfig {
    fn default() -> Self {
        let mut config_properties = GeneralConfigProperties::default();
        config_properties.build().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test_log::test]
    fn it_sets_config() {
        let mut conf_props = GeneralConfigProperties::default();
        let conf = conf_props.build().unwrap();
        assert_eq!(conf.broker_id, -1);
        conf_props.try_set_property(BROKER_ID_PROP, &String::from("1")).unwrap();
        let conf = conf_props.build().unwrap();
        assert_eq!(conf.broker_id, 1);
        conf_props.try_set_property(BROKER_ID_PROP, &String::from("-2")).unwrap();
        let conf_res = conf_props.build();
        assert!(conf_res.is_err());
    }
}
