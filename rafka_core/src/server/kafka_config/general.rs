//! Kafka Config - General Configuration

use super::KafkaConfigError;
use crate::common::config::topic_config;
use crate::common::config_def::{ConfigDef, ConfigDefImportance};
use crate::common::record::records;
use std::str::FromStr;

pub const BROKER_ID_GENERATION_ENABLED_PROP: &str = "broker.id.generation.enable";
pub const RESERVED_BROKER_MAX_ID_PROP: &str = "reserved.broker.max.id";
pub const BROKER_ID_PROP: &str = "broker.id";
pub const MESSAGE_MAX_BYTES_PROP: &str = "message.max.bytes";

#[derive(Debug)]
pub enum GeneralConfigKey {
    BrokerIdGenerationEnable,
    ReservedBrokerMaxId,
    BrokerId,
    MessageMaxBytes,
}

impl FromStr for GeneralConfigKey {
    type Err = KafkaConfigError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            BROKER_ID_GENERATION_ENABLE_PROP => Ok(Self::BrokerIdGenerationEnable),
            RESERVED_BROKER_MAX_ID_PROP => Ok(Self::ReservedBrokerMaxId),
            BROKER_ID_PROP => Ok(Self::BrokerId),
            MESSAGE_MAX_BYTES_PROP => Ok(Self::MessageMaxBytes),
            _ => Err(KafkaConfigError::UnknownKey(input.to_string())),
        }
    }
}
#[derive(Debug)]
pub struct GeneralConfigProperties {
    pub broker_id_generation_enable: ConfigDef<bool>,
    pub reserved_broker_max_id: ConfigDef<i32>,
    pub broker_id: ConfigDef<i32>,
    pub message_max_bytes: ConfigDef<usize>,
}

impl Default for GeneralConfigProperties {
    fn default() -> Self {
        Self {
            broker_id_generation_enable: ConfigDef::default()
                .with_key(BROKER_ID_GENERATION_ENABLED_PROP)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(format!(
                    "Enable automatic broker id generation on the server. When enabled the value \
                     configured for {} should be reviewed.",
                    RESERVED_BROKER_MAX_ID_PROP
                ))
                .with_default(true),
            reserved_broker_max_id: ConfigDef::default()
                .with_key(RESERVED_BROKER_MAX_ID_PROP)
                .with_importance(ConfigDefImportance::Medium)
                .with_doc(format!("Max number that can be used for a {}", BROKER_ID_PROP))
                .with_default(1000)
                .with_validator(Box::new(|data| {
                    // Safe to unwrap, we have a default
                    ConfigDef::at_least(data, &0, RESERVED_BROKER_MAX_ID_PROP)
                })),
            broker_id: ConfigDef::default()
                .with_key(BROKER_ID_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(format!(
                    "The broker id for this server. If unset, a unique broker id will be \
                     generated. To avoid conflicts between zookeeper generated broker id's and \
                     user configured broker id's, generated broker ids start from {} + 1.",
                    RESERVED_BROKER_MAX_ID_PROP
                ))
                .with_default(-1),
            message_max_bytes: ConfigDef::default()
                .with_key(MESSAGE_MAX_BYTES_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_default(1024 * 1024 + records::LOG_OVERHEAD)
                .with_doc(format!(
                    "{} This can be set per topic with the topic level `{}` config.",
                    topic_config::MAX_MESSAGE_BYTES_DOC,
                    topic_config::MAX_MESSAGE_BYTES_CONFIG
                ))
                .with_validator(Box::new(|data| {
                    // Safe to unwrap, we have a default
                    ConfigDef::at_least(data, &0, MESSAGE_MAX_BYTES_PROP)
                })),
        }
    }
}

impl GeneralConfigProperties {
    /// `try_from_config_property` transforms a string value from the config into our actual types
    pub fn try_set_property(
        &mut self,
        property_name: &str,
        property_value: &str,
    ) -> Result<(), KafkaConfigError> {
        let kafka_config_key = GeneralConfigKey::from_str(property_name)?;
        match kafka_config_key {
            GeneralConfigKey::BrokerIdGenerationEnable => {
                self.broker_id_generation_enable.try_set_parsed_value(property_value)?
            },
            GeneralConfigKey::ReservedBrokerMaxId => {
                self.reserved_broker_max_id.try_set_parsed_value(property_value)?
            },
            GeneralConfigKey::BrokerId => self.broker_id.try_set_parsed_value(property_value)?,
            GeneralConfigKey::MessageMaxBytes => {
                self.message_max_bytes.try_set_parsed_value(property_value)?
            },
        };
        Ok(())
    }

    /// `build` validates and resolves dependant properties from a KafkaConfigProperties into a
    /// KafkaConfig
    pub fn build(&mut self) -> Result<GeneralConfig, KafkaConfigError> {
        let broker_id_generation_enable = self.broker_id_generation_enable.build()?;
        let reserved_broker_max_id = self.reserved_broker_max_id.build()?;
        let broker_id = self.broker_id.build()?;
        let message_max_bytes = self.message_max_bytes.build()?;
        Ok(GeneralConfig {
            broker_id_generation_enable,
            reserved_broker_max_id,
            broker_id,
            message_max_bytes,
        })
    }
}
#[derive(Debug, PartialEq, Clone)]
pub struct GeneralConfig {
    pub broker_id_generation_enable: bool,
    pub reserved_broker_max_id: i32,
    pub broker_id: i32,
    pub message_max_bytes: usize,
}
impl GeneralConfig {
    pub fn validate_values(self) -> Result<Self, KafkaConfigError> {
        if self.broker_id_generation_enable {
            if self.broker_id < -1 || self.broker_id > self.reserved_broker_max_id {
                return Err(KafkaConfigError::InvalidValue(format!(
                    "{}: '{}' must be equal or greater than -1 and not greater than {}",
                    BROKER_ID_PROP, self.broker_id, RESERVED_BROKER_MAX_ID_PROP
                )));
            }
        } else if self.broker_id < 0 {
            return Err(KafkaConfigError::InvalidValue(format!(
                "{}: '{}' must be equal or greater than 0",
                BROKER_ID_PROP, self.broker_id
            )));
        }
        Ok(self)
    }
}
impl Default for GeneralConfig {
    fn default() -> Self {
        // Somehow this should only be allowed for testing...
        let mut config_properties = GeneralConfigProperties::default();
        let broker_id_generation_enable =
            config_properties.broker_id_generation_enable.build().unwrap();
        let reserved_broker_max_id = config_properties.reserved_broker_max_id.build().unwrap();
        let broker_id = config_properties.broker_id.build().unwrap();
        let message_max_bytes = config_properties.message_max_bytes.build().unwrap();
        Self { broker_id_generation_enable, reserved_broker_max_id, broker_id, message_max_bytes }
    }
}
