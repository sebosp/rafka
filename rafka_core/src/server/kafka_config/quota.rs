//! Kafka Config - Quota Configuration
use super::{ConfigSet, KafkaConfigError, TrySetProperty};
use crate::common::config_def::{ConfigDef, ConfigDefImportance};
use crate::server::client_quota_manager;
use enum_iterator::IntoEnumIterator;
use std::fmt;
use std::str::FromStr;

// Config Keys
pub const PRODUCER_QUOTA_BYTES_PER_SECOND_DEFAULT_PROP: &str = "quota.producer.default";
pub const CONSUMER_QUOTA_BYTES_PER_SECOND_DEFAULT_PROP: &str = "quota.consumer.default";
pub const QUOTA_WINDOW_SIZE_SECONDS_PROP: &str = "quota.window.size.seconds";

// Documentation
pub const PRODUCER_QUOTA_BYTES_PER_SECOND_DEFAULT_DOC: &str =
    "DEPRECATED: Used only when dynamic default quotas are not configured for <user, <client-id> \
     or <user, client-id> in Zookeeper. Any consumer distinguished by clientId/consumer group \
     will get throttled if it fetches more bytes than this value per-second";
pub const CONSUMER_QUOTA_BYTES_PER_SECOND_DEFAULT_DOC: &str =
    "DEPRECATED: Used only when dynamic default quotas are not configured for <user, <client-id> \
     or <user, client-id> in Zookeeper. Any consumer distinguished by clientId/consumer group \
     will get throttled if it fetches more bytes than this value per-second";
pub const QUOTA_WINDOW_SIZE_SECONDS_DOC: &str = "The time span of each sample for client quotas";
#[derive(Debug, IntoEnumIterator)]
pub enum QuotaConfigKey {
    ProducerQuotaBytesPerSecondDefault,
    ConsumerQuotaBytesPerSecondDefault,
    QuotaWindowSizeSeconds,
}

impl fmt::Display for QuotaConfigKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ProducerQuotaBytesPerSecondDefault => {
                write!(f, "{}", PRODUCER_QUOTA_BYTES_PER_SECOND_DEFAULT_PROP)
            },
            Self::ConsumerQuotaBytesPerSecondDefault => {
                write!(f, "{}", CONSUMER_QUOTA_BYTES_PER_SECOND_DEFAULT_PROP)
            },
            Self::QuotaWindowSizeSeconds => write!(f, "{}", QUOTA_WINDOW_SIZE_SECONDS_PROP),
        }
    }
}

impl FromStr for QuotaConfigKey {
    type Err = KafkaConfigError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            PRODUCER_QUOTA_BYTES_PER_SECOND_DEFAULT_PROP => {
                Ok(Self::ProducerQuotaBytesPerSecondDefault)
            },
            CONSUMER_QUOTA_BYTES_PER_SECOND_DEFAULT_PROP => {
                Ok(Self::ConsumerQuotaBytesPerSecondDefault)
            },
            QUOTA_WINDOW_SIZE_SECONDS_PROP => Ok(Self::QuotaWindowSizeSeconds),
            _ => Err(KafkaConfigError::UnknownKey(input.to_string())),
        }
    }
}

#[derive(Debug)]
pub struct QuotaConfigProperties {
    producer_quota_bytes_per_second_default: ConfigDef<i64>,
    consumer_quota_bytes_per_second_default: ConfigDef<i64>,
    quota_window_size_seconds: ConfigDef<i32>,
}

impl Default for QuotaConfigProperties {
    fn default() -> Self {
        Self {
            producer_quota_bytes_per_second_default: ConfigDef::default()
                .with_key(PRODUCER_QUOTA_BYTES_PER_SECOND_DEFAULT_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(PRODUCER_QUOTA_BYTES_PER_SECOND_DEFAULT_DOC)
                .with_default(client_quota_manager::QUOTA_BYTES_PER_SECOND_DEFAULT)
                .with_validator(Box::new(|data| {
                    // Safe to unwrap, we have a default
                    ConfigDef::at_least(data, &1, PRODUCER_QUOTA_BYTES_PER_SECOND_DEFAULT_PROP)
                })),
            consumer_quota_bytes_per_second_default: ConfigDef::default()
                .with_key(CONSUMER_QUOTA_BYTES_PER_SECOND_DEFAULT_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(CONSUMER_QUOTA_BYTES_PER_SECOND_DEFAULT_DOC)
                .with_default(client_quota_manager::QUOTA_BYTES_PER_SECOND_DEFAULT)
                .with_validator(Box::new(|data| {
                    // Safe to unwrap, we have a default
                    ConfigDef::at_least(data, &1, CONSUMER_QUOTA_BYTES_PER_SECOND_DEFAULT_PROP)
                })),
            quota_window_size_seconds: ConfigDef::default()
                .with_key(QUOTA_WINDOW_SIZE_SECONDS_PROP)
                .with_importance(ConfigDefImportance::Low)
                .with_doc(QUOTA_WINDOW_SIZE_SECONDS_DOC)
                .with_default(client_quota_manager::QUOTA_WINDOW_SIZE_SECONDS_DEFAULT)
                .with_validator(Box::new(|data| {
                    // Safe to unwrap, we have a default
                    ConfigDef::at_least(data, &1, QUOTA_WINDOW_SIZE_SECONDS_PROP)
                })),
        }
    }
}

impl TrySetProperty for QuotaConfigProperties {
    fn try_set_property(
        &mut self,
        property_name: &str,
        property_value: &str,
    ) -> Result<(), KafkaConfigError> {
        let kafka_config_key = QuotaConfigKey::from_str(property_name)?;
        match kafka_config_key {
            QuotaConfigKey::ConsumerQuotaBytesPerSecondDefault => {
                self.consumer_quota_bytes_per_second_default.try_set_parsed_value(property_value)?
            },
            QuotaConfigKey::ProducerQuotaBytesPerSecondDefault => {
                self.producer_quota_bytes_per_second_default.try_set_parsed_value(property_value)?
            },
            QuotaConfigKey::QuotaWindowSizeSeconds => {
                self.quota_window_size_seconds.try_set_parsed_value(property_value)?
            },
        };
        Ok(())
    }
}

impl ConfigSet for QuotaConfigProperties {
    type ConfigKey = QuotaConfigKey;
    type ConfigType = QuotaConfig;

    fn resolve(&mut self) -> Result<Self::ConfigType, KafkaConfigError> {
        let consumer_quota_bytes_per_second_default =
            self.consumer_quota_bytes_per_second_default.build()?;
        let producer_quota_bytes_per_second_default =
            self.producer_quota_bytes_per_second_default.build()?;
        let quota_window_size_seconds = self.quota_window_size_seconds.build()?;
        Ok(Self::ConfigType {
            consumer_quota_bytes_per_second_default,
            producer_quota_bytes_per_second_default,
            quota_window_size_seconds,
        })
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct QuotaConfig {
    pub consumer_quota_bytes_per_second_default: i64,
    pub producer_quota_bytes_per_second_default: i64,
    pub quota_window_size_seconds: i32,
}

impl Default for QuotaConfig {
    fn default() -> Self {
        let mut config_properties = QuotaConfigProperties::default();
        config_properties.build().unwrap()
    }
}
