//! Kafka Config - Quota Configuration
use super::{ConfigSet, KafkaConfigError, TrySetProperty};
use crate::common::config_def::{ConfigDef, ConfigDefImportance};
use crate::server::client_quota_manager;
use rafka_derive::ConfigDef;
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

#[derive(Debug, ConfigDef)]
pub struct QuotaConfigProperties {
    #[config_def(
        key = PRODUCER_QUOTA_BYTES_PER_SECOND_DEFAULT_PROP,
        importance = High,
        doc = PRODUCER_QUOTA_BYTES_PER_SECOND_DEFAULT_DOC,
        with_default_fn,
        with_validator_fn,
    )]
    producer_quota_bytes_per_second_default: ConfigDef<i64>,

    #[config_def(
        key = CONSUMER_QUOTA_BYTES_PER_SECOND_DEFAULT_PROP,
        importance = High,
        doc = CONSUMER_QUOTA_BYTES_PER_SECOND_DEFAULT_DOC,
        with_default_fn,
        with_validator_fn,
    )]
    consumer_quota_bytes_per_second_default: ConfigDef<i64>,

    #[config_def(
        key = QUOTA_WINDOW_SIZE_SECONDS_PROP,
        importance = Low,
        doc = QUOTA_WINDOW_SIZE_SECONDS_DOC,
        with_default_fn,
        with_validator_fn,
    )]
    quota_window_size_seconds: ConfigDef<i32>,
}

impl QuotaConfigProperties {
    // Defaults

    pub fn default_producer_quota_bytes_per_second_default() -> i64 {
        client_quota_manager::QUOTA_BYTES_PER_SECOND_DEFAULT
    }

    pub fn default_consumer_quota_bytes_per_second_default() -> i64 {
        client_quota_manager::QUOTA_BYTES_PER_SECOND_DEFAULT
    }

    pub fn default_quota_window_size_seconds() -> i32 {
        client_quota_manager::QUOTA_WINDOW_SIZE_SECONDS_DEFAULT
    }

    // Validators

    pub fn validate_producer_quota_bytes_per_second_default(&self) -> Result<(), KafkaConfigError> {
        self.producer_quota_bytes_per_second_default.validate_at_least(1)
    }

    pub fn validate_consumer_quota_bytes_per_second_default(&self) -> Result<(), KafkaConfigError> {
        self.consumer_quota_bytes_per_second_default.validate_at_least(1)
    }

    pub fn validate_quota_window_size_seconds(&self) -> Result<(), KafkaConfigError> {
        self.quota_window_size_seconds.validate_at_least(1)
    }
}

impl ConfigSet for QuotaConfigProperties {
    type ConfigType = QuotaConfig;

    fn resolve(&mut self) -> Result<Self::ConfigType, KafkaConfigError> {
        let consumer_quota_bytes_per_second_default =
            self.build_consumer_quota_bytes_per_second_default()?;
        let producer_quota_bytes_per_second_default =
            self.build_producer_quota_bytes_per_second_default()?;
        let quota_window_size_seconds = self.build_quota_window_size_seconds()?;
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
