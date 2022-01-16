//! Kafka Config - Transaction Management Configuration

use super::{ConfigSet, KafkaConfigError};
use crate::common::config_def::{ConfigDef, ConfigDefImportance};
use crate::coordinator::transaction::transaction_state_manager::TransactionStateManager;
use crate::message::compression_codec::BrokerCompressionCodec;
use enum_iterator::IntoEnumIterator;
use std::fmt;
use std::str::FromStr;
pub const TRANSACTIONAL_ID_EXPIRATION_MS_PROP: &str = "transactional.id.expiration.ms";
pub const DEFAULT_COMPRESSION_TYPE: BrokerCompressionCodec =
    BrokerCompressionCodec::gen_producer_compression_codec();
use tracing::trace;

#[derive(Debug, IntoEnumIterator)]
pub enum TransactionConfigKey {
    TransactionalIdExpirationMs,
}

impl fmt::Display for TransactionConfigKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TransactionalIdExpirationMs => {
                write!(f, "{}", TRANSACTIONAL_ID_EXPIRATION_MS_PROP)
            },
        }
    }
}

impl FromStr for TransactionConfigKey {
    type Err = KafkaConfigError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            TRANSACTIONAL_ID_EXPIRATION_MS_PROP => Ok(Self::TransactionalIdExpirationMs),
            _ => Err(KafkaConfigError::UnknownKey(input.to_string())),
        }
    }
}
#[derive(Debug)]
pub struct TransactionConfigProperties {
    transactional_id_expiration_ms: ConfigDef<i64>,
}

impl Default for TransactionConfigProperties {
    fn default() -> Self {
        Self {
            transactional_id_expiration_ms: ConfigDef::default()
                .with_key(TRANSACTIONAL_ID_EXPIRATION_MS_PROP)
                .with_importance(ConfigDefImportance::High)
                .with_doc(String::from(
                    "The time in ms that the transaction coordinator will wait without receiving \
                     any transaction  status updates for the current transaction before expiring \
                     its transactional id. This setting also influences producer id expiration - \
                     producer ids are expired once this time has elapsed  after the last write \
                     with the given producer id. Note that producer ids may expire sooner if the \
                     last write from the producer id is deleted due to the topic's retention \
                     settings.",
                ))
                .with_default(
                    TransactionStateManager::default().default_transactional_id_expiration_ms,
                )
                .with_validator(Box::new(|data| {
                    // Safe to unwrap, we have a default
                    ConfigDef::at_least(data, &1, TRANSACTIONAL_ID_EXPIRATION_MS_PROP)
                })),
        }
    }
}

impl ConfigSet for TransactionConfigProperties {
    type ConfigKey = TransactionConfigKey;
    type ConfigType = TransactionConfig;

    fn try_set_property(
        &mut self,
        property_name: &str,
        property_value: &str,
    ) -> Result<(), KafkaConfigError> {
        let kafka_config_key = Self::ConfigKey::from_str(property_name)?;
        match kafka_config_key {
            Self::ConfigKey::TransactionalIdExpirationMs => {
                self.transactional_id_expiration_ms.try_set_parsed_value(property_value)?
            },
        };
        Ok(())
    }

    fn resolve(&mut self) -> Result<Self::ConfigType, KafkaConfigError> {
        trace!("TransactionConfigProperties::resolve()");
        Ok(Self::ConfigType {
            transactional_id_expiration_ms: self.transactional_id_expiration_ms.build()?,
        })
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct TransactionConfig {
    pub transactional_id_expiration_ms: i64,
}
impl Default for TransactionConfig {
    fn default() -> Self {
        let mut config_properties = TransactionConfigProperties::default();
        let transactional_id_expiration_ms =
            config_properties.transactional_id_expiration_ms.build().unwrap();
        Self { transactional_id_expiration_ms }
    }
}
