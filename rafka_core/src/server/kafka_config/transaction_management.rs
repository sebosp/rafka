//! Kafka Config - Transaction Management Configuration

use super::{ConfigSet, KafkaConfigError, TrySetProperty};
use crate::common::config_def::{ConfigDef, ConfigDefImportance};
use crate::coordinator::transaction::transaction_state_manager::TransactionStateManager;
use crate::message::compression_codec::BrokerCompressionCodec;
use rafka_derive::ConfigDef;
use std::str::FromStr;
use tracing::trace;

// Config Keys
pub const TRANSACTIONAL_ID_EXPIRATION_MS_PROP: &str = "transactional.id.expiration.ms";
pub const DEFAULT_COMPRESSION_TYPE: BrokerCompressionCodec =
    BrokerCompressionCodec::gen_producer_compression_codec();

// Documentation
pub const TRANSACTIONAL_ID_EXPIRATION_MS_DOC: &str =
    "The time in ms that the transaction coordinator will wait without receiving any transaction  \
     status updates for the current transaction before expiring its transactional id. This \
     setting also influences producer id expiration - producer ids are expired once this time has \
     elapsed  after the last write with the given producer id. Note that producer ids may expire \
     sooner if the last write from the producer id is deleted due to the topic's retention \
     settings.";

#[derive(Debug, ConfigDef)]
pub struct TransactionConfigProperties {
    #[config_def(
        key = TRANSACTIONAL_ID_EXPIRATION_MS_PROP,
        importance = High,
        doc = TRANSACTIONAL_ID_EXPIRATION_MS_DOC,
        with_default_fn,
        with_validator_fn,
    )]
    transactional_id_expiration_ms: ConfigDef<i64>,
}

impl TransactionConfigProperties {
    // Defaults
    pub fn default_transactional_id_expiration_ms() -> i64 {
        TransactionStateManager::default().default_transactional_id_expiration_ms
    }

    // Custom validators
    pub fn validate_transactional_id_expiration_ms(&self) -> Result<(), KafkaConfigError> {
        self.transactional_id_expiration_ms.validate_at_least(1)
    }
}

impl ConfigSet for TransactionConfigProperties {
    type ConfigType = TransactionConfig;

    fn resolve(&mut self) -> Result<Self::ConfigType, KafkaConfigError> {
        trace!("TransactionConfigProperties::resolve()");
        Ok(Self::ConfigType {
            transactional_id_expiration_ms: self.build_transactional_id_expiration_ms()?,
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
        config_properties.build().unwrap()
    }
}
