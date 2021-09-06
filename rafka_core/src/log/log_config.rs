//! From core/src/main/scala/kafka/log/LogConfig.scala

use crate::common::config::topic_config::{
    MAX_COMPACTION_LAG_MS_CONFIG, MAX_COMPACTION_LAG_MS_DOC, MIN_COMPACTION_LAG_MS_CONFIG,
    MIN_COMPACTION_LAG_MS_DOC,
};
use crate::server::kafka_config::KafkaConfig;
use crate::{common::config_def::ConfigDef, server::kafka_config::KafkaConfigError};

#[derive(Debug)]
pub struct LogConfig {
    min_compaction_lag: ConfigDef<u64>,
    max_compaction_lag: ConfigDef<u64>,
}

impl Default for LogConfig {
    fn default() -> Self {
        // TODO: MISSING DEFAULTS
        Self {
            min_compaction_lag: ConfigDef::default()
                .with_key(MIN_COMPACTION_LAG_MS_CONFIG)
                .with_doc(MIN_COMPACTION_LAG_MS_DOC.to_string()),
            max_compaction_lag: ConfigDef::default()
                .with_key(MAX_COMPACTION_LAG_MS_CONFIG)
                .with_doc(MAX_COMPACTION_LAG_MS_DOC.to_string()),
        }
    }
}

impl LogConfig {
    pub fn validate_values(&self) -> Result<(), KafkaConfigError> {
        if let (Some(min), Some(max)) =
            (self.min_compaction_lag.get_value(), self.max_compaction_lag.get_value())
        {
            if *min > *max {
                return Err(KafkaConfigError::InvalidValue(format!(
                    "conflict topic config setting {} ({}) > {} ({})",
                    &self.min_compaction_lag.key, min, &self.max_compaction_lag.key, max,
                )));
            }
        }
        Ok(())
    }

    pub fn from_kafka_config(kafka_config: &KafkaConfig) -> Result<Self, KafkaConfigError> {
        unimplemented!()
    }
}
