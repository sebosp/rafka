//! From core/src/main/scala/kafka/log/LogConfig.scala
//!
//!

use crate::common::config::topic_config::MIN_COMPACTION_LAG_MS_CONFIG;
use crate::{common::config_def::ConfigDef, server::kafka_config::KafkaConfigError};

#[derive(Debug)]
pub struct LogConfig {
    min_compaction_lag: ConfigDef<u64>,
    max_compaction_lag: ConfigDef<u64>,
}

impl LogConfig {
    pub fn validate_values(&self) -> Result<(), KafkaConfigError> {
        if let (Some(min), Some(max)) =
            (self.min_compaction_lag.get_value(), self.max_compaction_lag.get_value())
        {
            if min > max {
                return Err(KafkaConfigError::InvalidValue(format!(
                    "conflict topic config setting {} ({}) > ({})",
                    MIN_COMPACTION_LAG_MS_CONFIG,
                    &self.min_compaction_lag.key,
                    &self.max_compaction_lag.key,
                )));
            }
        }
        Ok(())
    }
}
