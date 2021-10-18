//! From rafka_core/src/common/feature/base_version_range.rs

use crate::log::cleaner_config::CleanerConfig;
use crate::server::kafka_config::KafkaConfig;
#[derive(Debug)]
pub struct LogCleaner {}

impl LogCleaner {
    pub fn cleaner_config(config: &KafkaConfig) -> CleanerConfig {
        // RAFKA NOTE: For some reason this is in LogCleaner and not something that calls
        // CleanerConfig. I'll keep it here proxied just for the sake of following the same
        // structure
        CleanerConfig::from(config)
    }
}
