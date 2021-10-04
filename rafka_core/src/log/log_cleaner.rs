//! From rafka_core/src/common/feature/base_version_range.rs
//!

use crate::majordomo::AsyncTaskError;
use crate::server::kafka_config::KafkaConfig;
use super::log_manager::CleanerConfig;
#[derive(Debug)]
pub struct LogCleaner {

}

impl LogCleaner{
    pub fn cleaner_config(config: &KafkaConfig) -> Result<CleanerConfig, AsyncTaskError> {
        CleanerConfig::try_from(config)
    CleanerConfig(num_threads: config.log_cleaner_threads,
      dedupe_buffer_size: config.log_cleaner_dedupe_buffer_size,
      dedupe_buffer_load_factor: config.log_cleaner_dedupe_buffer_load_factor,
      io_buffer_size: config.log_cleaner_io_buffer_size,
      max_message_size: config.message_max_bytes,
      max_io_bytes_per_second: config.log_cleaner_io_max_bytes_per_second,
      jack_off_ms: config.log_cleaner_backoff_ms,
      enable_cleaner: config.log_cleaner_enable)
  }
}
