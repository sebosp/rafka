//! From core/src/main/scala/kafka/log/LogManager.scala
//! The LogManager stores log files in the `log.dirs`, new logs are stored in the in the directory
//! with the fewer logs. Once a log is created, it won't be automatically balanced either for I/O
//! speed reasons or disk space exhausted.

use crate::server::log_failure_channel::LogDirFailureChannel;
use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use tokio::sync::mpsc;

use crate::log::log_config::LogConfig;
use crate::majordomo::{AsyncTask, AsyncTaskError};
use std::convert::TryFrom;
use std::time::Instant;
use thiserror::Error;

use crate::server::broker_states::BrokerState;
use crate::server::kafka_config::KafkaConfig;
use crate::server::kafka_server::BrokerTopicStats;
use crate::utils::kafka_scheduler::KafkaScheduler;
use crate::zk::kafka_zk_client::KafkaZkClient;

use super::log_cleaner::LogCleaner;

#[derive(Debug)]
pub struct Scheduler;

#[derive(Debug, Error)]
pub enum LogManagerError {
    FailedLogConfigs(Vec<String>),
}

#[derive(Debug)]
pub struct LogManager {
    pub recovery_point_checkpoint_file: String,
    pub log_start_offset_checkpoint_file: String,
    pub producer_id_expiration_check_interval_ms: u64,
    log_dirs: Vec<PathBuf>,
    initial_offline_dirs: Vec<PathBuf>,
    topic_configs: HashMap<String, LogConfig>, // note that this doesn't get updated after creation
    initial_default_config: LogConfig,
    cleaner_config: CleanerConfig,
    recovery_threads_per_data_dir: i32,
    flush_check_ms: i64,
    flush_recovery_offset_checkpoint_ms: i64,
    flush_start_offset_checkpoint_ms: i64,
    retention_check_ms: i64,
    max_pid_expiration_ms: i32,
    scheduler: Scheduler,
    broker_state: BrokerState,
    broker_topic_stats: BrokerTopicStats,
    log_dir_failure_channel: LogDirFailureChannel,
    time: Instant,
    log_cleaner: LogCleaner,
}

impl fmt::Display for LogManagerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LogManagerError({:?})", self)
    }
}

impl Default for LogManager {
    fn default() -> Self {
        Self {
            recovery_point_checkpoint_file: String::from("recovery-point-offset-checkpoint"),
            log_start_offset_checkpoint_file: String::from("log-start-offset-checkpoint"),
            producer_id_expiration_check_interval_ms: 10 * 60 * 1000,
            log_dirs: vec![],
        }
    }
}

impl LogManager {
    pub async fn new(
        config: KafkaConfig,
        initial_offline_dirs: Vec<String>,
        broker_state: &BrokerState,
        kafka_scheduler: KafkaScheduler,
        time: Instant,
        broker_topic_stats: &Option<BrokerTopicStats>,
        majordomo_tx: mpsc::Sender<AsyncTask>,
    ) -> Result<Self, AsyncTaskError> {
        // RAFKA NOTE:
        // - broker_topic_stats is unused for now.
        // - log_dirs was a String of absolute/canonicalized paths before.
        let default_log_config: LogConfig = LogConfig::try_from(config)?;

        let majordomo_tx_cp = majordomo_tx.clone();
        // read the log configurations from zookeeper
        let (topic_configs, failed) = KafkaZkClient::get_log_configs(
            majordomo_tx.clone(),
            KafkaZkClient::get_all_topics_in_cluster(majordomo_tx_cp).await?,
            &default_log_config,
        )
        .await?;
        if !failed.is_empty() {
            return Err(AsyncTaskError::LogManager(LogManagerError::FailedLogConfigs(failed)));
        }

        let cleaner_config = LogCleaner::cleaner_config(config);

        Ok(Self {
            log_dirs: config.log_dirs.iter().map(|path| PathBuf::from(path)).collect(),
            initial_offline_dirs: initial_offline_dirs
                .iter()
                .map(|path| PathBuf::from(path))
                .collect(),
            topic_configs,
            initial_default_config: default_log_config,
            cleaner_config,
            recovery_threads_per_data_dir: config.num_recovery_threads_per_data_dir,
            flush_check_ms: config.log_flush_scheduler_interval_ms,
            flush_recovery_offset_checkpoint_ms: config.log_flush_offset_checkpoint_interval_ms,
            flush_start_offset_checkpoint_ms: config.log_flush_start_offset_checkpoint_interval_ms,
            retention_check_ms: config.log_cleanup_interval_ms,
            kax_pid_expiration_ms: config.transactional_id_expiration_ms,
            scheduler: kafka_scheduler,
            broker_state,
            broker_topic_stats,
            majordomo_tx: majordomo_tx.clone(),
            time,
        })
    }
}
