//! From core/src/main/scala/kafka/log/LogManager.scala
//! The LogManager stores log files in the `log.dirs`, new logs are stored in the data directory
//! with the fewer logs. Once a log is created, it won't be automatically balanced either for I/O
//! speed reasons or disk space exhausted.

use crate::log::cleaner_config::CleanerConfig;
use crate::log::log_config::{LogConfig, LogConfigProperties};
use crate::majordomo::{AsyncTask, AsyncTaskError};
use crate::server::broker_states::BrokerState;
use crate::server::kafka_config::{ConfigSet, KafkaConfig};
use crate::utils::kafka_scheduler::KafkaScheduler;
use crate::zk::kafka_zk_client::KafkaZkClient;
use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::time::Instant;
use thiserror::Error;
use tokio::sync::mpsc;

use super::log_cleaner::LogCleaner;

pub const RECOVERY_POINT_CHECKPOINT_FILE: &str = "recovery-point-offset-checkpoint";
pub const LOG_START_OFFSET_CHECKPOINT_FILE: &str = "log-start-offset-checkpoint";
#[derive(Debug)]
pub struct Scheduler;

#[derive(Debug, Error)]
pub enum LogManagerError {
    FailedLogConfigs(HashMap<std::string::String, AsyncTaskError>),
}

#[derive(Debug)]
pub struct LogManager {
    pub producer_id_expiration_check_interval_ms: u64,
    log_dirs: Vec<PathBuf>,
    initial_offline_dirs: Vec<PathBuf>,
    topic_configs: HashMap<String, LogConfig>, // note that this doesn't get updated after creation
    initial_default_config: LogConfig,
    cleaner_config: CleanerConfig,
    recovery_threads_per_data_dir: i32,
    flush_check_ms: i64,
    flush_recovery_offset_checkpoint_ms: i32,
    flush_start_offset_checkpoint_ms: i32,
    retention_check_ms: i64,
    max_pid_expiration_ms: i64,
    // RAFKA TODO: scheduler used to be a Trait Schedule, double check
    scheduler: KafkaScheduler,
    broker_state: BrokerState,
    // log_dir_failure_channel: LogDirFailureChannel, This may work with just the Majordomo tx.
    majordomo_tx: mpsc::Sender<AsyncTask>,
    time: Instant,
    lock_file: String,
}

impl fmt::Display for LogManagerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LogManagerError({:?})", self)
    }
}

impl Default for LogManager {
    fn default() -> Self {
        let kafka_config = KafkaConfig::default();
        let (majordomo_tx, _majordomo_rx) = mpsc::channel(4_096); // TODO: Magic number removal
        Self {
            producer_id_expiration_check_interval_ms: 10 * 60 * 1000,
            log_dirs: vec![],
            initial_offline_dirs: vec![],
            topic_configs: HashMap::new(),
            initial_default_config: LogConfig::default(),
            cleaner_config: CleanerConfig::default(),
            recovery_threads_per_data_dir: kafka_config.log.num_recovery_threads_per_data_dir,
            flush_check_ms: kafka_config.log.log_flush_scheduler_interval_ms,
            flush_recovery_offset_checkpoint_ms: kafka_config
                .log
                .log_flush_offset_checkpoint_interval_ms,
            flush_start_offset_checkpoint_ms: kafka_config
                .log
                .log_flush_start_offset_checkpoint_interval_ms,
            retention_check_ms: kafka_config.log.log_cleanup_interval_ms,
            max_pid_expiration_ms: kafka_config.transaction.transactional_id_expiration_ms,
            scheduler: KafkaScheduler::default(),
            broker_state: BrokerState::default(),
            majordomo_tx,
            time: Instant::now(),
            lock_file: String::from(".lock"),
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
        majordomo_tx: mpsc::Sender<AsyncTask>,
    ) -> Result<Self, AsyncTaskError> {
        // RAFKA NOTE:
        // - broker_topic_stats has been removed for now.
        // - log_dirs was a String of absolute/canonicalized paths before.
        let majordomo_tx_cp = majordomo_tx.clone();
        // read the log configurations from zookeeper
        let (topic_configs, failed) = KafkaZkClient::get_log_configs(
            majordomo_tx.clone(),
            KafkaZkClient::get_all_topics_in_cluster(majordomo_tx_cp).await?,
            &config,
        )
        .await;
        if !failed.is_empty() {
            return Err(AsyncTaskError::LogManager(LogManagerError::FailedLogConfigs(failed)));
        }

        let cleaner_config = LogCleaner::cleaner_config(&config);

        let broker_defaults = LogConfigProperties::try_from(&config)?.build()?;
        Ok(Self {
            log_dirs: config.log.log_dirs.iter().map(|path| PathBuf::from(path)).collect(),
            initial_offline_dirs: initial_offline_dirs
                .iter()
                .map(|path| PathBuf::from(path))
                .collect(),
            topic_configs,
            initial_default_config: broker_defaults,
            cleaner_config,
            recovery_threads_per_data_dir: config.log.num_recovery_threads_per_data_dir,
            flush_check_ms: config.log.log_flush_scheduler_interval_ms,
            flush_recovery_offset_checkpoint_ms: config.log.log_flush_offset_checkpoint_interval_ms,
            flush_start_offset_checkpoint_ms: config
                .log
                .log_flush_start_offset_checkpoint_interval_ms,
            retention_check_ms: config.log.log_cleanup_interval_ms,
            max_pid_expiration_ms: config.transaction.transactional_id_expiration_ms,
            scheduler: kafka_scheduler,
            broker_state: broker_state.clone(),
            majordomo_tx: majordomo_tx.clone(),
            time,
            ..Default::default()
        })
    }
}
