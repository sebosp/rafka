//! From core/src/main/scala/kafka/log/LogManager.scala
//! The LogManager stores log files in the `log.dirs`, new logs are stored in the data directory
//! with the fewer logs. Once a log is created, it won't be automatically balanced either for I/O
//! speed reasons or disk space exhausted.

use crate::common::topic_partition::TopicPartition;
use crate::log::cleaner_config::CleanerConfig;
use crate::log::log::{self, Log};
use crate::log::log_config::{LogConfig, LogConfigProperties};
use crate::majordomo::{AsyncTask, AsyncTaskError, MajordomoCoordinator};
use crate::server::broker_states::BrokerState;
use crate::server::checkpoints::checkpoint_file::TopicPartitionCheckpointFileError;
use crate::server::checkpoints::offset_checkpoint_file::OffsetCheckpointFile;
use crate::server::kafka_config::{ConfigSet, KafkaConfig};
use crate::server::log_failure_channel::LogDirFailureChannelAsyncTask;
use crate::utils::kafka_scheduler::KafkaScheduler;
use crate::zk::kafka_zk_client::KafkaZkClient;
use file_lock::{FileLock, FileOptions};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::path::PathBuf;
use std::time::Instant;
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::{debug, error, info, trace, warn};

use super::log_cleaner::LogCleaner;

pub const RECOVERY_POINT_CHECKPOINT_FILE: &str = "recovery-point-offset-checkpoint";
pub const LOG_START_OFFSET_CHECKPOINT_FILE: &str = "log-start-offset-checkpoint";

pub const DEFAULT_PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS: u64 = 10 * 60 * 1000;
pub const DEFAULT_LOCK_FILE: &str = ".lock";
pub const INITIAL_TASK_DELAY_MS: i32 = 30 * 1000;

#[derive(Debug)]
pub struct Scheduler;

#[derive(Debug, Error)]
pub enum LogManagerError {
    #[error("Failed Log Configs: {0:?}")]
    FailedLogConfigs(HashMap<std::string::String, AsyncTaskError>),
    #[error("Failed To Load Direcotry During Startup: {0}")]
    FailedToLoadDuringStartup(String),
    #[error("Not a Directory: {0}")]
    NotADirectory(String),
    #[error("Duplicate Log Directory: {0}")]
    DuplicateLogDirectory(String),
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error(
        "Failed to acquire lock on file in {0}. Another kafka process may be using that log-dir"
    )]
    AcquireLock(String),
    #[error("Topic Partition Checkpoint File: {0}")]
    TopicPartitionCheckpointFile(#[from] TopicPartitionCheckpointFileError),
}

#[derive(Debug)]
pub struct LogManager {
    pub producer_id_expiration_check_interval_ms: u64,
    log_dirs: Vec<PathBuf>,
    live_log_dirs: Vec<PathBuf>,
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
    dir_locks: Vec<FileLock>,
    recovery_point_checkpoints: HashMap<PathBuf, OffsetCheckpointFile>,
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
            live_log_dirs: vec![],
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
            producer_id_expiration_check_interval_ms:
                DEFAULT_PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS,
            lock_file: DEFAULT_LOCK_FILE.to_string(),
            dir_locks: vec![],
            recovery_point_checkpoints: HashMap::new(),
        })
    }

    pub fn create_checkpoint_file(
        &self,
        file_name: &str,
    ) -> HashMap<PathBuf, OffsetCheckpointFile> {
        self.live_log_dirs
            .iter()
            .map(|dir| {
                let checkpoint_file = PathBuf::from(format!("{}/{}", dir.display(), file_name));
                (
                    dir.clone(),
                    OffsetCheckpointFile::new(
                        checkpoint_file,
                        self.majordomo_tx.clone(),
                        dir.clone(),
                    ),
                    /* The OffsetCheckpointFile may fail due to io error, RAFKA TODO: maybe add
                     * offline dir? */
                )
            })
            .filter(|x| x.1.is_ok())
            .map(|x| (x.0, x.1.unwrap()))
            .collect()
    }

    pub async fn init(&mut self) -> Result<(), AsyncTaskError> {
        let log_creation_or_deletion_lock: Option<()> = None; // XXX: Was set as object, figure out what to d
        let current_logs: HashMap<TopicPartition, Log> = HashMap::new();
        // The logs being moved across kafka (as with partition reassigment) contain a '-future',
        // later on when they catch up with partitions they would be removed the '-future'
        let future_logs: HashMap<TopicPartition, Log> = HashMap::new();
        let logs_to_be_deleted: HashMap<Log, u64> = HashMap::new();

        self.create_and_validate_live_log_dirs().await?;
        let current_default_config = self.initial_default_config.clone();
        let num_recovery_threads_per_data_dir = self.recovery_threads_per_data_dir;
        self.lock_log_dirs().await?;
        self.recovery_point_checkpoints =
            self.create_checkpoint_file(RECOVERY_POINT_CHECKPOINT_FILE);
        let log_start_offset_checkpoints: HashMap<PathBuf, OffsetCheckpointFile> =
            self.create_checkpoint_file(LOG_START_OFFSET_CHECKPOINT_FILE);
        let preferred_log_dirs: HashMap<TopicPartition, String> = HashMap::new();
        self.load_logs().await?;
        Ok(())
    }

    fn load_dir_logs(&self, dir: &PathBuf) -> Result<(), io::Error> {
        if dir.is_dir() {
            for dir_content in fs::read_dir(dir)? {
                let path = dir_content?.path();
                if path.is_dir() {}
            }
        }
        Ok(())
    }

    /// Recovers and loads all the logs from the log_dirs data directories
    async fn load_logs(&mut self) -> Result<(), AsyncTaskError> {
        info!("Loading logs");
        let init_time = self.time;
        let mut offline_dirs: Vec<(PathBuf, io::Error)> = vec![];
        for dir in &self.live_log_dirs {
            let clean_shutdown_file =
                PathBuf::from(format!("{}/{}", dir.display(), log::CLEAN_SHUTDOWN_FILE));
            if clean_shutdown_file.exists() {
                debug!(
                    "Clean shutdown file exists: {} Skipping recovery of logs.",
                    clean_shutdown_file.display()
                );
            } else {
                // log recovery itself is being performed by `Log` class during initialization
                // RAKFA TODO: Should this be broadcasted to the MajordomoCoordinator ?
                self.broker_state = BrokerState::RecoveringFromUncleanShutdown;
            }
            let mut recovery_points: HashMap<TopicPartition, i64> = HashMap::new();
            match self.recovery_point_checkpoints.get(dir) {
                Some(val) => match val.read() {
                    Ok(val) => {
                        recovery_points = val;
                    },
                    Err(err) => {
                        warn!(
                            "Error reading recovery-point-offset-checkpoint dir {}: {:?}. \
                             Resetting the recovery checkpoint to 0.",
                            dir.display(),
                            err
                        );
                    },
                },
                None => {
                    unreachable!();
                },
            };
            if let Err(err) = self.load_dir_logs(&dir) {
                offline_dirs.push((dir.clone(), err));
            }
        }
        Ok(())
    }

    /// Creates and validates the directories that are not offline.
    /// Directories must not be duplicated and must be readable
    /// RAFKA TODO: When a directory is invalidated it is still in self.log_dirs, should be cleaned
    /// maybe?
    async fn create_and_validate_live_log_dirs(&mut self) -> Result<(), AsyncTaskError> {
        let mut live_log_dirs = vec![];
        let mut canonical_paths: HashSet<String> = HashSet::new();
        for dir in self.log_dirs.clone() {
            let dir_absolute_path = match dir.canonicalize() {
                Ok(val) => val.display().to_string(),
                Err(err) => {
                    LogDirFailureChannelAsyncTask::send_maybe_add_offline_log_dir(
                        self.majordomo_tx.clone(),
                        dir.display().to_string(),
                        LogManagerError::Io(err),
                    )
                    .await?;
                    continue;
                },
            };
            if self.initial_offline_dirs.contains(&dir) {
                LogDirFailureChannelAsyncTask::send_maybe_add_offline_log_dir(
                    self.majordomo_tx.clone(),
                    dir_absolute_path.clone(),
                    LogManagerError::FailedToLoadDuringStartup(dir_absolute_path),
                )
                .await?;
                continue;
            }
            if !dir.exists() {
                info!("Log directory {dir_absolute_path} not found, creating it.");
                match fs::create_dir(&dir) {
                    Err(err) => {
                        LogDirFailureChannelAsyncTask::send_maybe_add_offline_log_dir(
                            self.majordomo_tx.clone(),
                            dir_absolute_path,
                            LogManagerError::Io(err),
                        )
                        .await?;
                        continue;
                    },
                    Ok(_) => trace!("Successfully created dir: {dir_absolute_path}"),
                };
            }
            if !dir.is_dir() {
                error!("dir: {dir_absolute_path} not a directory");
                LogDirFailureChannelAsyncTask::send_maybe_add_offline_log_dir(
                    self.majordomo_tx.clone(),
                    dir_absolute_path.clone(),
                    LogManagerError::NotADirectory(dir_absolute_path),
                )
                .await?;
                continue;
            }
            if let Err(err) = dir.read_dir() {
                // RAFKA TODO: Check if we can read the directory there may be a better way.
                error!("dir: {dir_absolute_path} unable to read directory");
                LogDirFailureChannelAsyncTask::send_maybe_add_offline_log_dir(
                    self.majordomo_tx.clone(),
                    dir_absolute_path,
                    LogManagerError::Io(err),
                )
                .await?;
                continue;
            }
            if !canonical_paths.insert(dir_absolute_path.clone()) {
                LogDirFailureChannelAsyncTask::send_maybe_add_offline_log_dir(
                    self.majordomo_tx.clone(),
                    dir_absolute_path.clone(),
                    LogManagerError::DuplicateLogDirectory(dir_absolute_path),
                )
                .await?;
            }
            live_log_dirs.push(dir.clone());
        }
        if live_log_dirs.is_empty() {
            error!(
                "Shutting down broker because none of the specified log dirs from {:?} can be \
                 created or validated",
                self.log_dirs
            );
            MajordomoCoordinator::shutdown(self.majordomo_tx.clone()).await;
        }
        Ok(())
    }

    /// Locks all the log directories
    /// RAFKA TODO: When a lock cannot be acquired, the `live_log_dirs` is not updated.
    async fn lock_log_dirs(&mut self) -> Result<(), AsyncTaskError> {
        let mut res = vec![];
        for dir in self.live_log_dirs.clone() {
            let options = FileOptions::new().create(true);

            // Path's canonicalize has been previously validated so we can unwrap() it maybe.
            let dir_absolute_path = dir.canonicalize().unwrap();
            let dir_absolute_path = dir_absolute_path.display();
            let lock_absolute_path = format!("{}/{}", dir_absolute_path, self.lock_file);
            match FileLock::lock(&lock_absolute_path, false, options) {
                Ok(lock) => res.push(lock),
                Err(err) => {
                    self.send_maybe_add_offline_log_dir(
                        dir_absolute_path.to_string(),
                        LogManagerError::AcquireLock(format!(
                            "Error locking directory {}: {} ",
                            &dir_absolute_path,
                            err.to_string()
                        )),
                    )
                    .await?;
                },
            }
        }
        self.dir_locks = res;
        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::server::kafka_config;
    pub fn default_log_manager_test() -> LogManager {
        let kafka_config = kafka_config::tests::default_config_for_test();
        let (majordomo_tx, _majordomo_rx) = mpsc::channel(4_096); // TODO: Magic number removal
        LogManager {
            producer_id_expiration_check_interval_ms:
                DEFAULT_PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS,
            log_dirs: vec![],
            live_log_dirs: vec![],
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
            lock_file: DEFAULT_LOCK_FILE.to_string(),
            dir_locks: vec![],
            recovery_point_checkpoints: HashMap::new(),
        }
    }
}
