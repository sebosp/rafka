//! From core/src/main/scala/kafka/log/LogManager.scala
//! The LogManager stores log files in the `log.dirs`, new logs are stored in the data directory
//! with the fewer logs. Once a log is created, it won't be automatically balanced either for I/O
//! speed reasons or disk space exhausted.

use crate::common::topic_partition::TopicPartition;
use crate::log::cleaner_config::CleanerConfig;
use crate::log::log::{self, Log};
use crate::log::log_config::{LogConfig, LogConfigProperties};
use crate::majordomo::{AsyncTask, AsyncTaskError, CoordinatorTask, MajordomoCoordinator};
use crate::server::broker_states::BrokerState;
use crate::server::checkpoints::checkpoint_file::CheckpointFileError;
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
use tokio::sync::{mpsc, oneshot};

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
    CheckpointFile(#[from] CheckpointFileError),
    #[error("Log Segment Offset Overflow")]
    LogSegmentOffsetOverflow,
}

#[derive(Debug)]
pub struct LogManager {
    pub producer_id_expiration_check_interval_ms: u64,
    log_dirs: Vec<PathBuf>,
    live_log_dirs: Vec<PathBuf>,
    initial_offline_dirs: Vec<PathBuf>,
    cleaner_config: CleanerConfig,
    recovery_threads_per_data_dir: i32,
    flush_check_ms: i64,
    flush_recovery_offset_checkpoint_ms: i32,
    flush_start_offset_checkpoint_ms: i32,
    retention_check_ms: i64,
    max_pid_expiration_ms: i64,
    // RAFKA TODO: scheduler used to be a Trait Schedule, double check
    scheduler: KafkaScheduler,
    // log_dir_failure_channel: LogDirFailureChannel, This may work with just the Majordomo tx.
    majordomo_tx: mpsc::Sender<AsyncTask>,
    time: Instant,
    lock_file: String,
    dir_locks: Vec<FileLock>,
    recovery_point_checkpoints: HashMap<PathBuf, OffsetCheckpointFile>,
    log_start_offset_checkpoints: HashMap<PathBuf, OffsetCheckpointFile>,
}

impl LogManager {
    /// Creates an instance of the LogManager. Note that the initial_offline_dirs is not provided,
    /// KafkaServer will send us the initial_offline_dirs afterwards.
    /// RAFKA TODO: Perhaps add an enum State with Uninitialized/Initialized to understand when the
    /// offline dirs have been received from the KafkaServer thread.
    pub fn new(
        config: KafkaConfig,
        kafka_scheduler: KafkaScheduler,
        time: Instant,
        majordomo_tx: mpsc::Sender<AsyncTask>,
    ) -> Result<Self, AsyncTaskError> {
        // RAFKA NOTE:
        // - broker_topic_stats has been removed for now.
        // - log_dirs was a String of absolute/canonicalized paths before.
        let cleaner_config = LogCleaner::cleaner_config(&config);
        Ok(Self {
            log_dirs: config.log.log_dirs.iter().map(|path| PathBuf::from(path)).collect(),
            live_log_dirs: vec![],
            initial_offline_dirs: vec![],
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
            majordomo_tx: majordomo_tx.clone(),
            time,
            producer_id_expiration_check_interval_ms:
                DEFAULT_PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS,
            lock_file: DEFAULT_LOCK_FILE.to_string(),
            dir_locks: vec![],
            recovery_point_checkpoints: HashMap::new(),
            log_start_offset_checkpoints: HashMap::new(),
        })
    }

    /// The KafkaServer, when it inits, collects a list of offline_dirs, afterwards, it sends the
    /// LogManagerCoordinator the offline_dirs and then it's transferred here.
    pub fn set_initial_offline_dirs(&mut self, initial_offline_dirs: Vec<String>) {
        // RAFKA TODO: Once the offline_dirs have been received we can make progress and check them
        // further. Another option would be to receive the offline dirs from the
        // LogDirFailureChannel.
        self.initial_offline_dirs =
            initial_offline_dirs.iter().map(|path| PathBuf::from(path)).collect();
    }

    /// Creates a file on each [`live_log_dirs`] directory for either a recovery point or a log
    /// start offset.
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

    /// Initializes the LogManager. It validates log.dirs, checkpoint files and starts the log
    /// loading process.
    pub async fn init(&mut self) -> Result<(), AsyncTaskError> {
        let log_creation_or_deletion_lock: Option<()> = None; // XXX: Was set as object, figure out what to d
                                                              // The logs being moved across kafka (as with partition reassigment) contain a '-future',
                                                              // later on when they catch up with partitions they would be removed the '-future'
        self.create_and_validate_live_log_dirs().await?;
        let num_recovery_threads_per_data_dir = self.recovery_threads_per_data_dir;
        self.lock_log_dirs().await?;
        self.recovery_point_checkpoints =
            self.create_checkpoint_file(RECOVERY_POINT_CHECKPOINT_FILE);
        self.log_start_offset_checkpoints =
            self.create_checkpoint_file(LOG_START_OFFSET_CHECKPOINT_FILE);
        let preferred_log_dirs: HashMap<TopicPartition, String> = HashMap::new();
        self.load_logs().await?;
        Ok(())
    }

    /// We have iterated over each log.dirs and from there listed, they would look like
    /// `topic`-`partition_number` and must be dir
    fn load_potential_dir_logs(
        &self,
        partition_dir: PathBuf,
        load_log_finished: &mut Vec<(PathBuf, oneshot::Receiver<Result<(), AsyncTaskError>>)>,
        recovery_points: HashMap<TopicPartition, i64>,
        log_start_offsets: HashMap<TopicPartition, i64>,
    ) -> Result<(), io::Error> {
        if partition_dir.is_dir() {
            for dir_content in fs::read_dir(partition_dir.clone())? {
                let path = dir_content?.path();
                if path.is_dir() {
                    let majordomo_tx_cp = self.majordomo_tx.clone();
                    let (tx, rx) = oneshot::channel();
                    load_log_finished.push((partition_dir.clone(), rx));
                    // RAFKA TODO: In the orignal code, a threadpool sized
                    // num_recovery_threads_per_data_dir would be used to
                    // recover per-data-dir logs, we need to adapt that somehow to these
                    // spawns, perhaps a bounded-channel for this case?
                    let partition_dir_cp = partition_dir.clone();
                    let recovery_points_cp = recovery_points.clone();
                    let log_start_offsets_cp = log_start_offsets.clone();
                    tokio::spawn(async move {
                        majordomo_tx_cp
                            .send(AsyncTask::LogManager(LogManagerAsyncTask::ReqLoadLogs(
                                LoadLogRequest {
                                    recovery_points: recovery_points_cp,
                                    log_start_offsets: log_start_offsets_cp,
                                    log_dir: partition_dir_cp,
                                    tx,
                                },
                            )))
                            .await
                            .expect(
                                "Unable to queue potential dir log load to majordomo coordinator",
                            );
                    });
                }
            }
        }
        Ok(())
    }

    /// Recovers and loads all the logs from the log_dirs data directories
    async fn load_logs(&mut self) -> Result<(), AsyncTaskError> {
        tracing::info!("Loading logs");
        let _init_time = self.time;
        let mut offline_dirs: Vec<(PathBuf, io::Error)> = vec![];
        // Store the log_dir and a response channel that contains the result from the async task to
        // load the logs.
        let mut load_log_finished: Vec<(PathBuf, oneshot::Receiver<Result<(), AsyncTaskError>>)> =
            vec![];
        for dir in &self.live_log_dirs {
            let clean_shutdown_file =
                PathBuf::from(format!("{}/{}", dir.display(), log::CLEAN_SHUTDOWN_FILE));
            if clean_shutdown_file.exists() {
                tracing::debug!(
                    "Clean shutdown file exists: {} Skipping recovery of logs.",
                    clean_shutdown_file.display()
                );
            } else {
                // RAFKA TODO: Somehow the log recovery itself is being performed by `Log` class
                // during initialization
                let tx = self.majordomo_tx.clone();
                tx.send(AsyncTask::Coordinator(CoordinatorTask::UpdateBrokerState(
                    BrokerState::RecoveringFromUncleanShutdown,
                )))
                .await?;
            }
            let mut recovery_points: HashMap<TopicPartition, i64> = HashMap::new();
            match self.recovery_point_checkpoints.get(dir) {
                Some(val) => match val.read().await {
                    Ok(val) => {
                        recovery_points = val;
                    },
                    Err(err) => {
                        tracing::warn!(
                            "Error reading recovery-point-offset-checkpoint dir {}: {:?}. \
                             Resetting the recovery checkpoint to 0.",
                            dir.display(),
                            err
                        );
                    },
                },
                None => unreachable!(),
            };
            let mut log_start_offsets: HashMap<TopicPartition, i64> = HashMap::new();
            match self.log_start_offset_checkpoints.get(dir) {
                Some(val) => match val.read().await {
                    Ok(val) => log_start_offsets = val,
                    Err(err) => {
                        tracing::warn!(
                            "Error occurred while reading log-start-offset-checkpoint file of \
                             directory {}: {:?}",
                            dir.display(),
                            err
                        );
                    },
                },
                None => unreachable!(),
            };
            if let Err(err) = self.load_potential_dir_logs(
                dir.clone(),
                &mut load_log_finished,
                recovery_points,
                log_start_offsets,
            ) {
                offline_dirs.push((dir.clone(), err));
            }
        }
        for (dir, rx) in load_log_finished {
            //  Nait for all the logs to be read
            if let Err(err) = rx.await? {
                match err {
                    AsyncTaskError::LogManager(err) => {
                        LogDirFailureChannelAsyncTask::send_maybe_add_offline_log_dir(
                            self.majordomo_tx.clone(),
                            dir.display().to_string(),
                            err,
                        )
                        .await
                        .expect("Unable to send failed log_load to LogDirFailureChannel")
                    },
                    err @ _ => tracing::error!(
                        "Unexpected error type: {:?} while on operation LoadLogRequest",
                        err
                    ),
                };
            }
        }

        // RAFKA NEXT: delete clean shutdown files
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
                tracing::info!("Log directory {dir_absolute_path} not found, creating it.");
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
                    Ok(_) => tracing::trace!("Successfully created dir: {dir_absolute_path}"),
                };
            }
            if !dir.is_dir() {
                tracing::error!("dir: {dir_absolute_path} not a directory");
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
                tracing::error!("dir: {dir_absolute_path} unable to read directory");
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
            tracing::error!(
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
                    LogDirFailureChannelAsyncTask::send_maybe_add_offline_log_dir(
                        self.majordomo_tx.clone(),
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

#[derive(Debug)]
pub struct LoadLogResponse {
    topic_partition: TopicPartition,
    log_dir: PathBuf,
    log: Log,
    tx: oneshot::Sender<Result<(), AsyncTaskError>>,
}

#[derive(Debug)]
pub struct LoadLogRequest {
    recovery_points: HashMap<TopicPartition, i64>,
    log_start_offsets: HashMap<TopicPartition, i64>,
    log_dir: PathBuf,
    tx: oneshot::Sender<Result<(), AsyncTaskError>>,
}

#[derive(Debug)]
pub enum LogManagerAsyncTask {
    /// When the KafkaServer starts it will read the different directories and send the LogManager
    /// the initial_offline_dirs here, then we can init the LogManager
    LoadInitialOfflineDirs(Vec<String>, Instant),
    /// loads the logs on a given dir.
    ReqLoadLogs(LoadLogRequest),
    ResLoadLogs(LoadLogResponse),
}

#[derive(Debug, PartialEq, Eq)]
enum LogManagerState {
    PreInit,
    Initialized,
}

#[derive(Debug)]
pub struct LogManagerCoordinator {
    tx: mpsc::Sender<LogManagerAsyncTask>,
    rx: mpsc::Receiver<LogManagerAsyncTask>,
    topic_configs: HashMap<String, LogConfig>, // note that this doesn't get updated after creation
    initial_default_config: LogConfig,
    log_manager: LogManager,
    future_logs: HashMap<TopicPartition, Log>,
    current_logs: HashMap<TopicPartition, Log>,
    logs_to_be_deleted: HashMap<Log, Instant>,
    state: LogManagerState,
}
impl LogManagerCoordinator {
    /// Creates a new instance of the LogManagerCoordinator
    pub async fn new(
        config: KafkaConfig,
        time: Instant,
        tx: mpsc::Sender<LogManagerAsyncTask>,
        rx: mpsc::Receiver<LogManagerAsyncTask>,
        majordomo_tx: mpsc::Sender<AsyncTask>,
    ) -> Result<Self, AsyncTaskError> {
        let _init_time = Instant::now();
        let broker_defaults = LogConfigProperties::try_from(&config)?.build()?;
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
        let future_logs: HashMap<TopicPartition, Log> = HashMap::new();
        let current_logs: HashMap<TopicPartition, Log> = HashMap::new();
        // res.log_manager.init().await?;
        // The LogManagerCoordinator cannot yet initialize, it must first wait for the
        // LoadInitialOfflineDirs from the KafkaServer

        Ok(Self {
            tx,
            rx,
            topic_configs,
            future_logs,
            current_logs,
            initial_default_config: broker_defaults,
            log_manager: LogManager::new(config, KafkaScheduler::default(), time, majordomo_tx)?,
            logs_to_be_deleted: HashMap::new(),
            state: LogManagerState::PreInit,
        })
    }

    /// `main_tx` clones the current transmission endpoint in the coordinator channel.
    pub fn main_tx(&self) -> mpsc::Sender<LogManagerAsyncTask> {
        self.tx.clone()
    }

    /// `process_message_queue` receives LogManagerAsyncTask requests from clients
    /// If a client wants a response it may use a oneshot::channel for it
    pub async fn process_message_queue(&mut self) -> Result<(), AsyncTaskError> {
        while let Some(task) = self.rx.recv().await {
            tracing::info!("LogManager coordinator {:?}", task);
            match task {
                LogManagerAsyncTask::ReqLoadLogs(req) => {
                    // RAKA TODO: handle sending back a AsyncTaskRetry for when the LogManagerState
                    // is not Initialized, otherwise the req.tx would never be used and the waiting
                    // process would be stuck forever
                    if self.state == LogManagerState::Initialized {
                        self.req_load_log(req).await.expect("Unable to process ReqLoadLogs");
                    }
                },
                LogManagerAsyncTask::ResLoadLogs(res) => {
                    // RAKA TODO: handle sending back a AsyncTaskRetry for when the LogManagerState
                    // is not Initialized, otherwise the req.tx would never be used and the waiting
                    // process would be stuck forever
                    if self.state == LogManagerState::Initialized {
                        self.res_load_log(res).await.expect("Unable to process ResLoadLogs");
                    }
                },
                LogManagerAsyncTask::LoadInitialOfflineDirs(initial_offline_dirs, time) => {
                    self.log_manager.set_initial_offline_dirs(initial_offline_dirs);
                    self.log_manager.init().await?;
                    self.state = LogManagerState::Initialized;
                    self.log_manager.time = time;
                },
            }
        }
        Ok(())
    }

    /// For a specific topic_partition directory, the log is checked to understand if:
    /// - log is to be deleted (A topic that has been deleted (Would reassignment cause this name
    ///   to?))
    /// - log is future (A partition that is being reassigned to another broker[:log.dir]
    /// - log is current (An active partition)
    /// Async thoughts:
    /// - This function performs I/O read operation for the filename.
    /// - Reports back to the LogManager that certain logs have been loaded for either
    ///   Deleted/Future/Current
    /// - Reports back to the caller that the log has been read.
    pub async fn req_load_log(&mut self, req: LoadLogRequest) -> Result<(), AsyncTaskError> {
        let topic_partition = Log::parse_topic_partition_name(&req.log_dir)?;
        let config = self
            .topic_configs
            .get(topic_partition.topic())
            .map_or(self.initial_default_config.clone(), |v| v.clone());
        let log_recovery_point = req.recovery_points.get(&topic_partition).unwrap_or(&0).clone();
        let log_start_offset = req.log_start_offsets.get(&topic_partition).unwrap_or(&0).clone();

        let log_mgr_time = self.log_manager.time;
        let log_mgr_max_pid_expiration_ms = self.log_manager.max_pid_expiration_ms;
        let log_mgr_producer_id_expiration_check_interval_ms =
            self.log_manager.producer_id_expiration_check_interval_ms;
        let log_mgr_majordomo_tx = self.log_manager.majordomo_tx.clone();
        tokio::spawn(async move {
            match Log::new(
                req.log_dir.clone(),
                config,
                log_start_offset,
                log_recovery_point,
                // broker_topic_stats: brokerTopicStats,
                log_mgr_time,
                log_mgr_max_pid_expiration_ms,
                log_mgr_producer_id_expiration_check_interval_ms,
                log_mgr_majordomo_tx.clone(),
            ) {
                Ok(mut log) => {
                    tracing::debug!("{} Successfully loaded log", log.log_ident());
                    match log.init().await {
                        Ok(()) => log_mgr_majordomo_tx
                            .send(AsyncTask::LogManager(LogManagerAsyncTask::ResLoadLogs(
                                LoadLogResponse {
                                    topic_partition,
                                    log_dir: req.log_dir,
                                    log,
                                    tx: req.tx,
                                },
                            )))
                            .await
                            .expect("Unable to send ResLoadLogs to LogManagerCoordinator"),
                        Err(err) => {
                            req.tx.send(Err(err.into())).expect("Oneshot receiver handle dropped.");
                        },
                    }
                },
                Err(err) => {
                    tracing::error!("Unable to load log: {:?}", err);
                    req.tx.send(Err(err.into())).expect("Oneshot receiver handle dropped.");
                },
            };
        });
        Ok(())
    }

    /// Handles the finish Response triggered by a previous async LoadLogRequest. The Request was
    /// previously spawned and once finished it would send a message to process LoadLogResponse
    /// here.
    pub async fn res_load_log(&mut self, res: LoadLogResponse) -> Result<(), AsyncTaskError> {
        if res.log_dir.display().to_string().ends_with(log::DELETE_DIR_SUFFIX) {
            self.logs_to_be_deleted.insert(res.log, Instant::now());
        } else {
            let log_is_future = res.log.is_future();
            let dir_absolute_path = res.log_dir.canonicalize().unwrap();
            let previous = if res.log.is_future() {
                self.future_logs.insert(res.topic_partition.clone(), res.log)
            } else {
                self.current_logs.insert(res.topic_partition.clone(), res.log)
            };
            if let Some(previous) = previous {
                let dir_absolute_path = dir_absolute_path.display();
                // RAFKA NOTE: Technically both `log_dir` and `previous` have been already
                // canonicalized and unwrapped... Could this fail later?
                let prev_absolute_path = previous.dir().canonicalize().unwrap();
                let prev_absolute_path = prev_absolute_path.display();

                if log_is_future {
                    return Err(AsyncTaskError::LogManager(
                        LogManagerError::DuplicateLogDirectory(format!(
                            "Duplicate log directories found: {dir_absolute_path}, \
                             {prev_absolute_path}"
                        )),
                    ));
                } else {
                    return Err(AsyncTaskError::LogManager(
                        LogManagerError::DuplicateLogDirectory(format!(
                            "Duplicate log directories for {} are found in both \
                             {dir_absolute_path} and {prev_absolute_path}. It is likely because \
                             log directory failure happened while broker was replacing current \
                             replica with future replica. Recover broker from this failure by \
                             manually deleting one of the two directories for this partition. It \
                             is recommended to delete the partition in the log directory that is \
                             known to have failed recently.",
                            res.topic_partition
                        )),
                    ));
                }
            }
        }
        tokio::spawn(async move {
            res.tx.send(Ok(())).expect("Unable to send loag log response signal to caller.")
        });
        Ok(())
    }

    /// Helper function that allows clients to LogManagerCoordinator to send the initial offline
    /// dirs without having to build/import the whole request, the KafkaServer does some initial
    /// validation and would send here the initial offline dirs
    pub async fn send_offline_dirs(
        tx: mpsc::Sender<AsyncTask>,
        initial_offline_dirs: Vec<String>,
        time: Instant,
    ) -> Result<(), AsyncTaskError> {
        tx.send(AsyncTask::LogManager(LogManagerAsyncTask::LoadInitialOfflineDirs(
            initial_offline_dirs,
            time,
        )))
        .await?;
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
            majordomo_tx,
            time: Instant::now(),
            lock_file: DEFAULT_LOCK_FILE.to_string(),
            dir_locks: vec![],
            recovery_point_checkpoints: HashMap::new(),
            log_start_offset_checkpoints: HashMap::new(),
        }
    }
}
