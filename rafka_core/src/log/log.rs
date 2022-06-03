//! From core/bin/main/kafka/log/Log.scala

use super::log_config::LogConfig;
use super::log_manager::LogManagerError;
use crate::common::record::record_version::RecordVersion;
use crate::common::topic_partition::TopicPartition;
use crate::log::producer_state_manager::ProducerStateManager;
use crate::majordomo::{AsyncTask, AsyncTaskError};
use crate::server::checkpoints::leader_epoch_checkpoint_file::LeaderEpochCheckpointFile;
use crate::server::epoch::leader_epoch_file_cache::LeaderEpochFileCache;
use crate::server::log_offset_metadata::LogOffsetMetadata;
use crate::utils::core_utils::replace_suffix;
use crate::utils::delete_file_if_exists;
use crate::KafkaException;
use lazy_static::lazy_static;
use regex::Regex;
use std::num;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::time::Instant;
use tokio::sync::mpsc;
use std::io;
use std::fs::{self, DirEntry};
use std::path::Path;

/// Log file
pub const LOG_FILE_SUFFIX: &str = ".log";

// Used by kafka 0.8 and higher to indicate kafka was shutdown properly.
// This helps avoiding recovering a log.
pub const CLEAN_SHUTDOWN_FILE: &str = ".kafka_cleanshutdown";
// A directory to be deleted
pub const DELETE_DIR_SUFFIX: &str = "-delete";
/// Index File
pub const INDEX_FILE_SUFFIX: &str = ".index";

/// Time index file
pub const TIME_INDEX_FILE_SUFFIX: &str = ".timeindex";

/// A potentially aborted transaction index
pub const TXN_INDEX_FILE_SUFFIX: &str = ".txnindex";

/// A file scheduled for deletion
pub const DELETED_FILE_SUFFIX: &str = ".deleted";

/// Temporary file used for log cleaning
pub const CLEANED_FILE_SUFFIX: &str = ".cleaned";

/// Temporary file used for swapping files into a log
pub const SWAP_FILE_SUFFIX: &str = ".swap";


// A directory that is used for future partition, for example for a partition being sent to/from
// another broker/log-dir
pub const FUTURE_DIR_SUFFIX: &str = "-future";

pub const UNKNOWN_OFFSET: i64 = -1;

pub fn delete_dir_pattern(input: &str) -> Option<(&str, &str, &str)> {
    lazy_static! {
        static ref DELETE_DIR_PATTERN: Regex =
            Regex::new(&format!(r"^(\S+)-(\S+)\.(\S+){}", DELETE_DIR_SUFFIX)).unwrap();
    }
    let captures = DELETE_DIR_PATTERN.captures(input)?;
    Some((
        captures.get(1).map_or("", |m| m.as_str()),
        captures.get(2).map_or("", |m| m.as_str()),
        captures.get(3).map_or("", |m| m.as_str()),
    ))
}

pub fn future_dir_pattern(input: &str) -> Option<(&str, &str, &str)> {
    lazy_static! {
        static ref FUTURE_DIR_PATTERN: Regex =
            Regex::new(&format!(r"^(\S+)-(\S+)\.(\S+){}", FUTURE_DIR_SUFFIX)).unwrap();
    }
    let captures = FUTURE_DIR_PATTERN.captures(input)?;
    Some((
        captures.get(1).map_or("", |m| m.as_str()),
        captures.get(2).map_or("", |m| m.as_str()),
        captures.get(3).map_or("", |m| m.as_str()),
    ))
}

/// A series of errors when handling log files, their `.swap`, `.clean`, etc. 
#[derive(thiserror::Error, Debug)]
pub enum LogError{
    #[error("Parse error: {0}")]
    ParseInt(#[from] num::ParseIntError),
    #[error("dot separator not found on filename: {0}")]
    MissingDotSeparator(String),
    #[error("Io {0:?}")]
    Io(#[from] std::io::Error),
    #[error("Expected string to end with '{}' but string is '{}'")]
    UnexpectedSuffix(String, String),
}

/// Append-only log for storing messages, it is a sequence of segments.
/// Each segment has a base offset showing the first message in such segment.
/// Logs are created based on policies such as byte size, time, etc.
#[derive(Debug)]
pub struct Log {
    /// directory to store the log segments
    dir: PathBuf,
    /// The Log Configuration
    config: LogConfig,
    /// Earliest offset visible to Kafka Clients
    /// The following cause updates:
    /// - User DeleteRecordsRequest (RAFKA TODO)
    /// - The Kafka Broker Log Retention
    /// - The Kafka Broker Log Truncation
    /// Based on its value the following conditions are evaluated:
    /// - Log Deletenio: if the next_offset is <= the current log_start_offset, then this log can
    ///   be deleted. This may trigger log rolling on deletion of the active segment.
    /// - ListOffsetRequest(RAFKA TODO): Respond to this type of mesage. Allows checking for
    /// OffsetOutOfRange error (RAFKA TODO) when user wants to go for the earliest offset. Taking
    /// into consideration that the log_start_offset <= this log's high_watermark.
    /// Activies such as the log cleaning process are not affected by this value
    log_start_offset: i64,
    /// The offset to begin recovery at. For example the first offset that has not been flushed to
    /// disk
    recovery_point: i64,
    time: Instant,
    /// Maximum amount of time to wait before considering the producer id as expired
    max_producer_id_expiration_ms: i64,
    /// Interval to check for expiration of producer ids
    producer_id_expiration_check_interval_ms: u64,
    topic_partition: TopicPartition,
    producer_state_manager: ProducerStateManager,
    next_offset_metadata: LogOffsetMetadata,
    leader_epoch_cache: Option<LeaderEpochFileCache>,
    // The identifier of a Log
    log_ident: String,
    majordomo_tx: mpsc::Sender<AsyncTask>,
}

impl PartialEq for Log {
    fn eq(&self, rhs: &Log) -> bool {
        self.dir == rhs.dir
    }
}

impl Eq for Log {}

impl Hash for Log {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.dir.hash(state);
    }
}

impl Log {
    pub fn new(
        dir: PathBuf,
        config: LogConfig,
        log_start_offset: i64,
        recovery_point: i64,
        // broker_topic_stats: BrokerTopicStats,
        time: Instant,
        max_producer_id_expiration_ms: i64,
        producer_id_expiration_check_interval_ms: u64,
        majordomo_tx: mpsc::Sender<AsyncTask>,
    ) -> Result<Log, AsyncTaskError> {
        let topic_partition = Self::parse_topic_partition_name(&dir)?;
        let dir_parent = match dir.parent() {
            Some(val) => val.display().to_string(),
            None => PathBuf::from("/").display().to_string(),
        };
        let log_ident = format!("[Log partition={topic_partition}, dir={dir_parent}] ");
        let producer_state_manager = ProducerStateManager::new(
            topic_partition.clone(),
            dir.clone(),
            Some(max_producer_id_expiration_ms),
        );
        Ok(Self {
            dir,
            config,
            log_start_offset,
            recovery_point,
            time,
            max_producer_id_expiration_ms,
            producer_id_expiration_check_interval_ms,
            topic_partition,
            producer_state_manager,
            next_offset_metadata: LogOffsetMetadata::default(),
            leader_epoch_cache: None,
            log_ident,
            majordomo_tx,
        })
    }

    pub async fn init(&mut self) -> Result<(), AsyncTaskError> {
        // Create the log directory path, it may already exist.
        fs::create_dir_all(self.dir.clone())?;
        self.initialize_leader_epoch_cache().await?;
        let next_offset = self.load_segments().await?;
        Ok(())
    }

    /// The offset of the next message that will be appended to the log
    fn log_end_offset(&self) -> i64 {
        self.next_offset_metadata.message_offset
    }

    fn record_version(&self) -> RecordVersion {
        self.config.message_format_version.record_version()
    }

    async fn new_leader_epoch_file_cache(
        &self,
        leader_epoch_file: PathBuf,
    ) -> Result<LeaderEpochFileCache, AsyncTaskError> {
        let checkpoint_file =
            LeaderEpochCheckpointFile::new(leader_epoch_file, self.majordomo_tx.clone())?;
        LeaderEpochFileCache::new(
            self.topic_partition.clone(),
            self.log_end_offset(), /* RAFKA TODO: somehow make the FileCache use the value of
                                    * log_end_offset */
            checkpoint_file,
        )
        .await
    }

    pub async fn initialize_leader_epoch_cache(&mut self) -> Result<(), AsyncTaskError> {
        let leader_epoch_file = LeaderEpochCheckpointFile::new_file(self.dir.clone());
        let leader_epoch_file_path = leader_epoch_file.as_path();
        let record_version = self.record_version();
        if record_version.precedes(RecordVersion::V2) {
            let current_cache = if leader_epoch_file.exists() {
                Some(self.new_leader_epoch_file_cache(leader_epoch_file.clone()).await?)
            } else {
                None
            };
            if let Some(cached_entry) = current_cache {
                if !cached_entry.is_empty() {
                    tracing::warn!(
                        "Deleting non-empty leader epoch cache due to incompatible message format \
                         {record_version}"
                    )
                }
            }
            Self::delete_file_if_exists(leader_epoch_file.as_path())?;
            self.leader_epoch_cache = None;
        } else {
            self.leader_epoch_cache = Some(self.new_leader_epoch_file_cache(leader_epoch_file)?);
        }
        Ok(())
    }

    /// Gets the topic, partition from a directory of a log
    pub fn parse_topic_partition_name(dir: &PathBuf) -> Result<TopicPartition, KafkaException> {
        match TopicPartition::try_from(dir.clone()) {
            Ok(val) => Ok(val),
            Err(err) => {
                tracing::error!("Unable to parse TopicPartition: {:?}", err);
                return Err(KafkaException::InvalidTopicPartitionDir(
                    dir.canonicalize().unwrap().display().to_string(),
                    dir.display().to_string(),
                ));
            },
        }
    }

    pub fn is_future(&self) -> bool {
        self.dir.display().to_string().ends_with(FUTURE_DIR_SUFFIX)
    }

    pub fn log_ident(&self) -> &str {
        &self.log_ident
    }

    pub fn dir(&self) -> &PathBuf {
        &self.dir
    }

   fn is_index_file(file: &PathBuf) -> bool {
    let filename = file.file_name().unwrap().to_str();
    filename.ends_with(INDEX_FILE_SUFFIX) || filename.ends_with(TIME_INDEX_FILE_SUFFIX) || filename.ends_with(TXN_INDEX_FILE_SUFFIX)
   }

   fn is_log_file(file: &PathBuf) -> bool {
    let filename = file.file_name().unwrap().to_str();
    filename.ends_with(LOG_FILE_SUFFIX)
   }
}
