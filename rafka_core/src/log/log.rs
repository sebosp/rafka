//! From core/bin/main/kafka/log/Log.scala

use super::log_config::LogConfig;
use super::log_manager::LogManagerError;
use super::log_segment::LogSegment;
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
use std::collections::HashMap;
use std::fs::{self, DirEntry};
use std::hash::{Hash, Hasher};
use std::io;
use std::num;
use std::path::Path;
use std::path::PathBuf;
use std::time::Instant;
use tokio::sync::mpsc;

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
pub enum LogError {
    #[error("Parse error: {0}")]
    ParseInt(#[from] num::ParseIntError),
    #[error("dot separator not found on filename: {0}")]
    MissingDotSeparator(String),
    #[error("Io {0:?}")]
    Io(#[from] std::io::Error),
    #[error("Expected string to end with '{0}' but string is '{1}'")]
    UnexpectedSuffix(String, String),
    #[error(
        "The size of segment {0} ({1}) is larger than the maximum allowed segment size of {2}"
    )]
    SegmentSizeAboveMax(String, u64, i32),
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
    segments: HashMap<i64, LogSegment>,
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
            segments: HashMap::new(),
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
            delete_file_if_exists(leader_epoch_file_path)?;
            self.leader_epoch_cache = None;
        } else {
            self.leader_epoch_cache =
                Some(self.new_leader_epoch_file_cache(leader_epoch_file).await?);
        }
        Ok(())
    }

    fn delete_indices_if_exist(&self, base_file: PathBuf, suffix: &str) -> Result<(), LogError> {
        tracing::info!("Deleting index files with suffix {suffix:?} for base_file {base_file:?}");
        let offset = Self::offset_from_file(&base_file)?;
        delete_file_if_exists(&Self::offset_index_file(self.dir.clone(), offset, suffix))?;
        delete_file_if_exists(&Self::time_index_file(self.dir.clone(), offset, suffix))?;
        delete_file_if_exists(&Self::transaction_index_file(self.dir.clone(), offset, suffix))?;
        Ok(())
    }

    /// Deletes all the temp files found in the directory. Returns a list of the `.swap` files that
    /// need to be swapped in place for existing segments. Additionally, incomplete log splitting
    /// is identified by looking into the `.swap` file base offset, if it is higher than the
    /// smallest offset `.clean`. These `.swap` files are also deleted by this method.
    async fn remove_temp_files_and_collect_swap_files(&self) -> Result<Vec<PathBuf>, LogError> {
        let mut swap_files = vec![];
        let mut clean_files = vec![];
        let mut min_cleaned_file_offset = i64::MAX;

        for entry in fs::read_dir(self.dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {}
            let filename: String = format!("{}", path.file_name().unwrap().to_string_lossy());
            if filename.ends_with(DELETED_FILE_SUFFIX) {
                tracing::debug!("Deleting stray temporary file {:?}", path.canonicalize());
                delete_file_if_exists(&path);
            } else if filename.ends_with(CLEANED_FILE_SUFFIX) {
                let min_cleaned_file_offset =
                    std::cmp::min(Self::offset_from_file_name(filename)?, min_cleaned_file_offset);
                clean_files.push(path);
            } else if filename.ends_with(SWAP_FILE_SUFFIX) {
                // There was a crash in the middle of a swap operation.
                // Steps to recover:
                // - If this is a log file, first delete the index files, then complete the swap
                // operation later.
                // - If this is an index file, just delete the index file as it will be rebuilt.
                let base_file = PathBuf::from(replace_suffix(&filename, SWAP_FILE_SUFFIX, "")?);
                tracing::info!(
                    "Found file {:?} from interrupted swap operation.",
                    path.canonicalize()
                );
                if Self::is_index_file(&base_file) {
                    self.delete_indices_if_exist(base_file, "")?;
                } else if Self::is_log_file(&base_file) {
                    self.delete_indices_if_exist(base_file, "")?;
                    swap_files.push(path)
                }
            }
        }

        // KAFKA-6264: Delete all .swap files whose base offset is greater than the minimum .cleaned
        // segment offset. Such .swap files could be part of an incomplete split operation
        // that could not complete. See Log#splitOverflowedSegment for more details about
        // the split operation.
        let (invalid_swap_files, valid_swap_files) = swap_files
            .into_iter()
            .partition(|file| Self::offset_from_file(&file)? >= min_cleaned_file_offset)
            .collect();
        for invalid_swap_file in invalid_swap_files {
            tracing::debug!(
                "Deleting invalid swap file {} minCleanedFileOffset: {min_cleaned_file_offset}",
                invalid_swap_file.canonicalize().unwrap().display()
            );
            let base_file =
                PathBuf::from(replace_suffix(&invalid_swap_file, SWAP_FILE_SUFFIX, "")?);
            self.delete_indices_if_exist(base_file, SWAP_FILE_SUFFIX)?;
            delete_file_if_exists(&invalid_swap_file)?;
        }

        // after the .swap files from incomplete split operations have been deleted, we can clean up
        // the .clear files.
        for clean_file in clean_files {
            tracing::debug!(
                "Deleting stray .clean file {}",
                clean_file.canonicalize().unwrap().display()
            );
            delete_file_if_exists(&clean_file);
        }

        Ok(valid_swap_files)
    }

    /// Clears the current segments and attempts to load them.
    /// This function is called multiple times from [`load_segments`] and is retried on
    /// LogManagerError::LogSegmentOffsetOverflow
    fn clear_and_load_segments(&mut self) -> Result<(), LogManagerError> {
        for (_key, segment) in self.segments.iter_mut() {
            *segment.close()?;
        }
        self.segments.clear();
        self.load_segment_files()?;
        Ok(())
    }

    /// Loads the segments from the files on disk to return the next offset.
    /// This method is called on [`Self::init`] and may return an
    /// [`LogManagerError::LogSegmentOffsetOverflow error when:
    /// - A `.swap` file is encountered with messages that overflow the index offset.
    /// - An unexpected number of .log files are found "with overflow"
    async fn load_segments(&mut self) -> Result<i64, LogError> {
        // Go through the files in the log directory, temp files need to be removed and interrupted
        // swap operations need to be completed.
        let swap_files = self.remove_temp_files_and_collect_swap_files().await?;

        // Load all the log and segment files
        // KAFKA-6264 there may be legacy log segements that generate LogSegmentOffsetOverflow,
        // these are split when encountered, once split all the logs are loaded again from
        // clean state.
        loop {
            // retry_on_offset_overflow
            match self.clear_and_load_segments() {
                Ok(()) => break,
                Err(LogManagerError::LogSegmentOffsetOverflow(offset, segment)) => {
                    // In case we encounter a segment with offset overflow, the retry logic will
                    // split it after which we need to retry loading of
                    // segments. In that case, we also need to close all segments that could have
                    // been left open in previous call to loadSegmentFiles().
                    tracing::info!(
                        "Caught segment overflow error: {:?}. Splittting segment and retrying.",
                        LogManagerError::LogSegmentOffsetOverflow(offset, segment)
                    );
                    self.split_overflowed_segment(segment);
                },
                Err(err) => {
                    return Err(err.into());
                },
            }
        }
    }

    /// All the log segments in this log ordered from oldest to newest
    fn log_segments(&self) -> Vec<LogSegment> {
        self.segments.values
    }

    fn offset_from_file_name(filename: String) -> Result<i64, LogError> {
        match filename.find('.') {
            Some(idx) => {
                let (offset, _) = filename.split_at(idx);
                Ok(offset.parse::<i64>()?)
            },
            None => Err(LogError::MissingDotSeparator(filename)),
        }
    }

    /// Creates filenames using padding so that `ls` sorts files numerically.
    fn filename_prefix_from_offset(offset: i64) -> String {
        format!("{:020}", offset)
    }

    /// Creates a file name in the provided dir using the base offset and the suffix
    fn offset_index_file(dir: PathBuf, offset: i64, suffix: &str) -> PathBuf {
        dir.set_file_name(format!(
            "{}{}{}",
            Self::filename_prefix_from_offset(offset),
            INDEX_FILE_SUFFIX,
            suffix
        ));
        dir
    }

    /// Creates a time index file name in the provided dir using the base offset and the suffix
    fn time_index_file(dir: PathBuf, offset: i64, suffix: &str) -> PathBuf {
        dir.set_file_name(format!(
            "{}{}{}",
            Self::filename_prefix_from_offset(offset),
            TIME_INDEX_FILE_SUFFIX,
            suffix
        ));
        dir
    }

    /// Construct a transaction index file name in the given dir using the given base offset and the
    /// given suffix
    fn transaction_index_file(dir: PathBuf, offset: i64, suffix: &str) -> PathBuf {
        dir.set_file_name(format!(
            "{}{}{}",
            Self::filename_prefix_from_offset(offset),
            TXN_INDEX_FILE_SUFFIX,
            suffix
        ));
        dir
    }

    fn offset_from_file(file: &PathBuf) -> Result<i64, LogError> {
        Self::offset_from_file_name(format!("{}", file.file_name().unwrap().to_str().unwrap()))
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
        filename.ends_with(INDEX_FILE_SUFFIX)
            || filename.ends_with(TIME_INDEX_FILE_SUFFIX)
            || filename.ends_with(TXN_INDEX_FILE_SUFFIX)
    }

    fn is_log_file(file: &PathBuf) -> bool {
        let filename = file.file_name().unwrap().to_str();
        filename.ends_with(LOG_FILE_SUFFIX)
    }
}
