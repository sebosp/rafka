//! From core/bin/main/kafka/log/Log.scala

use super::log_config::LogConfig;
use crate::common::topic_partition::TopicPartition;
use crate::log::producer_state_manager::ProducerStateManager;
use crate::majordomo::{AsyncTask, AsyncTaskError};
use crate::KafkaException;
use lazy_static::lazy_static;
use regex::Regex;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::time::Instant;
use tokio::sync::mpsc;

// Used by kafka 0.8 and higher to indicate kafka was shutdown properly.
// This helps avoiding recovering a log.
pub const CLEAN_SHUTDOWN_FILE: &str = ".kafka_cleanshutdown";
// A directory to be deleted
pub const DELETE_DIR_SUFFIX: &str = "-delete";

// A directory that is used for future partition, for example for a partition being sent to/from
// another broker/log-dir
pub const FUTURE_DIR_SUFFIX: &str = "-future";

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
            log_ident,
            majordomo_tx,
        })
    }

    pub async fn init(&mut self) -> Result<(), AsyncTaskError> {
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
}
