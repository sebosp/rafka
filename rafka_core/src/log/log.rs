//! From core/bin/main/kafka/log/Log.scala

use super::log_config::LogConfig;
use crate::common::topic_partition::TopicPartition;
use std::{path::PathBuf, time::Instant};

// Used by kafka 0.8 and higher to indicate kafka was shutdown properly.
// This helps avoiding recovering a log.
pub const CLEAN_SHUTDOWN_FILE: &str = ".kafka_cleanshutdown";

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
    max_producer_id_expiration_ms: u32,
    /// Interval to check for expiration of producer ids
    producer_id_expiration_check_interval_ms: u32,
    topic_partition: TopicPartition,
    // The identifier of a Log
    log_ident: String,
}

// impl Log {
// pub fn new(topic_partition: TopicPartition, dir: PathBuf) -> Self {
//    let dir_parent = dir.parent().unwrap_or(PathBuf::from("/"));
// Self {
// log_ident = format!("[Log partition={topic_partition}, dir={dir_parent}] ")
//}
