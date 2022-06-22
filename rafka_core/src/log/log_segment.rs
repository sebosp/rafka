//! From kafka/log/LogSegment.scala
//! A segment has a base offset, which is an offset less than or equal to any of the offsets of any
//! messages it contains and it's greater than any offset in any previous segments.
//! A Segment consists of two files:
//! - [`<base_offset>.log`] containing the messages.
//! - [`<base_offset>.index`] that allows mapping of logical offsets to physical file positions.
//! RAFKA NOTES:
//! - The original code uses volatile vars for keeping the max timestamps seen so far. In this
//! initial rust version, the value has been moved to a LogSegmentCoordinator which keeps it
//! losely synchronized through mpsc calls.

use super::{lazy_index::LazyIndex, log_config::LogConfig};
use super::log_manager::LogManagerError;
use crate::common::record::file_records::FileRecords;
use crate::log::transaction_index::TransactionIndex;
use crate::majordomo::AsyncTaskError;
use std::{time::Instant, path::PathBuf};
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
pub struct LogSegment {
    /// The file records containing log entries
    log: FileRecords,
    /// The offset index
    lazy_offset_index: LazyIndex,
    /// Timestamp index
    lazy_time_index: LazyIndex,
    /// Transaction index
    txn_index: TransactionIndex,
    /// Lower bound of offsets in the segment
    base_offset: i64,
    /// Approximate bytes between entries in the index
    index_interval_bytes: i32,
    /// Max random jitter subtracted from the scheduled roll time of the segment.
    roll_jitter_ms: i64,
    time: Instant,
    log_segment_coordinator_tx: mpsc::Sender<LogSegmentAsyncTask>,
}
impl LogSegment {
    pub fn new(
        log: FileRecords,
        lazy_offset_index: LazyIndex,
        lazy_time_index: LazyIndex,
        txn_index: TransactionIndex,
        base_offset: i64,
        index_interval_bytes: i32,
        roll_jitter_ms: i64,
        time: Instant,
        log_segment_coordinator_tx: mpsc::Sender<LogSegmentAsyncTask>,
    ) -> Self {
        Self {
            log,
            lazy_offset_index,
            lazy_time_index,
            txn_index,
            base_offset,
            index_interval_bytes,
            roll_jitter_ms,
            time,
            log_segment_coordinator_tx,
        }
    }

    pub fn open(dir: PathBuf, base_offset: i64, config: LogConfig, time: Instant, file_already_exists: Option<bool>,
           init_file_size: Option<i32>, preallocate: bool, file_suffix: String) -> Self {
        let file_already_exists = file_already_exists.unwrap_or(false);
        let init_file_size = init_file_size.unwrap_or(0);
        let preallocate = preallocate.unwrap_or(false);
        let file_suffix = file_suffix.unwrap_or(String::from(""));
        let txn_index TransactionIndex(base_offset, Log.transactionIndexFile(dir, baseOffset, fileSuffix));
        Self::new(
        )
    }

    pub async fn close(&mut self) -> Result<(), LogManagerError> {
        let (rx, tx) = oneshot::channel();
        self.log_segment_coordinator_tx
            .send(LogSegmentAsyncTask::GetMaxTimestampCollectedSoFar(rx))
            .await?;
        let max_timestamp_so_far = rx.await?;
        let (rx, tx) = oneshot::channel();
        self.log_segment_coordinator_tx
            .send(LogSegmentAsyncTask::GetOffsetOfMaxTimestampCollectedSoFar(rx))
            .await?;
        let offset_of_max_timestamp_so_far = rx.await?;
        if max_timestamp_so_far.is_some() || offset_of_max_timestamp_so_far.is_some() {
            self.time_index.maybe_append(
                self.max_timestamp_so_far(),
                self.offset_of_max_timestamp_so_far(),
                // skip_full_check =
                true,
            )
        }
        self.lazy_offset_index.close();
        self.lazy_time_index.close();
        self.log.close();
        self.txn_index.close();
        Ok(())
    }
}

#[derive(Debug)]
pub enum LogSegmentAsyncTask {
    GetMaxTimestampCollectedSoFar(oneshot::Sender<Option<u64>>),
    GetOffsetOfMaxTimestampCollectedSoFar(oneshot::Sender<Option<u64>>),
    SetMaxTimestampCollectedSoFar(oneshot::Sender<()>, u64),
    SetOffsetOfMaxTimestampCollectedSoFar(oneshot::Sender<()>, u64),
}

#[derive(Debug)]
pub struct LogSegmentCoordinator {
    rx: mpsc::Receiver<LogSegmentAsyncTask>,
    tx: mpsc::Sender<LogSegmentAsyncTask>,
    max_timestamp_so_far: Option<u64>,
    offset_of_max_timestamp_so_far: Option<u64>,
}

impl LogSegmentCoordinator {
    pub fn new() -> Self {
        let (rx, tx) = mpsc::channel(4_096);
        Self { rx, tx, max_timestamp_so_far: None, offset_of_max_timestamp_so_far: None }
    }

    /// `send_tx_to_caller` clones the current transmission endpoint in the coordinator channel for
    /// the caller to interact with the message queueu processor.
    pub async fn send_tx_to_caller(
        &self,
        tx: oneshot::Sender<mpsc::Sender<LogSegmentAsyncTask>>,
    ) -> Result<(), AsyncTaskError> {
        tx.send(self.tx.clone()).await
    }

    /// `process_message_queue` receives LogManagerAsyncTask requests from clients
    /// If a client wants a response it may use a oneshot::channel for it
    pub async fn process_message_queue(&mut self) -> Result<(), AsyncTaskError> {
        while let Some(task) = self.rx.recv().await {
            tracing::info!("LogSegment coordinator {:?}", task);
            match task {
                LogSegmentAsyncTask::GetMaxTimestampCollectedSoFar(tx) => {
                    tokio::spawn(async {
                        tx.send(self.max_timestamp_so_far)
                            .await
                            .expect("Unable to send GetMaxTimestampCollectedSoFar to caller.");
                    });
                },
                LogSegmentAsyncTask::GetOffsetOfMaxTimestampCollectedSoFar(tx) => {
                    tokio::spawn(async {
                        tx.send(self.offset_of_max_timestamp_so_far).await.expect(
                            "Unable to send GetOffsetOfMaxTimestampCollectedSoFar to caller.",
                        );
                    });
                },
                LogSegmentAsyncTask::SetMaxTimestampCollectedSoFar(tx, val) => {
                    self.max_timestamp_so_far = Some(val);
                    tx.send(()).await?;
                },
                LogSegmentAsyncTask::SetOffsetOfMaxTimestampCollectedSoFar(tx, val) => {
                    self.offset_of_max_timestamp_so_far = Some(val);
                    tx.send(()).await?;
                },
            }
        }
        Ok(())
    }
}
