//! From core/src/main/scala/kafka/server/LogDirFailureChannel.scala
//!
//! The purpose is to allow a channel to receive "Lock" state when there's a rew offline log.dir.
//! As a difference to the kafka code, in this version, the ownership of the failure channel
//! resides in the majordomo coordinator.
//! When a fn that performs I/O operations encounters an I/O error, it needs to communicate this
//! failure with the coordinator, which in turn will add the log.dir to the offline direcories vec.
//! The broker needs to be restarted once the I/O error is resolved by human intervention (i.e. disk
//! full).

use std::collections::HashMap;
use tracing::error;

#[derive(Debug)]
pub struct LogDirFailureChannel {
    offline_log_dirs: HashMap<String, String>,
    offline_log_dir_queue: Vec<String>,
}

impl LogDirFailureChannel {
    pub fn new(log_dir_num: usize) -> Self {
        Self {
            offline_log_dirs: HashMap::new(),
            offline_log_dir_queue: Vec::with_capacity(log_dir_num),
        }
    }

    /// `maybe_add_offline_log_dir` Potentially adds the log_dir to the queue if it doesn't exist
    /// there yet.
    pub fn maybe_add_offline_log_dir(&mut self, log_dir: String, msg: String) {
        error!("maybe_add_offline_log_dir: {}", msg);
        let log_dir_already_added = self.offline_log_dirs.get(&log_dir).is_some();
        if !log_dir_already_added {
            self.offline_log_dir_queue.push(log_dir.clone());
            self.offline_log_dirs.insert(log_dir.clone(), log_dir);
        }
    }

    // In the original code with shared state, a thread may decide to block and wait for new
    // log_dir_failures to appear and perform operations such as alerting, metrics, logging,
    // cleanup, etc. But this doesn't make much sense in the case of Rust, rather, the failure would
    // trigger a function on a cleaner/handler and that in turn would perform the operation.
    // pub fn take_next_offline_log_dir(&self) -> String {
    // self.offline_log_dir_queue.take()
    //}
}
