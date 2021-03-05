//! Finalized Feature Change Listener
//! kafka/server/FinalizedFeatureChangeListener.scala
//! Listens to changes in the ZK feature node, via the ZK client. Whenever a change notification
//! is received from ZK, the feature cache in FinalizedFeatureCache is asynchronously updated
//! to the latest features read from ZK. The cache updates are serialized through a single
//! notification processor thread.
use crate::majordomo::{AsyncTask, AsyncTaskError};
use crate::server::finalized_feature_cache::FinalizedFeatureCacheAsyncTask;
use crate::zk::kafka_zk_client::KafkaZkClientAsyncTask;
use crate::zk::zk_data;
use thiserror::Error;
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info};
use tracing_attributes::instrument;
#[derive(Debug)]
pub struct FinalizedFeatureChangeListener {
    pub async_task_tx: Sender<AsyncTask>,
}

#[derive(Debug, Error)]
pub enum FeatureCacheUpdaterError {
    #[error("Can not notify after update_latest_or_throw was called more than once successfully.")]
    CalledMoreThanOnce,
}

#[derive(Debug)]
pub struct FeatureCacheUpdater {
    feature_zk_node_path: String,
    maybe_notify_once: Option<u8>, // TODO: implement this with CountDownLatch
}

impl FeatureCacheUpdater {
    pub fn new(feature_zk_node_path: String) -> Self {
        Self { maybe_notify_once: None, feature_zk_node_path }
    }

    /// Updates the feature cache in FinalizedFeatureCache with the latest features read from the
    /// ZK node in featureZkNodePath. If the cache update is not successful, then, a suitable
    /// Error is returned
    /// NOTE: if a notifier was provided in the constructor, then, this method can be invoked
    /// exactly once successfully. A subsequent invocation will return CalledMoreThanOnce
    pub async fn update_latest_or_throw(
        &mut self,
        majordomo_tx: mpsc::Sender<AsyncTask>,
    ) -> Result<(), AsyncTaskError> {
        if let Some(notifier) = self.maybe_notify_once {
            if notifier != 1u8 {
                return Err(AsyncTaskError::FeatureCacheUpdater(
                    FeatureCacheUpdaterError::CalledMoreThanOnce,
                ));
            }
        }
        self.maybe_notify_once = None;

        debug!("Reading feature ZK node at path: {}", self.feature_zk_node_path);
        let (mayBeFeatureZNodeBytes, version) = zkClient.getDataAndVersion(featureZkNodePath)
        Ok(())
    }
}

#[derive(Debug)]
pub struct ChangeNotificationProcessor {
    pub name: String,
    /// A clonable tx to send messages to the processor.
    pub tx: mpsc::Sender<String>,
    /// A ZNode path where something has changed (deleted, created, updated)
    rx: mpsc::Receiver<String>,
    // queue: Vec<AsyncTask>,
}

impl ChangeNotificationProcessor {
    #[instrument]
    pub async fn do_work(&mut self, async_task_tx: mpsc::Sender<AsyncTask>) {
        // RAFKA: Originally this is a loop in a thread that is consantly reading from a
        // LinkedBlockingQueue, for now we are just gonna read whatever message is sent to us
        while let Some(message) = self.rx.recv().await {
            info!("do_work: Got request: {}", message);
        }
    }
}
