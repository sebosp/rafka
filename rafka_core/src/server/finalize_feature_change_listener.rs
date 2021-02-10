//! Finalized Feature Change Listener
//! kafka/server/FinalizedFeatureChangeListener.scala
//! Listens to changes in the ZK feature node, via the ZK client. Whenever a change notification
//! is received from ZK, the feature cache in FinalizedFeatureCache is asynchronously updated
//! to the latest features read from ZK. The cache updates are serialized through a single
//! notification processor thread.
use crate::tokio::{AsyncTask, AsyncTaskError, CoordinatorTask, ZookeeperAsyncTask};
use thiserror::Error;
use tokio::sync::mpsc::{channel, Sender};
use tracing::debug;
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
    /// exception is raised.
    ///
    /// NOTE: if a notifier was provided in the constructor, then, this method can be invoked
    /// exactly once successfully. A subsequent invocation will raise an exception.
    ///
    /// @throws   IllegalStateException, if a non-empty notifier was provided in the constructor,
    /// and           this method is called again after a successful previous invocation.
    /// @throws   FeatureCacheUpdateException, if there was an error in updating the
    ///           FinalizedFeatureCache.
    fn update_latest_or_throw(&mut self) -> Result<(), FeatureCacheUpdaterError> {
        if let Some(notifier) = self.maybe_notify_once {
            if notifier != 1u8 {
                return Err(FeatureCacheUpdaterError::CalledMoreThanOnce);
            }
        }

        debug!("Reading feature ZK node at path: {}", self.feature_zk_node_path);
        let (mayBeFeatureZNodeBytes, version) = zkClient.getDataAndVersion(featureZkNodePath)
        Ok(())
    }
}
