//! Finalized Feature Change Listener
//! kafka/server/FinalizedFeatureChangeListener.scala
//! Listens to changes in the ZK feature node, via the ZK client. Whenever a change notification
//! is received from ZK, the feature cache in FinalizedFeatureCache is asynchronously updated
//! to the latest features read from ZK. The cache updates are serialized through a single
//! notification processor thread.
use crate::majordomo::{AsyncTask, AsyncTaskError};
use crate::server::finalized_feature_cache::{
    FinalizedFeatureCache, FinalizedFeatureCacheAsyncTask,
};
use crate::zk::kafka_zk_client::KafkaZkClientAsyncTask;
use crate::zk::zk_data;
use thiserror::Error;
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info};
use tracing_attributes::instrument;
#[derive(Debug)]
pub struct FinalizedFeatureChangeListener {
    pub async_task_tx: Sender<AsyncTask>,
}

#[derive(Debug, Error)]
pub enum FeatureCacheUpdaterError {
    #[error("Can not notify after update_latest_or_throw was called more than once successfully.")]
    CalledMoreThanOnce,
    #[error(
        "FinalizedFeatureCache update failed due to invalid epoch in new finalized {0}. The \
         existing cache contents are {:1}"
    )]
    InvalidEpoch(String, String),
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

    /// Sends the Clear operation to the majordomo coordinator that holds the shared state.
    #[instrument]
    pub async fn clear_finalized_feature_cache(
        majordomo_tx: mpsc::Sender<AsyncTask>,
    ) -> Result<(), AsyncTaskError> {
        debug!("Sending the FinalizedFeatureCacheAsyncTask::Clear message");
        // Clear the finalized feature cache:
        majordomo_tx
            .send(AsyncTask::FinalizedFeatureCache(FinalizedFeatureCacheAsyncTask::Clear))
            .await?;
        Ok(())
    }

    /// Updates the feature cache in FinalizedFeatureCache with the latest features read from the
    /// ZK node in featureZkNodePath. If the cache update is not successful, then, a suitable
    /// Error is returned
    /// NOTE: if a notifier was provided in the constructor, then, this method can be invoked
    /// exactly once successfully. A subsequent invocation will return CalledMoreThanOnce
    #[instrument]
    pub async fn update_latest_or_throw(
        &mut self,
        majordomo_tx: mpsc::Sender<AsyncTask>,
    ) -> Result<(), AsyncTaskError> {
        // TODO: This data should be owned by the async_coordinator and no tx/rx would be needed
        // except from zookeeper change listeners to the async coordinator thread.
        if let Some(notifier) = self.maybe_notify_once {
            if notifier != 1u8 {
                return Err(AsyncTaskError::FeatureCacheUpdater(
                    FeatureCacheUpdaterError::CalledMoreThanOnce,
                ));
            }
        }
        self.maybe_notify_once = None;

        debug!("Requesting read on feature ZK node at path: {}", self.feature_zk_node_path);
        let (tx, mut rx) = oneshot::channel();
        majordomo_tx
            .send(AsyncTask::Zookeeper(KafkaZkClientAsyncTask::GetDataAndVersion(
                tx,
                self.feature_zk_node_path.clone(),
            )))
            .await?;
        let response = rx.try_recv()?;
        // From the original code:
        // There are 4 cases:
        //
        // - (empty dataBytes, valid version)
        // The empty dataBytes will fail FeatureZNode deserialization.
        //    FeatureZNode, when present in ZK, can not have empty contents.
        // - (non-empty dataBytes, valid version)
        // This is a valid case, and should pass FeatureZNode deserialization
        // if dataBytes contains valid data.
        // - (empty dataBytes, unknown version)
        // This is a valid case, and this can happen if the FeatureZNode
        // does not exist in ZK.
        // - (non-empty dataBytes, unknown version)
        // This case is impossible, since, KafkaZkClient.getDataAndVersion
        // API ensures that unknown version is returned only when the
        // ZK node is absent. Therefore dataBytes should be empty in such
        // a case.

        if response.version == zk_data::ZkVersion::UnknownVersion as i32 {
            info!("Feature ZK node at path: {} does not exist", self.feature_zk_node_path);
            Self::clear_finalized_feature_cache(majordomo_tx.clone()).await?;
            return Ok(());
        }
        if let Some(data) = response.data {
            match zk_data::FeatureZNode::decode(data) {
                Ok(val) => match val.status {
                    zk_data::FeatureZNodeStatus::Disabled => {
                        info!(
                            "Feature ZK node at path: {} is in disabled status.",
                            self.feature_zk_node_path
                        );
                        Self::clear_finalized_feature_cache(majordomo_tx.clone()).await?;
                    },
                    zk_data::FeatureZNodeStatus::Enabled => {
                        // RAFKA TODO: The ZK Reader and the FinalizedFeatureCache may be owned by
                        // the same thread and so no need for sending messages between them...
                        FinalizedFeatureCache::update_or_throw(val.features, response.version);
                    },
                    /* RAFKA NOTE: The original code checks for other possible values on the
                     * FeatureZNodeStatus However, if when decoded, the value
                     * is not Enabled/Disabled, then it already
                     * throws an Exception, we should verify if this can be built in ways other
                     * than decode that may result in really having another value */
                },
                Err(err) => {
                    error!(
                        "Unable to deserialize feature ZK node at path: {} error: {}",
                        self.feature_zk_node_path, err
                    );
                    Self::clear_finalized_feature_cache(majordomo_tx.clone()).await?;
                },
            }
        }
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
