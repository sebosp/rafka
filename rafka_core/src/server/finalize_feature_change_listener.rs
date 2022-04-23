//! Finalized Feature Change Listener
//! kafka/server/FinalizedFeatureChangeListener.scala
//! Listens to changes in the ZK feature node, via the ZK client. Whenever a change notification
//! is received from ZK, the feature cache in FinalizedFeatureCache is asynchronously updated
//! to the latest features read from ZK. The cache updates are serialized through a single
//! notification processor thread.
//! RAFKA Specific:
//! In the original code, a volatile var contains the finalized feature cache and seems to be
//! accessed mostly from here. In this version, the feature cache is owned by the
//! FeatureCacheUpdater and the FeatureCacheUpdater itself is owned by the Majordomo coordinator.
use crate::majordomo::{AsyncTask, AsyncTaskError};
use crate::server::finalized_feature_cache::FinalizedFeatureCache;
use crate::server::supported_features::SupportedFeatures;
use crate::zk::kafka_zk_client::{
    GetDataAndVersionResponse, KafkaZkClient, KafkaZkClientAsyncTask,
};
use crate::zk::zk_data;
use thiserror::Error;
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, trace, warn};
use tracing_attributes::instrument;
#[derive(Debug)]
pub struct FinalizedFeatureChangeListener {
    pub async_task_tx: Sender<AsyncTask>,
}

#[derive(Debug, Error)]
pub enum FeatureCacheUpdaterError {
    #[error(
        "FinalizedFeatureCache update failed due to invalid epoch in new finalized {0}. The \
         existing cache contents are {:1}"
    )]
    InvalidEpoch(String, String),
    #[error(
        "FinalizedFeatureCache update failed since feature compatibility checks failed! Supported \
         {0} has incompatibilities with the latest {1}."
    )]
    Incompatible(String, String),
    #[error("Expected waitOnceForCacheUpdateMs > 0, but provided: {0}")]
    InvalidWaitForCacheValue(i64),
}

#[derive(Debug)]
pub struct FeatureCacheUpdater {
    feature_zk_node_path: String,
    finalized_feature_cache: FinalizedFeatureCache,
}

impl FeatureCacheUpdater {
    pub fn new(feature_zk_node_path: String) -> Self {
        Self { feature_zk_node_path, finalized_feature_cache: FinalizedFeatureCache::default() }
    }

    /// Clears the finalized feature cache
    pub fn clear_finalized_feature_cache(&mut self) {
        self.finalized_feature_cache.clear();
    }

    /// Spawns a tokio task to request GetDataAndVersion from Zookeeper for the /feature znode, this
    /// request is done to the MajorDomoCoordinator.
    /// When the data is received, it is also sent back to the MajordomoCoordinator.
    /// This is done to prevent the data request from zookeeper to block the majordomo message
    /// processing loop.
    #[instrument]
    pub async fn req_feature_latest_data_and_version(
        feature_zk_node_path: String,
        majordomo_tx: mpsc::Sender<AsyncTask>,
    ) -> Result<(), AsyncTaskError> {
        // TODO: Currently this unwraps errors, figure out how to return AsyncErrors from tasks
        tokio::spawn(async move {
            let (tx, rx) = oneshot::channel();
            match KafkaZkClient::req_get_data_and_version(
                majordomo_tx.clone(),
                tx,
                feature_zk_node_path,
            )
            .await
            {
                Ok(()) => trace!("Successfully requested GetDataAndVersion through KafkaZkClient"),
                // RAFKA TODO: Send a Result through tx/rx so that we can send Errors to callers
                Err(err) => {
                    panic!("Error requesting GetDataAndVersion through KafkaZkClient: {:?}", err)
                },
            };
            let response = rx.await.unwrap();
            match FeatureCacheUpdaterAsyncTask::rep_update_latest(majordomo_tx, response).await {
                Ok(()) => trace!("Successfully replied GetDataAndVersion to caller"),
                // RAFKA TODO: Send a Result through tx/rx so that we can send Errors to callers
                Err(err) => panic!("Error replying GetDataAndVersion to caller: {:?}", err),
            };
        });
        Ok(())
    }

    /// Updates the feature cache in FinalizedFeatureCache with the latest features read from the
    /// ZK node in featureZkNodePath. If the cache update is not successful, then, a suitable
    /// Error is returned
    /// NOTE: if a notifier was provided in the constructor, then, this method can be invoked
    /// once
    pub fn update_latest_or_throw(
        &mut self,
        supported_features: &mut SupportedFeatures,
        data_and_version: GetDataAndVersionResponse,
    ) -> Result<(), AsyncTaskError> {
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

        if data_and_version.version == zk_data::ZkVersion::UnknownVersion as i32 {
            info!("Feature ZK node at path: {} does not exist", self.feature_zk_node_path);
            self.finalized_feature_cache.clear();
            return Ok(());
        }
        if let Some(data) = data_and_version.data {
            match zk_data::FeatureZNode::decode(data) {
                Ok(val) => match val.status {
                    zk_data::FeatureZNodeStatus::Disabled => {
                        info!(
                            "Feature ZK node at path: {} is in disabled status.",
                            self.feature_zk_node_path
                        );
                        self.finalized_feature_cache.clear();
                    },
                    zk_data::FeatureZNodeStatus::Enabled => {
                        // RAFKA SPECIFIC: The supported and finalized features are owned by the
                        // same coordinator thread and so no need to make them shared across
                        // threads.
                        self.finalized_feature_cache.update_or_throw(
                            supported_features,
                            val.features,
                            data_and_version.version,
                        )?;
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
                    self.finalized_feature_cache.clear();
                },
            }
        }
        Ok(())
    }

    /// From the Trait ZNodeChangeHandler, should be made trait once the trait fns can be async
    #[instrument]
    pub async fn handle_creation(
        &mut self,
        supported_features: &mut SupportedFeatures,
        majordomo_tx: mpsc::Sender<AsyncTask>,
    ) -> Result<(), AsyncTaskError> {
        // RAFKA TODO: Tie to zookeeper watcher
        info!("Feature ZK node created at path: {}", self.feature_zk_node_path);
        FeatureCacheUpdater::req_feature_latest_data_and_version(
            self.feature_zk_node_path.clone(),
            majordomo_tx,
        )
        .await
    }

    /// From the Trait ZNodeChangeHandler, should be made trait once the trait fns can be async
    #[instrument]
    pub async fn handle_data_change(
        &mut self,
        supported_features: &mut SupportedFeatures,
        majordomo_tx: mpsc::Sender<AsyncTask>,
    ) -> Result<(), AsyncTaskError> {
        info!("Feature ZK node updated at path: {}", self.feature_zk_node_path);
        FeatureCacheUpdater::req_feature_latest_data_and_version(
            self.feature_zk_node_path.clone(),
            majordomo_tx,
        )
        .await
    }

    /// From the Trait ZNodeChangeHandler, should be made trait once the trait fns can be async
    #[instrument]
    pub async fn handle_deletion(
        &mut self,
        supported_features: &mut SupportedFeatures,
        majordomo_tx: mpsc::Sender<AsyncTask>,
    ) -> Result<(), AsyncTaskError> {
        warn!("Feature ZK node deleted at path: {}", self.feature_zk_node_path);
        // This event may happen, rarely (ex: ZK corruption or operational error).
        // In such a case, we prefer to just log a warning and treat the case as if the node is
        // absent, and populate the FinalizedFeatureCache with empty finalized features.
        FeatureCacheUpdater::req_feature_latest_data_and_version(
            self.feature_zk_node_path.clone(),
            majordomo_tx,
        )
        .await
    }

    /// From the Trait StateChangeHandler, should be made trait once the trait fns can be async
    #[instrument]
    pub async fn after_initializing_session(
        &mut self,
        supported_features: &mut SupportedFeatures,
        majordomo_tx: mpsc::Sender<AsyncTask>,
    ) -> Result<(), AsyncTaskError> {
        info!("Feature ZK node After Initializing Session: {}", self.feature_zk_node_path);
        FeatureCacheUpdater::req_feature_latest_data_and_version(
            self.feature_zk_node_path.clone(),
            majordomo_tx,
        )
        .await
    }

    /// This method initializes the feature ZK node change listener. Optionally, it also ensures to
    /// update the FinalizedFeatureCache once with the latest contents of the feature ZK node
    /// (if the node exists). This step helps ensure that feature incompatibilities (if any) in
    /// brokers are conveniently detected before the initOrThrow() method returns to the caller.
    /// If feature incompatibilities are detected, this method will throw an Exception to the
    /// caller, and the Broker will exit eventually.
    /// # Arguments
    /// * `wait_once_for_cache_update_ms` number of milli seconds to wait for feature cache to be
    ///   updated once.
    /// If this parameter <= 0, no wait operation happens.
    #[instrument]
    pub async fn init_or_throw(
        &mut self,
        majordomo_tx: mpsc::Sender<AsyncTask>,
        wait_once_for_cache_update_ms: i64,
    ) -> Result<(), AsyncTaskError> {
        if wait_once_for_cache_update_ms <= 0 {
            return Err(AsyncTaskError::from(FeatureCacheUpdaterError::InvalidWaitForCacheValue(
                wait_once_for_cache_update_ms,
            )));
        }
        // Register a tx channel that receives the updates from the ZNode
        majordomo_tx
            .send(AsyncTask::Zookeeper(KafkaZkClientAsyncTask::RegisterFeatureChange(
                majordomo_tx.clone(),
            )))
            .await?;
        majordomo_tx
            .send(AsyncTask::FinalizedFeatureCache(FeatureCacheUpdaterAsyncTask::TriggerChange))
            .await?;
        // RAFKA TODO: The original code waits certain millis for the cache to be updated
        // ensureCacheUpdateOnce.awaitUpdateOrThrow(waitOnceForCacheUpdateMs)
        // In the current setup, the Trigger change would cause the same, but it won't wait until
        // the Trigger is fulfilled. Maybe a Option<oneshot::channel> can be used for this if it
        // turns out we need it.
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

/// Majordomo Coordinator handling of async tasks. This may not be needed as the supported and
/// finalized features are owned by the majordomo coordinator
#[derive(Debug)]
pub enum FeatureCacheUpdaterAsyncTask {
    Clear,
    TriggerChange,
    UpdateLatest(GetDataAndVersionResponse),
}

impl FeatureCacheUpdaterAsyncTask {
    #[instrument]
    pub async fn process_task(
        cache: &mut FeatureCacheUpdater,
        supported_features: &mut SupportedFeatures,
        majordomo_tx: mpsc::Sender<AsyncTask>,
        task: Self,
    ) -> Result<(), AsyncTaskError> {
        match task {
            Self::Clear => cache.clear_finalized_feature_cache(),
            Self::TriggerChange => {
                let tx_cp = majordomo_tx.clone();
                let feature_zk_node_path = cache.feature_zk_node_path.clone();
                FeatureCacheUpdater::req_feature_latest_data_and_version(
                    feature_zk_node_path,
                    tx_cp,
                )
                .await
                .unwrap();
            },
            Self::UpdateLatest(data_and_version) => {
                cache.update_latest_or_throw(supported_features, data_and_version)?
            },
        }
        Ok(())
    }

    /// rep_update_latest Replies/Sends an UpdateLatest response to the MajorDomoCoordinator
    pub async fn rep_update_latest(
        majordomo_tx: mpsc::Sender<AsyncTask>,
        res: GetDataAndVersionResponse,
    ) -> Result<(), AsyncTaskError> {
        Ok(majordomo_tx
            .send(AsyncTask::FinalizedFeatureCache(FeatureCacheUpdaterAsyncTask::UpdateLatest(res)))
            .await?)
    }
}
