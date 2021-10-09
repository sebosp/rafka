//! Majordomo - An async message handler
//! This module provides Multiple-Producer-Single-Consumer functionality to handles I/O operation
//! requests from different threads to networking resources.
//! The coordinator was built on thoughts of tokio versions before 1.0.
//! The name is of the module is a reference to ZMQ ZGuide Majordomo protocol, as it handles
//! messages in a similar fasion.
//! Several times an object has been found as volatile in the source code, it has been moved
//! ownership here so that there's its state can be shared across threads, for example:
//! - FeatureCacheUpdater from Finalize Feature Cache Change Listener
//! - supportedFeatures from SupportedFeatures
//! - ZookeeperClient (This was moved here because the zookeeper-async client cannot be shared
//! between threads.
//! - KafkaConfig may be moved here so that hot-reloads are possible and its state shared.
//! Currently the Config is read once and then copied, once for background kafka server and another
//! for Majordomo.
//! TODO:
//! - Add zookeeper watcher ties for the FeatureZNode

use crate::common::cluster_resource::ClusterResource;
use crate::common::internals::cluster_resource_listeners::ClusterResourceListeners;
use crate::log::log_manager::LogManagerError;
use crate::server::finalize_feature_change_listener::{
    FeatureCacheUpdater, FeatureCacheUpdaterAsyncTask, FeatureCacheUpdaterError,
};
use crate::server::kafka_config::KafkaConfig;
use crate::server::kafka_config::KafkaConfigError;
use crate::server::kafka_server::KafkaServerError;
use crate::server::log_failure_channel::LogDirFailureChannel;
use crate::server::supported_features::SupportedFeatures;
use crate::zk::kafka_zk_client::KafkaZkClientAsyncTask;
use crate::zk::zk_data::FeatureZNode;
use std::error::Error;
use std::thread;
use thiserror::Error;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tracing::debug;
use tracing::{error, info};
use tracing_attributes::instrument;

#[derive(Debug)]
pub enum CoordinatorTask {
    Shutdown,
}

/// `AsyncTask` contains message types that majordomo coordinator can work on
#[derive(Debug)]
pub enum AsyncTask {
    Zookeeper(KafkaZkClientAsyncTask),
    FinalizedFeatureCache(FeatureCacheUpdaterAsyncTask),
    Coordinator(CoordinatorTask),
    ClusterResource(ClusterResource),
}

#[derive(Error, Debug)]
pub enum AsyncTaskError {
    #[error("Zookeeper {0:?}")]
    ZooKeeper(#[from] zookeeper_async::ZkError),
    #[error("Majordomo Tokio Mpsc Send {0:?}")]
    MajordomoMpscSend(#[from] tokio::sync::mpsc::error::SendError<AsyncTask>),
    #[error("KafkaZk Tokio Mpsc Send {0:?}")]
    KafkaZkMpscSend(#[from] tokio::sync::mpsc::error::SendError<KafkaZkClientAsyncTask>),
    #[error("Tokio OneShot TryRecv {0:?}")]
    OneShotRecvErrorRecv(#[from] tokio::sync::oneshot::error::RecvError),
    #[error("ZooKeeperClient {0:?}")]
    ZooKeeperClient(#[from] crate::zookeeper::zoo_keeper_client::ZooKeeperClientError),
    #[error("KafkaZkClient {0:?}")]
    KafkaZkClient(#[from] crate::zk::kafka_zk_client::KafkaZkClientError),
    #[error("FeatureCacheUpdater {0:?}")]
    FeatureCacheUpdater(#[from] FeatureCacheUpdaterError),
    #[error("Invalid UTF-8 Sequence {0:?}")]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("KafkaServer {0:?}")]
    KafkaServer(#[from] KafkaServerError),
    #[error("KafkaConfig {0:?}")]
    KafkaConfig(#[from] KafkaConfigError),
    #[error("LogManager {0:?}")]
    LogManager(#[from] LogManagerError),
}

impl AsyncTaskError {
    /// `is_zookeeper_async_node_exists` checks that the error belogs to a variant of the crate
    /// zookeeper_async and that it is the NodeExists variant. This because it seems used a couple
    /// of times. This should be changed in the future to receive a T that we can try to cast to.
    pub fn is_zookeeper_async_node_exists(&self) -> bool {
        if let Some(err) = self.source() {
            if let Some(zk_err) = err.downcast_ref::<zookeeper_async::ZkError>() {
                match zk_err {
                    zookeeper_async::ZkError::NodeExists => return true,
                    _ => return false,
                }
            }
        }
        false
    }

    /// `is_zookeeper_async_no_node` checks that the error belogs to a variant of the crate
    /// zookeeper_async and that it is the NoNode variant.
    pub fn is_zookeeper_async_no_node(&self) -> bool {
        if let Some(err) = self.source() {
            if let Some(zk_err) = err.downcast_ref::<zookeeper_async::ZkError>() {
                match zk_err {
                    zookeeper_async::ZkError::NoNode => return true,
                    _ => return false,
                }
            }
        }
        false
    }
}

/// A MajordomoCoordinator that keeps the state to be shared across async tasks
#[derive(Debug)]
pub struct MajordomoCoordinator {
    kafka_config: KafkaConfig,
    feature_cache_updater: FeatureCacheUpdater,
    supported_features: SupportedFeatures,
    kafka_zk_tx: mpsc::Sender<KafkaZkClientAsyncTask>,
    tx: mpsc::Sender<AsyncTask>,
    rx: mpsc::Receiver<AsyncTask>,
    cluster_resource_listeners: ClusterResourceListeners,
    log_dir_failure_channel: LogDirFailureChannel,
}

impl MajordomoCoordinator {
    pub async fn new(
        kafka_config: KafkaConfig,
        kafka_zk_tx: mpsc::Sender<KafkaZkClientAsyncTask>,
        main_tx: mpsc::Sender<AsyncTask>,
        main_rx: mpsc::Receiver<AsyncTask>,
    ) -> Result<Self, AsyncTaskError> {
        let supported_features = SupportedFeatures::default();
        let feature_cache_updater = FeatureCacheUpdater::new(FeatureZNode::default_path());
        let cluster_resource_listeners = ClusterResourceListeners::default();
        let log_dir_failure_channel = LogDirFailureChannel::new(kafka_config.log_dirs.len());
        Ok(MajordomoCoordinator {
            kafka_config,
            tx: main_tx,
            rx: main_rx,
            feature_cache_updater,
            kafka_zk_tx,
            supported_features,
            cluster_resource_listeners,
            log_dir_failure_channel,
        })
    }

    #[instrument]
    pub async fn process_message_queue(&mut self) -> Result<(), AsyncTaskError> {
        debug!("majordomo coordinator: Preparing",);
        self.feature_cache_updater
            .init_or_throw(self.tx.clone(), self.kafka_config.zk_connection_timeout_ms.into())
            .await?;
        debug!("majordomo coordinator: Main loop starting");
        while let Some(message) = self.rx.recv().await {
            debug!("majordomo coordinator: message: {:?}", message);
            match message {
                AsyncTask::Zookeeper(task) => {
                    // Forward the task to KafkaZkClient coordinator
                    let kfk_zk_tx_copy = self.kafka_zk_tx.clone();
                    tokio::spawn(async move {
                        kfk_zk_tx_copy.send(task).await.unwrap();
                    });
                },
                AsyncTask::Coordinator(task) => {
                    info!("coordinator coord_task is {:?}", task);
                },
                AsyncTask::FinalizedFeatureCache(task) => {
                    info!("finalized feature cache task is {:?}", task);
                    FeatureCacheUpdaterAsyncTask::process_task(
                        &mut self.feature_cache_updater,
                        &mut self.supported_features,
                        self.tx.clone(),
                        task,
                    )
                    .await?;
                },
                AsyncTask::ClusterResource(cluster_resource) => {
                    self.notify_cluster_listeners(cluster_resource).await?;
                },
            }
        }
        self.kafka_zk_tx.send(KafkaZkClientAsyncTask::Shutdown).await?;
        error!("majordomo coordinator: Exiting.");
        Ok(())
    }

    /// Creates a thread that will handle tasks that should be forwarded to zookeeper
    /// # Arguments
    /// * `kafka_config` -  a KafkaConfig that contains the zookeeper endpoints/timeouts
    /// * `kfk_zk_tx` - A tx channel to send zookeeper data requests to the KafkaZkClient that runs
    /// on the foreground thread
    /// * `main_rx` - A rx channel that will be used by the coordinator internally to communicate
    /// with other tasks.
    #[instrument]
    pub async fn init_coordinator_thread(
        kafka_config: KafkaConfig,
        kfk_zk_tx: mpsc::Sender<KafkaZkClientAsyncTask>,
        main_tx: mpsc::Sender<AsyncTask>,
        main_rx: mpsc::Receiver<AsyncTask>,
    ) -> Result<thread::JoinHandle<()>, AsyncTaskError> {
        let current_tokio_handle = Handle::current();
        let coordinator_thread = ::std::thread::Builder::new()
            .name("Majordomo Coordinator I/O".to_owned())
            .spawn(move || {
                current_tokio_handle.spawn(async move {
                    let mut majordomo_coordinator =
                        Self::new(kafka_config, kfk_zk_tx, main_tx, main_rx)
                            .await
                            .expect("Unable to create Majordomo Coordinator");
                    majordomo_coordinator
                        .process_message_queue()
                        .await
                        .expect("Majordomo coordinator exited with error.");
                });
            })
            .expect("Unable to start Majordomo Coordinator async I/O thread");
        Ok(coordinator_thread)
    }

    #[instrument]
    pub async fn shutdown(tx: mpsc::Sender<AsyncTask>) {
        tx.send(AsyncTask::Coordinator(CoordinatorTask::Shutdown)).await.unwrap();
    }

    pub async fn notify_cluster_listeners(
        &self,
        cluster_resource: ClusterResource,
    ) -> Result<(), AsyncTaskError> {
        self.cluster_resource_listeners.on_update(cluster_resource).await?;
        Ok(())
    }
}
