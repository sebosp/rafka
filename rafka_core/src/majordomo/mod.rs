//! Majordomo - An async message handler
//! This module provides Multiple-Producer-Single-Consumer functionality to handles I/O operation
//! requests from different threads to networking resources.
//! The coordinator was built on thoughts of tokio versions before 1.0.
//! The name is of the module is a reference to ZMQ ZGuide Majordomo protocol, as it handles
//! messages in a similar fasion.
//! Several times an object has been found as volatile in the source code, it has been moved
//! ownership here so that there's its state can be shared across threads, for example:
//! - featuresAndEpoch from FinalizedFeatureCache
//! - supportedFeatures from SupportedFeatures
//! - ZookeeperClient (This was moved here because the zookeeper-async client cannot be shared
//! between threads.
//! - KafkaConfig may be moved here so that hot-reloads are possible and its state shared.
//! Currently the Config is read once and then copied, once for background kafka server and another
//! for Majordomo.
//! TODO:
//! - Add zookeeper watcher ties for the FeatureZNode

use crate::server::finalize_feature_change_listener::FeatureCacheUpdaterError;
use crate::server::finalized_feature_cache::{
    FinalizedFeatureCache, FinalizedFeatureCacheAsyncTask,
};
use crate::server::kafka_config::KafkaConfig;
use crate::server::supported_features::SupportedFeatures;
use crate::zk::kafka_zk_client::{KafkaZkClient, KafkaZkClientAsyncTask};
use std::error::Error;
use std::time::Instant;
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::debug;
use tracing::{error, info};
use tracing_attributes::instrument;

#[derive(Debug)]
pub enum CoordinatorTask {
    Shutdown,
}

/// `AsyncTask` contains message types that async_coordinator can work on
#[derive(Debug)]
pub enum AsyncTask {
    Zookeeper(KafkaZkClientAsyncTask),
    FinalizedFeatureCache(FinalizedFeatureCacheAsyncTask),
    Coordinator(CoordinatorTask),
}

#[derive(Error, Debug)]
pub enum AsyncTaskError {
    #[error("Zookeeper {0:?}")]
    ZooKeeper(#[from] zookeeper_async::ZkError),
    #[error("Tokio Mpsc Send {0:?}")]
    MpscSend(#[from] tokio::sync::mpsc::error::SendError<AsyncTask>),
    #[error("Tokio OneShot TryRecv {0:?}")]
    OneShotTryRecv(#[from] tokio::sync::oneshot::error::TryRecvError),
    #[error("ZooKeeperClient {0:?}")]
    ZooKeeperClient(#[from] crate::zookeeper::zoo_keeper_client::ZooKeeperClientError),
    #[error("KafkaZkClient {0:?}")]
    KafkaZkClient(#[from] crate::zk::kafka_zk_client::KafkaZkClientError),
    #[error("FeatureCacheUpdater {0:?}")]
    FeatureCacheUpdater(#[from] FeatureCacheUpdaterError),
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
}

/// A Coordinator that keeps the state to be shared across async tasks
#[derive(Debug)]
pub struct Coordinator {
    kafka_config: KafkaConfig,
    finalized_feature_cache: FinalizedFeatureCache,
    supported_features: SupportedFeatures,
    pub tx: mpsc::Sender<AsyncTask>,
    rx: mpsc::Receiver<AsyncTask>,
}

impl Coordinator {
    pub fn new(kafka_config: KafkaConfig) -> Self {
        let (tx, rx) = mpsc::channel(4_096); // TODO: Magic number removal
        let finalized_feature_cache = FinalizedFeatureCache::default();
        let supported_features = SupportedFeatures::default();
        Coordinator { kafka_config, finalized_feature_cache, tx, rx, supported_features }
    }

    pub fn main_tx(&self) -> mpsc::Sender<AsyncTask> {
        self.tx.clone()
    }

    #[instrument]
    pub async fn async_coordinator(&mut self) -> Result<(), AsyncTaskError> {
        debug!("async_coordinator: Preparing");
        let init_time = Instant::now();
        let mut kafka_zk_client = KafkaZkClient::build(
            &self.kafka_config.zk_connect,
            &self.kafka_config,
            Some(String::from("Async Coordinator")),
            init_time,
            None,
        );
        debug!("async_coordinator: Main loop starting");
        while let Some(message) = self.rx.recv().await {
            debug!("async_coordinator: message: {:?}", message);
            match message {
                AsyncTask::Zookeeper(task) => {
                    KafkaZkClientAsyncTask::process_task(
                        &mut kafka_zk_client,
                        &self.kafka_config,
                        task,
                    )
                    .await?
                },
                AsyncTask::Coordinator(task) => {
                    info!("coordinator coord_task is {:?}", task);
                },
                AsyncTask::FinalizedFeatureCache(task) => {
                    info!("finalized feature cache task is {:?}", task);
                    FinalizedFeatureCacheAsyncTask::process_task(
                        &mut self.finalized_feature_cache,
                        &mut self.supported_features,
                        task,
                    )
                    .await?;
                },
            }
        }
        kafka_zk_client.close().await.unwrap();
        error!("async_coordinator: Exiting.");
        Ok(())
    }

    #[instrument]
    pub async fn shutdown(tx: mpsc::Sender<AsyncTask>) {
        tx.send(AsyncTask::Coordinator(CoordinatorTask::Shutdown)).await.unwrap();
    }
}
