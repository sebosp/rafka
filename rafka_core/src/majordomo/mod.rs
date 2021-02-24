//! Majordomo - An async message handler
//! This module handles creation of Multiple-Producer-Single-Consumer that handles I/O operation
//! requests from different threads to networking  resources.
//! The coordinator was built on thoughts of tokio versions before 1.0.
//! The name is of the module is a reference to ZMQ ZGuide Majordomo protocol, as it handles
//! messages in a similar fasion.

use crate::server::finalize_feature_change_listener::FeatureCacheUpdaterError;
use crate::server::finalized_feature_cache::FinalizedFeatureCacheAsyncTask;
use crate::server::kafka_config::KafkaConfig;
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
    #[error("ZookeeperError {0:?}")]
    ZooKeeper(#[from] zookeeper_async::ZkError),
    #[error("Tokio Mpsc SendError {0:?}")]
    MpscSend(#[from] tokio::sync::mpsc::error::SendError<AsyncTask>),
    #[error("Tokio OneShot SendError {0:?}")]
    OneShotTryRecv(#[from] tokio::sync::oneshot::error::TryRecvError),
    #[error("ZooKeeperClientError {0:?}")]
    ZooKeeperClient(#[from] crate::zookeeper::zoo_keeper_client::ZooKeeperClientError),
    #[error("KafkaZkClientError {0:?}")]
    KafkaZkClient(#[from] crate::zk::kafka_zk_client::KafkaZkClientError),
    #[error(" FeatureCacheUpdaterError {0:?}")]
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

#[instrument]
pub async fn async_coordinator(
    kafka_config: KafkaConfig,
    mut rx: mpsc::Receiver<AsyncTask>,
) -> Result<(), AsyncTaskError> {
    debug!("async_coordinator: Preparing");
    let init_time = Instant::now();
    let mut kafka_zk_client = KafkaZkClient::build(
        &kafka_config.zk_connect,
        &kafka_config,
        Some(String::from("Async Coordinator")),
        init_time,
        None,
    );
    debug!("async_coordinator: Main loop starting");
    while let Some(message) = rx.recv().await {
        debug!("async_coordinator: message: {:?}", message);
        match message {
            AsyncTask::Zookeeper(task) => {
                KafkaZkClientAsyncTask::process_task(&mut kafka_zk_client, &kafka_config, task)
                    .await?
            },
            AsyncTask::Coordinator(coord_task) => {
                info!("coordinator coord_task is {:?}", coord_task);
                break;
            },
        }
    }
    kafka_zk_client.close().await.unwrap();
    error!("async_coordinator: Exiting.");
    Ok(())
}

#[instrument]
pub async fn shutdown(tx: mpsc::Sender<AsyncTask>) {
    tx.send(AsyncTask::Coordinator(CoordinatorTask::Shutdown)).await;
}
