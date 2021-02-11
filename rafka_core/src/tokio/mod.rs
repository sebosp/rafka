use crate::server::kafka_config::KafkaConfig;
use crate::zk::kafka_zk_client::KafkaZkClient;
use std::error::Error;
use std::time::Instant;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tracing::debug;
use tracing::{error, info};
use tracing_attributes::instrument;

#[derive(Debug)]
pub enum ZookeeperBackendTask {
    EnsurePersistentPathExists(String),
}
#[derive(Debug)]
pub enum ZookeeperAsyncTask {
    Init,
    EnsurePersistentPathExists(String),
    GetDataAndVersion(oneshot::Sender<(Data, Version)>, String),
}
impl ZookeeperAsyncTask {
    pub async fn ProcessTask(
        kafka_zk_client: &mut KafkaZkClient,
        kafka_config: &KafkaConfig,
        zk_task: Self,
    ) {
        info!("coordinator zk_task is {:?}", zk_task);
        match zk_task {
            Self::Init => kafka_zk_client.init(&kafka_config).await.unwrap(),
            Self::GetDataAndVersion(tx, znode_path) => {
                kafka_zk_client.get_data_and_version(tx, znode_path)
            },
            _ => unimplemented!("Task not implemented"),
        }
    }
}

#[derive(Debug)]
pub enum CoordinatorTask {
    Shutdown,
}

/// `AsyncTask` contains message types that async_coordinator can work on
#[derive(Debug)]
pub enum AsyncTask {
    Zookeeper(ZookeeperAsyncTask),
    Coordinator(CoordinatorTask),
}

#[derive(Error, Debug)]
pub enum AsyncTaskError {
    #[error("ZookeeperError {0:?}")]
    ZooKeeperError(#[from] zookeeper_async::ZkError),
    #[error("Mpsc SendError {0:?}")]
    MpscSendError(#[from] tokio::sync::mpsc::error::SendError<AsyncTask>),
    #[error("ZooKeeperClientError {0:?}")]
    ZooKeeperClientError(#[from] crate::zookeeper::zoo_keeper_client::ZooKeeperClientError),
    #[error("KafkaZkClientError {0:?}")]
    KafkaZkClientError(#[from] crate::zk::kafka_zk_client::KafkaZkClientError),
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
pub async fn async_coordinator(kafka_config: KafkaConfig, mut rx: mpsc::Receiver<AsyncTask>) {
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
            AsyncTask::Zookeeper(zk_task) => {
                ZookeeperAsyncTask::ProcessTask(&mut kafka_zk_client, &kafka_config, zk_task).await
            },
            AsyncTask::Coordinator(coord_task) => {
                info!("coordinator zk_task is {:?}", coord_task);
                break;
            },
        }
    }
    kafka_zk_client.close().await.unwrap();
    error!("async_coordinator: Exiting.");
}

#[instrument]
pub async fn shutdown(tx: mpsc::Sender<AsyncTask>) {
    tx.send(AsyncTask::Coordinator(CoordinatorTask::Shutdown)).await;
}
