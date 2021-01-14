use crate::server::kafka_config::KafkaConfig;
use crate::zk::kafka_zk_client::KafkaZkClient;
use crate::zookeeper::zoo_keeper_client;
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::debug;
use tracing::{error, info};
use tracing_attributes::instrument;

#[derive(Debug)]
pub enum ZookeeperBackendTask {
    EnsurePersistentPathExists(String),
}
#[derive(Debug)]
pub enum ZookeeperAsyncTask {
    EnsurePersistentPathExists(String),
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
}

// pub async make_sure_persistent_path_exists(String) -> Result<(), AsyncTaskError> {
//
//
// }

#[instrument]
pub async fn async_coordinator(kafka_config: KafkaConfig, mut rx: mpsc::Receiver<AsyncTask>) {
    debug!("async_coordinator: Starting");
    debug!("Connecting to ZooKeeper");
    let mut kafka_zk_client = KafkaZkClient::default();
    kafka_zk_client.create_chroot_path_if_set(&kafka_config.zk_connect);
    kafka_zk_client.connect().await.unwrap();
    while let Some(message) = rx.recv().await {
        debug!("async_coordinator: message: {:?}", message);
        match message {
            AsyncTask::Zookeeper(zk_task) => info!("coordinator zk_task is {:?}", zk_task),
            AsyncTask::Coordinator(coord_task) => {
                info!("coordinator zk_task is {:?}", coord_task);
                break;
            },
        }
    }
    error!("async_coordinator: Exiting.");
}
