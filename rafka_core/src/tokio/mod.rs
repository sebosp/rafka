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

/// `AsyncTask` contains message types that async_coordinator can work on
#[derive(Debug)]
pub enum AsyncTask {
    Zookeeper(ZookeeperAsyncTask),
}

#[derive(Error, Debug)]
pub enum AsyncTaskError {
    #[error("ZookeeperError {0:?}")]
    ZooKeeperError(#[from] crate::zookeeper::zoo_keeper_client::ZooKeeperClientError),
}

// pub async make_sure_persistent_path_exists(String) -> Result<(), AsyncTaskError> {
//
//
// }

#[instrument]
pub async fn async_coordinator(mut rx: mpsc::Receiver<AsyncTask>) {
    let mut zookeeper_client = zoo_keeper_client::ZooKeeperClient::default();
    zookeeper_client.connect().await.unwrap();
    debug!("async_coordinator: Starting");
    while let Some(message) = rx.recv().await {
        debug!("async_coordinator: message: {:?}", message);
        match message {
            AsyncTask::Zookeeper(zk_task) => info!("coordinator zk_task is {:?}", zk_task),
        }
    }
    error!("async_coordinator: Exiting. This shouldn't happen");
}
