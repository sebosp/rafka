//! Provides higher level Kafka-specific operations on top of the pipelined
//! [[kafka::zookeeper::ZooKeeperClient]]. ( TODO RAFKA version may not be pipelined?)
//! core/src/main/scala/kafka/zk/KafkaZkClient.scala
//!
//! Implementation note: this class includes methods for various components (Controller, Configs,
//! Old Consumer, etc.) and returns instances of classes from the calling packages in some cases.
//! This is not ideal, but it made it easier to migrate away from `ZkUtils` (since removed). We
//! should revisit this. We should also consider whether a monolithic [[kafka.zk.ZkData]] is the way
//! to go.

// RAFKA TODO: The documentation may not be accurate anymore.

use crate::server::kafka_config::KafkaConfig;
use crate::tokio::{AsyncTask, AsyncTaskError};
use crate::zk::zk_data::ZkData;
use crate::zookeeper::zoo_keeper_client::ZKClientConfig;
use crate::zookeeper::zoo_keeper_client::ZooKeeperClient;
use std::error::Error;
use std::time::Instant;
use tracing::{debug, error};
use zookeeper_async::CreateMode;

#[derive(thiserror::Error, Debug)]
pub enum KafkaZkClientError {
    #[error("InvalidPath")]
    InvalidPath,
    #[error("Create zookeeper path:  already exists")]
    CreatePathExists,
}

#[derive(Debug)]
pub struct KafkaZkClient {
    zoo_keeper_client: ZooKeeperClient,
    // is_secure: bool,
    time: Instant,
    // This variable holds the Zookeeper session id at the moment a Broker gets registered in
    // Zookeeper and the subsequent updates of the session id. It is possible that the session
    // id changes over the time for 'Session expired'. This code is part of the work around
    // done in the KAFKA-7165, once ZOOKEEPER-2985 is complete, this code must be deleted.
    current_zookeeper_session_id: i32,
    zk_data: ZkData,
}

impl Default for KafkaZkClient {
    fn default() -> Self {
        KafkaZkClient {
            zoo_keeper_client: ZooKeeperClient::default(),
            time: Instant::now(),
            current_zookeeper_session_id: -1i32,
            zk_data: ZkData::default(),
        }
    }
}

impl KafkaZkClient {
    pub fn new(zoo_keeper_client: ZooKeeperClient, time: Instant) -> Self {
        KafkaZkClient {
            zoo_keeper_client,
            time,
            current_zookeeper_session_id: -1i32,
            zk_data: ZkData::default(),
        }
    }

    /// The builder receives params to create the ZookeeperClient and builds a local instance.
    /// in java this maps to the apply() of the KafkaZkClient Object
    pub fn build(
        zk_connect: &str,
        kafka_config: &KafkaConfig,
        name: Option<String>,
        time: std::time::Instant,
        zk_client_config: Option<ZKClientConfig>,
    ) -> Self {
        let zoo_keeper_client = ZooKeeperClient::new(
            zk_connect.to_string(),
            kafka_config.zk_session_timeout_ms,
            kafka_config.zk_connection_timeout_ms,
            kafka_config.zk_max_in_flight_requests,
            time,
            name,
            zk_client_config,
        );
        KafkaZkClient::new(zoo_keeper_client, time)
    }

    /// `parent_path` returns the path from the start until the last slash.
    /// If there is no slash, an error is returned about an InvalidPath
    fn parent_path(path: &str) -> Result<String, KafkaZkClientError> {
        // XXX: Change this to return a slice to avoid creating strings for no good reason.
        match path.rfind("/") {
            Some(idx) => {
                let (parent, _) = path.split_at(idx);
                Ok(parent.to_string())
            },
            None => {
                error!("Invalid path provided: {}", path);
                Err(KafkaZkClientError::InvalidPath)
            },
        }
    }

    /// zookeeper_async::proto::CreateRequest is private (proto module is not pub)
    async fn retry_request_until_connected(
        create_request: String,
    ) -> Result<(), KafkaZkClientError> {
        unimplemented!()
    }

    /// `create_looped_0` is a replacement for createRecursive0 from java code.
    async fn create_looped_0(&mut self, path: &str) -> Result<(), AsyncTaskError> {
        // NOTE: This function used to be createRecursive0, but has been transfromed into a loop
        let mut path = path.to_string();
        loop {
            let create_request = self
                .zoo_keeper_client
                .create_request(
                    &path,
                    vec![],
                    self.zk_data.default_acls(&path),
                    CreateMode::Persistent,
                )
                .await;
            if let Err(err) = create_request {
                if let Some(err) = err.source() {
                    if let Some(zk_err) = err.downcast_ref::<zookeeper_async::ZkError>() {
                        match zk_err {
                            zookeeper_async::ZkError::NoNode => {
                                path = KafkaZkClient::parent_path(&path)?;
                                continue;
                            },
                            zookeeper_async::ZkError::NodeExists => {
                                break;
                            },
                            _ => return Err(crate::tokio::AsyncTaskError::ZooKeeperError(*zk_err)),
                        }
                    }
                }
                return Err(err);
            }
        }
        Ok(())
    }

    /// `create_looped` is a replacement for createRecursive from java code.
    pub async fn create_looped(
        &mut self,
        path: &str,
        data: Vec<u8>,
        fail_on_exists: bool,
    ) -> Result<(), AsyncTaskError> {
        let create_request = self
            .zoo_keeper_client
            .create_request(path, vec![], self.zk_data.default_acls(path), CreateMode::Persistent)
            .await;
        if let Err(err) = create_request {
            if let Some(err) = err.source() {
                if let Some(zk_err) = err.downcast_ref::<zookeeper_async::ZkError>() {
                    match zk_err {
                        zookeeper_async::ZkError::NodeExists => {
                            if fail_on_exists {
                                return Err(AsyncTaskError::KafkaZkClientError(
                                    KafkaZkClientError::CreatePathExists,
                                ));
                            }
                        },
                        zookeeper_async::ZkError::NoNode => {
                            if let Err(err) = self.create_looped_0(path).await {
                                if fail_on_exists || !err.is_zookeeper_async_node_exists() {
                                    return Err(crate::tokio::AsyncTaskError::KafkaZkClientError(
                                        KafkaZkClientError::CreatePathExists,
                                    ));
                                }
                            }
                        },
                        _ => return Err(crate::tokio::AsyncTaskError::ZooKeeperError(*zk_err)),
                    }
                }
            }
        }
        Ok(())
    }

    /// `make_sure_persistent_path_exists` Make sure a persistent path exists in ZK.
    pub async fn make_sure_persistent_path_exists(
        &mut self,
        path: &str,
    ) -> Result<(), AsyncTaskError> {
        self.create_looped(path, vec![], false).await
    }

    pub async fn connect(&mut self) -> Result<(), AsyncTaskError> {
        self.zoo_keeper_client.connect().await
    }

    /// `create_chroot_path_if_set` is called from the async coordinator before the real connection
    /// done to zookeeper so that the chroot path is checked as persistent.
    pub async fn create_chroot_path_if_set(
        &mut self,
        zk_connect: &str,
    ) -> Result<(), AsyncTaskError> {
        match zk_connect.find('/') {
            Some(chroot_index) => {
                let (zk_connect_host_port, zk_chroot) = zk_connect.split_at(chroot_index);
                debug!("Zookeeper host:port list: {}, chroot: {}", zk_connect_host_port, zk_chroot);
                let mut temp_kafka_zk_client = KafkaZkClient {
                    zoo_keeper_client: self.zoo_keeper_client.clone_uninit(),
                    time: self.time,
                    current_zookeeper_session_id: self.current_zookeeper_session_id,
                    zk_data: ZkData::default(),
                };
                temp_kafka_zk_client.connect().await?;
                temp_kafka_zk_client.make_sure_persistent_path_exists(zk_chroot).await?;
            },
            None => {
                debug!(
                    "Zookeeper path does not contain chroot, no need to make sure the path exists"
                );
            },
        };
        Ok(())
    }
}
