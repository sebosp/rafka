//! Coordinates majordomo requests of type KafkaZkClientAsyncTask, it holds the connection to
//! zookeeper in its own thread and communicates via channels to callers
//! [[kafka::zookeeper::ZooKeeperClient]]. ( TODO RAFKA version may not be pipelined?)
//! core/src/main/scala/kafka/zk/KafkaZkClient.scala
//!
//! Implementation note: this class includes methods for various components (Controller, Configs,
//! Old Consumer, etc.) and returns instances of classes from the calling packages in some cases.
//! This is not ideal, but it made it easier to migrate away from `ZkUtils` (since removed). We
//! should revisit this. We should also consider whether a monolithic [[kafka.zk.ZkData]] is the way
//! to go.

// RAFKA TODO: The documentation may not be accurate anymore.

use crate::majordomo::{AsyncTask, AsyncTaskError};
use crate::server::kafka_config::KafkaConfig;
use crate::zk::zk_data;
use crate::zk::zk_data::ZNodeHandle;
use crate::zookeeper::zoo_keeper_client::ZKClientConfig;
use crate::zookeeper::zoo_keeper_client::ZooKeeperClient;
use std::error::Error;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::{debug, error, info, trace};
use tracing_attributes::instrument;
use uuid::Uuid;
use zookeeper_async::CreateMode;

#[derive(thiserror::Error, Debug)]
pub enum KafkaZkClientError {
    #[error("InvalidPath")]
    InvalidPath,
    #[error("Create zookeeper path:  already exists")]
    CreatePathExists,
    // RAFA NOTE: Originally, the ClusterId error is a KafkaException, maybe worth create a
    // KafkaServerError enum
    #[error(
        "Failed to get cluster id from Zookeeper. This can happen if /cluster/id is deleted from \
         Zookeeper."
    )]
    ClusterIdDeleted,
    #[error("Failed to parse the cluster id json {0:?}")]
    ClusterIdDeserialize(#[from] serde_json::Error),
    #[error("Failed to generate broker.id due to {0:?}")]
    BrokerIdGenerate(String),
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
    pub zk_data: zk_data::ZkData,
}

impl Default for KafkaZkClient {
    fn default() -> Self {
        KafkaZkClient {
            zoo_keeper_client: ZooKeeperClient::default(),
            time: Instant::now(),
            current_zookeeper_session_id: -1i32,
            zk_data: zk_data::ZkData::default(),
        }
    }
}

impl KafkaZkClient {
    pub fn new(zoo_keeper_client: ZooKeeperClient, time: Instant) -> Self {
        KafkaZkClient {
            zoo_keeper_client,
            time,
            current_zookeeper_session_id: -1i32,
            zk_data: zk_data::ZkData::default(),
        }
    }

    /// The builder receives params to create the ZookeeperClient and builds a local instance.
    /// in java this maps to the apply() of the KafkaZkClient Object
    pub fn build(
        zk_connect: &str,
        kafka_config: &KafkaConfig,
        _name: Option<String>,
        time: std::time::Instant,
        zk_client_config: Option<ZKClientConfig>,
    ) -> Self {
        let zoo_keeper_client = ZooKeeperClient::new(
            zk_connect.to_string(),
            kafka_config.zk_session_timeout_ms,
            kafka_config.zk_connection_timeout_ms,
            kafka_config.zk_max_in_flight_requests,
            // time,
            // name,
            zk_client_config,
        );
        KafkaZkClient::new(zoo_keeper_client, time)
    }

    /// `parent_path` returns the path from the start until the last slash.
    /// If there is no slash, an error is returned about an InvalidPath
    fn parent_path(path: &str) -> Result<String, KafkaZkClientError> {
        // XXX: Change this to return a slice to avoid creating strings for no good reason.
        match path.rfind('/') {
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
    #[instrument]
    async fn retry_request_until_connected(
        _create_request: String,
    ) -> Result<(), KafkaZkClientError> {
        unimplemented!()
    }

    /// `create_looped_0` creates the top-most non-existent path in zookeeper. For example if only
    /// path /a/ exist in zookeeper and a path request for /a/b/c/d is received, it will create
    /// /a/b/.
    #[instrument]
    async fn create_looped_0(&mut self, path: &str) -> Result<(), AsyncTaskError> {
        // NOTE: This function used to be createRecursive0, but has been transfromed into a loop
        let path_sections: Vec<&str> = path.split('/').collect();
        if path_sections.len() < 2 {
            return Err(AsyncTaskError::KafkaZkClient(KafkaZkClientError::InvalidPath));
        }
        let mut curr_path_section = 2usize;
        loop {
            let path = path_sections[0..curr_path_section].join("/");
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
                            zookeeper_async::ZkError::NodeExists => {
                                if path_sections.len() == curr_path_section {
                                    // If all paths exist, then we can stop creating dirs
                                    break;
                                }
                                curr_path_section += 1;
                                continue;
                            },
                            _ => return Err(AsyncTaskError::ZooKeeper(*zk_err)),
                        }
                    }
                }
                return Err(err);
            }
        }
        Ok(())
    }

    /// `close` closes the connecting to zookeeper
    #[instrument]
    pub async fn close(&mut self) -> Result<(), AsyncTaskError> {
        self.zoo_keeper_client.close().await
    }

    /// `create_looped` is a replacement for createRecursive from java code.
    #[instrument]
    pub async fn create_looped(
        &mut self,
        path: &str,
        data: Vec<u8>,
        fail_on_exists: bool,
    ) -> Result<(), AsyncTaskError> {
        let create_request = self
            .zoo_keeper_client
            .create_request(
                path,
                data.clone(),
                self.zk_data.default_acls(path),
                CreateMode::Persistent,
            )
            .await;
        if let Err(err) = create_request {
            if err.is_zookeeper_async_node_exists() {
                if fail_on_exists {
                    return Err(AsyncTaskError::KafkaZkClient(
                        KafkaZkClientError::CreatePathExists,
                    ));
                }
            } else if err.is_zookeeper_async_no_node() {
                match self.create_looped_0(&Self::parent_path(&path)?).await {
                    Err(err) => {
                        return Err(err);
                    },
                    Ok(()) => {
                        // We try to create the path at location `path` in zookeeper
                        // `path may not exist and we may attempt to create the
                        // hierarchy from here. It is possible that another server
                        // started the same operation, thus creating the path for us.
                        // For example, another server may have been successful
                        // creating the ClusterID, in which case we should stop.
                        match self
                            .zoo_keeper_client
                            .create_request(
                                path,
                                data,
                                self.zk_data.default_acls(path),
                                CreateMode::Persistent,
                            )
                            .await
                        {
                            Ok(res) => {
                                trace!("zoo_keeper_client create_request got response: {:?}", res);
                            },
                            Err(err) => {
                                if err.is_zookeeper_async_node_exists() {
                                    if fail_on_exists {
                                        return Err(AsyncTaskError::KafkaZkClient(
                                            KafkaZkClientError::CreatePathExists,
                                        ));
                                    }
                                } else {
                                    return Err(err);
                                }
                            },
                        }
                    },
                }
            } else {
                return Err(err);
            }
        }
        Ok(())
    }

    /// `make_sure_persistent_path_exists` Make sure a persistent path exists in ZK.
    #[instrument]
    pub async fn make_sure_persistent_path_exists(
        &mut self,
        path: &str,
    ) -> Result<(), AsyncTaskError> {
        self.create_looped(path, vec![], false).await
    }

    /// `connect` performs a connection to the underlying zookeeper client
    #[instrument]
    pub async fn connect(&mut self) -> Result<(), AsyncTaskError> {
        self.zoo_keeper_client.connect().await
    }

    /// `create_chroot_path_if_set` is called from the async coordinator before the real connection
    /// done to zookeeper so that the chroot path is initalized as persistent.
    #[instrument]
    pub async fn create_chroot_path_if_set(
        &mut self,
        zk_connect: &str,
        kafka_config: &KafkaConfig,
    ) -> Result<(), AsyncTaskError> {
        match zk_connect.find('/') {
            Some(chroot_index) => {
                let (zk_connect_host_port, zk_chroot) = zk_connect.split_at(chroot_index);
                debug!("Zookeeper host:port list: {}, chroot: {}", zk_connect_host_port, zk_chroot);
                let mut temp_kafka_zk_client = KafkaZkClient::build(
                    &zk_connect_host_port.to_string(),
                    kafka_config,
                    Some(String::from("Chroot Path Handler")),
                    self.time,
                    None,
                );
                temp_kafka_zk_client.connect().await?;
                temp_kafka_zk_client.make_sure_persistent_path_exists(zk_chroot).await?;
                temp_kafka_zk_client.close().await.unwrap();
            },
            None => {
                debug!(
                    "Zookeeper path does not contain chroot, no need to make sure the path exists"
                );
            },
        };
        Ok(())
    }

    /// `create_top_level_paths` ensures the top level paths (on top of the chroot) are created,
    /// this  requires a connected client.
    #[instrument]
    pub async fn create_top_level_paths(&mut self) -> Result<(), AsyncTaskError> {
        let persistent_paths: Vec<String> = self
            .zk_data
            .persistent_zk_paths()
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<String>>();
        for path in persistent_paths {
            self.make_sure_persistent_path_exists(&path).await?;
        }
        Ok(())
    }

    /// `init` creates the chroot paths, creates the persistent paths and connects to the chroot-ed
    /// zookeeper location.
    #[instrument]
    pub async fn init(&mut self, kafka_config: &KafkaConfig) -> Result<(), AsyncTaskError> {
        self.create_chroot_path_if_set(&kafka_config.zk_connect, &kafka_config).await?;
        self.connect().await?;
        self.create_top_level_paths().await
    }

    /// `get_data_and_version` for a given zk path, the version is equivalent to the Zookeeper Stat
    /// if the Stat from get_data is None, then ZkVersion::UnknownVersion (-2) is returned
    #[instrument]
    pub async fn get_data_and_version(
        &self,
        path: &str,
    ) -> Result<GetDataAndVersionResponse, AsyncTaskError> {
        let (data, stat) = self.get_data_and_stat(path).await?;
        match stat {
            None => Ok(GetDataAndVersionResponse {
                data,
                version: zk_data::ZkVersion::UnknownVersion as i32,
            }),
            Some(zk_stat) => Ok(GetDataAndVersionResponse { data, version: zk_stat.version }),
        }
    }

    /// The server side component that creates the cluster Id, if it doesn't exist, one cluster_id
    /// is created and registered in Zookeeper
    #[instrument]
    pub async fn get_or_generate_cluster_id(&mut self) -> Result<String, AsyncTaskError> {
        let cluster_id = self.get_cluster_id().await?;
        match cluster_id {
            Some(val) => Ok(val),
            None => self.create_or_get_cluster_id(base64::encode(Uuid::new_v4().to_string())).await,
        }
    }

    #[instrument]
    pub async fn get_cluster_id(&self) -> Result<Option<String>, AsyncTaskError> {
        let (data, _stat) = self.get_data_and_stat(&self.zk_data.cluster_id.path()).await?;
        match data {
            Some(data) => Ok(Some(String::from_utf8(data)?)),
            None => Ok(None),
        }
    }

    /// Create the cluster Id. If the cluster id already exists, return the current cluster id.
    pub async fn create_or_get_cluster_id(
        &mut self,
        proposed_cluster_id: String,
    ) -> Result<String, AsyncTaskError> {
        let cluster_id_path = self.zk_data.cluster_id.path().to_owned();
        let cluster_id_content = self.zk_data.cluster_id.to_json(&proposed_cluster_id);
        trace!(
            "Creating Cluster ID path: {} if it does not exist, it will be initialized with \
             propoposed cluster ID: {:?}",
            cluster_id_path,
            cluster_id_content
        );
        let create_looped_response = self
            .create_looped(
                &cluster_id_path,
                cluster_id_content,
                true, // Fail on exists, so we can capture the error and set the content
            )
            .await;
        match create_looped_response {
            Ok(()) => {
                trace!("Cluster ID path has been created and initialized with proposed cluster ID");
                Ok(proposed_cluster_id)
            },
            Err(err) => {
                if let Some(err) = err.source() {
                    if let Some(zk_err) = err.downcast_ref::<zookeeper_async::ZkError>() {
                        match zk_err {
                            zookeeper_async::ZkError::NodeExists => {
                                return match self.get_cluster_id().await? {
                                    Some(val) => Ok(val),
                                    None => {
                                        // To get to this point we must- have
                                        // This can happen if /cluster/id is deleted from
                                        // Zookeeper while in the process of reading it.
                                        Err(AsyncTaskError::KafkaZkClient(
                                            KafkaZkClientError::ClusterIdDeleted,
                                        ))
                                    },
                                };
                            },
                            _ => return Err(crate::majordomo::AsyncTaskError::ZooKeeper(*zk_err)),
                        }
                    }
                }
                Err(err)
            },
        }
    }

    /// Gets the data and Stat at the given zk path, both the Data and the Stat may be empty
    #[instrument]
    pub async fn get_data_and_stat(
        &self,
        path: &str,
    ) -> Result<(Option<Vec<u8>>, Option<zookeeper_async::Stat>), AsyncTaskError> {
        let get_data_request = self.zoo_keeper_client.get_data_request(path).await;
        match get_data_request {
            Err(err) => {
                if err.is_zookeeper_async_no_node() {
                    Ok((None, None))
                } else {
                    Err(err)
                }
            },
            Ok((data, stat)) => Ok((Some(data), Some(stat))),
        }
    }

    /// Generate a broker id by updating the broker sequence id path in ZK and return the version of
    /// the path. The version is incremented by one on every update starting from 1.
    /// @return sequence number as the broker id
    #[instrument]
    pub async fn generate_broker_sequence_id(&mut self) -> Result<i32, AsyncTaskError> {
        let broker_sequence_id_path = self.zk_data.broker_sequence_id.path().to_owned();
        trace!("Setting Broker Sequence ID  in path: {}", broker_sequence_id_path);
        match self.zoo_keeper_client.set_data(&broker_sequence_id_path, vec![], None).await {
            Ok(stat) => {
                trace!("Successfully set Broker Sequence ID, stat: {:?}", stat);
                Ok(stat.version)
            },
            Err(err) => {
                if err.is_zookeeper_async_no_node() {
                    trace!("Broker Sequence ID does not exist, creating");
                    self.create_looped(
                        &broker_sequence_id_path,
                        vec![],
                        true, /* Fail on exists, so we can capture the error and
                               * set the content */
                    )
                    .await?;
                    trace!(
                        "Create finished, setting Broker Sequence ID data: {}",
                        broker_sequence_id_path
                    );
                    let stat = self
                        .zoo_keeper_client
                        .set_data(&broker_sequence_id_path, vec![], None)
                        .await?;
                    return Ok(stat.version);
                } else {
                    Err(err)
                }
            },
        }
    }

    /// Wrapper to request a Majordomo Request for GetDataAndVersion on a given path.
    /// This function helps encapsulate logic to prevent leaking too many implementation details to
    /// other functions.
    #[instrument]
    pub async fn req_get_data_and_version(
        majordomo_tx: mpsc::Sender<AsyncTask>,
        res_tx: oneshot::Sender<GetDataAndVersionResponse>,
        path: String,
    ) -> Result<(), AsyncTaskError> {
        trace!("Requesting GetDataAndVersion on ZK node at path: {}", path);
        Ok(majordomo_tx
            .send(AsyncTask::Zookeeper(KafkaZkClientAsyncTask::GetDataAndVersion(res_tx, path)))
            .await?)
    }

    /// Read the entity (topic, broker, client, user or <user, client>) config (if any) from zk
    /// sanitizedEntityName is <topic>, <broker>, <client-id>, <user> or <user>/clients/<client-id>.
    #[instrument]
    pub async fn get_entity_configs(
        majordomo_tx: mpsc::Sender<AsyncTask>,
        root_entity_type: &str,
        sanitized_entity_name: &str,
    ) -> Result<Option<Vec<u8>>, AsyncTaskError> {
        let (tx, rx) = oneshot::channel();
        let config_znode = zk_data::ConfigZNode::build();
        let root_entity_zk_node_path = zk_data::ConfigEntityZNode::build(
            &config_znode,
            root_entity_type,
            sanitized_entity_name,
        )
        .path()
        .to_string();
        KafkaZkClient::req_get_data_and_version(majordomo_tx, tx, root_entity_zk_node_path).await?;
        Ok(rx.await.unwrap().data)
    }
}

/// -----------------------
/// Rafka Async tasks/messages related to this module
/// -----------------------

/// For sending GetDataAndVersionResponse across a channel
#[derive(Debug)]
pub struct GetDataAndVersionResponse {
    pub data: Option<Vec<u8>>,
    pub version: i32,
}

/// Rafka Async tasks/messages related to this module
#[derive(Debug)]
pub enum KafkaZkClientAsyncTask {
    EnsurePersistentPathExists(String),
    GetDataAndVersion(oneshot::Sender<GetDataAndVersionResponse>, String),
    RegisterFeatureChange(mpsc::Sender<AsyncTask>),
    Shutdown,
    GetOrGenerateClusterId(oneshot::Sender<String>),
    GenerateBrokerId(oneshot::Sender<i32>),
}

#[derive(Debug)]
pub struct KafkaZkClientCoordinator {
    kafka_zk_client: KafkaZkClient,
    pub tx: mpsc::Sender<KafkaZkClientAsyncTask>,
    rx: mpsc::Receiver<KafkaZkClientAsyncTask>,
}

impl KafkaZkClientCoordinator {
    /// `new` creates a newm instance of the KafkaZkClientCoordinator.
    /// An mpsc channel is created to communicate with the internal coordinator for tasks.
    pub async fn new(kafka_config: KafkaConfig) -> Result<Self, AsyncTaskError> {
        let (tx, rx) = mpsc::channel(4_096); // TODO: Magic number removal
        let init_time = Instant::now();
        let mut kafka_zk_client = KafkaZkClient::build(
            &kafka_config.zk_connect,
            &kafka_config,
            Some(String::from("Async Coordinator")),
            init_time,
            None,
        );
        kafka_zk_client.init(&kafka_config).await?;
        Ok(KafkaZkClientCoordinator { kafka_zk_client, tx, rx })
    }

    /// `main_tx` clones the current transmission endpoint in the coordinator channel.
    pub fn main_tx(&self) -> mpsc::Sender<KafkaZkClientAsyncTask> {
        self.tx.clone()
    }

    /// `process_message_queue` receives KafkaZkClientAsyncTask requests from clients
    /// If a client wants a response it may use a oneshot::channel for it
    pub async fn process_message_queue(&mut self) -> Result<(), AsyncTaskError> {
        while let Some(task) = self.rx.recv().await {
            info!("KafkaZkClient coordinator {:?}", task);
            match task {
                KafkaZkClientAsyncTask::GetDataAndVersion(tx, znode_path) => {
                    debug!("KafkaZkClientAsyncTask calling get_data_and_version");
                    let result = self.kafka_zk_client.get_data_and_version(&znode_path).await?;
                    debug!("KafkaZkClientAsyncTask got response from get_data_and_version");
                    if let Err(err) = tx.send(result) {
                        // RAFKA TODO: should the process die here?
                        error!("Unable to send back GetDataAndVersionResponse: {:?}", err);
                    }
                },
                KafkaZkClientAsyncTask::GetOrGenerateClusterId(tx) => {
                    let result = self.kafka_zk_client.get_or_generate_cluster_id().await?;
                    if let Err(err) = tx.send(result) {
                        // RAFKA TODO: should the process die here?
                        error!("Unable to send back ClusterId response: {:?}", err);
                    }
                },
                KafkaZkClientAsyncTask::Shutdown => self.kafka_zk_client.close().await.unwrap(),
                KafkaZkClientAsyncTask::RegisterFeatureChange(majordomo_tx) => {
                    self.kafka_zk_client
                        .zoo_keeper_client
                        .register_feature_cache_change(majordomo_tx)
                        .await?
                },
                KafkaZkClientAsyncTask::GenerateBrokerId(tx) => {
                    let result = self.kafka_zk_client.generate_broker_sequence_id().await.unwrap();
                    if let Err(err) = tx.send(result) {
                        // RAFKA TODO: should the process die here?
                        error!("Unable to send back GenerateBrokerId response: {:?}", err);
                    }
                },
                _ => unimplemented!("Task not implemented"),
            }
        }
        Ok(())
    }
}
