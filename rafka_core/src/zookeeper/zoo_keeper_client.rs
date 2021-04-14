//! A ZooKeeper client that encourages pipelined requests.
//! core/src/main/scala/kafka/zookeeper/ZooKeeperClient.scala
//! RAFKA Specific:
//! - While the library uses re-entrant locks and concurrent structures extensively, this crate will
//!   rather use mpsc channels to communicate back and forth and have a main loop.
//! - The Kafkascheduler is not used, it creates an executor and schedules tasks, rather, the tokio
//!   scheduler will be used.
//! - Need to figure out reconnection to zookeeper
//! (https://docs.rs/tokio-zookeeper/0.1.3/tokio_zookeeper/struct.ZooKeeper.html)

// RAFKA TODO: Check if we can do the "pipeline" with the rust libraries

use crate::majordomo::{AsyncTask, AsyncTaskError};
use crate::server::finalize_feature_change_listener::FeatureCacheUpdaterAsyncTask;
use crate::zk::kafka_zk_client::KafkaZkClientAsyncTask;
use crate::zk::zk_data::FeatureZNode;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::{debug, info};
use tracing_attributes::instrument;
use zookeeper_async::recipes::cache::PathChildrenCache;
use zookeeper_async::{Acl, CreateMode, Stat, WatchedEvent, Watcher, ZooKeeper};
// TODO: Backtrace
// use std::backtrace::Backtrace;

// ZKClientConfig comes from
// https://zookeeper.apache.org/doc/r3.5.4-beta/api/org/apache/zookeeper/client/ZKClientConfig.html
// and seems to provide TLS related config. For now we will just provide an empty struct.
#[derive(Debug, Clone, Copy)]
pub enum ZKClientConfig {
    /// For now only PlainText communication is implemented.
    PlainText,
}

impl Default for ZKClientConfig {
    fn default() -> Self {
        ZKClientConfig::PlainText
    }
}

/// A placeholder for the possible requests to zookeeper
pub enum ZookeeperRequest {
    Unimplemented,
}

struct LoggingWatcher;
impl Watcher for LoggingWatcher {
    fn handle(&self, e: WatchedEvent) {
        println!("{:?}", e)
    }
}

#[derive(Error, Debug)]
pub enum ZooKeeperClientError {
    #[error("IO error {0}")]
    Io(#[from] std::io::Error),
    #[error("Tokio error {0}")]
    Tokio(#[from] tokio::task::JoinError),
    #[error("zookeeper-async error {0}")]
    ZookeeperAsync(#[from] zookeeper_async::ZkError),
    #[error("Zookeeper client not Initialized")]
    NotInitialized,
}

pub struct ZooKeeperClient {
    /// `connect_string` comma separated host:port pairs, each corresponding to a zk server, may
    /// contain a /chroot path
    connect_string: String,
    /// `session_timeout_ms` session timeout in milliseconds
    session_timeout_ms: u32,
    /// `connection_timeout_ms` connection timeout in milliseconds
    connection_timeout_ms: u32,
    /// `max_in_flight_requests` maximum number of unacknowledged requests the client will send
    /// before blocking
    max_in_flight_requests: u32,
    /// name of the client instance
    name: Option<String>,
    time: Instant,
    // zk_client_config ZooKeeper client configuration, for TLS configs if desired
    zk_client_config: Option<ZKClientConfig>,
    // RAFKA unimplemented:
    // private val initializationLock = new ReentrantReadWriteLock()
    // private val isConnectedOrExpiredLock = new ReentrantLock()
    // private val isConnectedOrExpiredCondition = isConnectedOrExpiredLock.newCondition()
    // inFlightRequests = new Semaphore(maxInFlightRequests) private val stateChangeHandlers
    // = new ConcurrentHashMap[String, StateChangeHandler]().asScala private[zookeeper] val
    // RAFKA TODO:
    // expiry_scheduler_handler: KafkaScheduler<T>,
    // zNodeChangeHandlers: HashMap<String, N>,
    // zNodeChildChangeHandlers: HashMap<String, C>,
    /// A connection to ZooKeeper.
    zookeeper: Option<Arc<ZooKeeper>>,
}

impl fmt::Debug for ZooKeeperClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ZooKeeperClient")
            .field("connect_string", &self.connect_string)
            .field("session_timeout_ms", &self.session_timeout_ms)
            .field("connection_timeout_ms", &self.connection_timeout_ms)
            .field("max_in_flight_requests", &self.max_in_flight_requests)
            .field("zk_client_config", &self.zk_client_config)
            .finish()
    }
}

impl ZooKeeperClient {
    pub fn new(
        connect_string: String,
        session_timeout_ms: u32,
        connection_timeout_ms: u32,
        max_in_flight_requests: u32,
        time: Instant,
        name: Option<String>,
        zk_client_config: Option<ZKClientConfig>,
    ) -> Self {
        ZooKeeperClient {
            connect_string,
            session_timeout_ms,
            connection_timeout_ms,
            max_in_flight_requests,
            time,
            name,
            zk_client_config,
            // expiry_scheduler_handler: KafkaScheduler { tx, ..KafkaScheduler::default() },
            // zNodeChangeHandlers: HashMap::new(),
            // zNodeChildChangeHandlers: HashMap::new(),
            zookeeper: None,
        }
    }

    /// `connect` peforms a connection to the zookeeper server.
    #[instrument]
    pub async fn connect(&mut self) -> Result<(), AsyncTaskError> {
        // RAFKA TODO: If zookeeper is not up, then it will continue forever trying to connect
        let zk = ZooKeeper::connect(
            &self.connect_string,
            Duration::from_millis(self.connection_timeout_ms.into()),
            LoggingWatcher,
        )
        .await?;
        info!("Connection to zookeeper successful");
        self.zookeeper = Some(Arc::new(zk));
        Ok(())
    }

    /// `get_data_request` Peforms a get data operation to zookeeper.
    /// For now not enabling "watching" the node
    #[instrument]
    pub async fn get_data_request(&self, path: &str) -> Result<(Vec<u8>, Stat), AsyncTaskError> {
        if let Some(zk) = &self.zookeeper {
            Ok(zk.get_data(path, false).await?)
        } else {
            Err(AsyncTaskError::ZooKeeperClient(ZooKeeperClientError::NotInitialized))
        }
    }

    /// `create_request` Creates an operation request for zookeeper, we do not seem to get a
    /// create_response, and we seem to be returned a string?
    #[instrument]
    pub async fn create_request(
        &self,
        path: &str,
        data: Vec<u8>,
        acls: Vec<Acl>,
        mode: zookeeper_async::CreateMode,
    ) -> Result<String, AsyncTaskError> {
        if let Some(zk) = &self.zookeeper {
            Ok(zk.create(path, data, acls, mode).await?)
        } else {
            Err(AsyncTaskError::ZooKeeperClient(ZooKeeperClientError::NotInitialized))
        }
    }

    /// `close` closes the zookeeper connect if initialized, otherwise returns uninitialized error.
    /// This error could be disregarded... Maybe
    #[instrument]
    pub async fn close(&mut self) -> Result<(), AsyncTaskError> {
        if let Some(zk) = &self.zookeeper {
            match zk.close().await {
                Err(err) => Err(AsyncTaskError::ZooKeeper(err)),
                Ok(()) => Ok(()),
            }
        } else {
            Err(AsyncTaskError::ZooKeeperClient(ZooKeeperClientError::NotInitialized))
        }
    }

    /// `register_change_handler` Registers a state or znode watcher.
    /// The state change is for connection/disconnection/auth failures to zookeeper in general
    /// The ZNode change is for a specific path being created/deleted/updated.
    /// # Arguments
    /// * `tx` a channel to the coordinator to send watch events to
    /// RAFKA TODO: For now only using feature cache znode, to be made generic later, maybe
    /// FnOnce(WatchedEvent)
    #[instrument]
    pub async fn register_feature_cache_change(
        &mut self,
        tx: mpsc::Sender<AsyncTask>,
    ) -> Result<(), AsyncTaskError> {
        debug!(
            "register_feature_cache_change event SEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEB"
        );
        if let Some(zk) = &self.zookeeper {
            // A listener to the Zookeeper State change
            let tx_0 = tx.clone();
            zk.add_listener(move |_s| {
                let tx_clone = tx_0.clone();
                tokio::spawn(async move {
                    tx_clone
                        .send(AsyncTask::FinalizedFeatureCache(
                            FeatureCacheUpdaterAsyncTask::TriggerChange,
                        ))
                        .await
                        .unwrap();
                });
            });
            let mut pcc =
                PathChildrenCache::new(zk.clone(), &FeatureZNode::default_path()).await.unwrap();
            pcc.start()?;
            let tx_1 = tx.clone();
            pcc.add_listener(move |e| {
                let tx_clone = tx_1.clone();
                tokio::spawn(async move {
                    tx_clone
                        .send(AsyncTask::Zookeeper(KafkaZkClientAsyncTask::PathChildrenCacheEvent(
                            e,
                        )))
                        .await
                        .unwrap();
                });
            });
            Ok(())
        } else {
            Err(AsyncTaskError::ZooKeeperClient(ZooKeeperClientError::NotInitialized))
        }
    }
}

/// Defaults come from: core/src/main/scala/kafka/server/KafkaConfig.scala
impl Default for ZooKeeperClient {
    // ZkSessionTimeoutMs = 18000
    // ZkSyncTimeMs = 2000
    // ZkEnableSecureAcls = false
    // ZkMaxInFlightRequests = 10
    // ZkSslClientEnable = false
    // ZkSslProtocol = "TLSv1.2"
    // ZkSslEndpointIdentificationAlgorithm = "HTTPS"
    // ZkSslCrlEnable = false
    // ZkSslOcspEnable = false
    fn default() -> Self {
        let session_timeout_ms = 1800;
        ZooKeeperClient {
            connect_string: String::from("127.0.0.1:2181"),
            session_timeout_ms,
            connection_timeout_ms: session_timeout_ms,
            max_in_flight_requests: 10,
            time: Instant::now(),
            name: None,
            zk_client_config: None,
            zookeeper: None,
        }
    }
}

pub trait ZNodeChangeHandler {
    fn handle_creation(&mut self);
    fn handle_deletion(&mut self);
    fn handle_data_change(&mut self);
}

pub trait ZNodeChildChangeHandler {
    fn handle_child_change();
}

pub trait StateChangeHandler {
    fn before_initializing_session();
    fn after_initializing_session();
    // fn on_auth_failure();
}
