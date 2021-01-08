//! A ZooKeeper client that encourages pipelined requests.
//! core/src/main/scala/kafka/zookeeper/ZooKeeperClient.scala

// RAFKA TODO: Check if we can do the "pipeline" with the rust libraries
use crate::utils::kafka_scheduler::KafkaScheduler;
use futures::future::lazy;
use zookeeper_async::{Acl, CreateMode, WatchedEvent, Watcher, ZooKeeper};
use crate::server::kafka_config::KafkaConfig;
use std::collections::HashMap;
/// RAFKA Specific:
/// - While the library uses re-entrant locks and concurrent structures extensively, this crate
///   will rather use mpsc channels to communicate back and forth and have a main loop.
/// - The Kafkascheduler is not used, it creates an executor and schedules tasks, rather, the
///   tokio scheduler will be used.
/// - Need to figure out reconnection to zookeeper
/// (https://docs.rs/tokio-zookeeper/0.1.3/tokio_zookeeper/struct.ZooKeeper.html)
use std::time::{Instant, Duration};
use tokio::sync::mpsc;
use thiserror::Error;
// TODO: Backtrace
// use std::backtrace::Backtrace;

use slog::{error, info};

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
    Io (#[from] std::io::Error),
    #[error("Tokio error {0}")]
    Tokio(#[from] tokio::task::JoinError),
    #[error("zookeeper-async error {0}")]
    ZookeeperAsync(#[from] zookeeper_async::ZkError),
}

pub struct ZooKeeperClient {
    /// `connect_string` comma separated host:port pairs, each corresponding to a zk server
    connect_string: String,
    /// `session_timeout_ms` session timeout in milliseconds
    session_timeout_ms: u32,
    /// `connection_timeout_ms` connection timeout in milliseconds
    connection_timeout_ms: u32,
    /// `max_in_flight_requests` maximum number of unacknowledged requests the client will send
    /// before blocking
    max_in_flight_requests: u32,
    /// name name of the client instance
    name: Option<String>,
    time: Instant,
    /// monitoring related fields
    metric_group: String,
    metric_type: String,
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
    zookeeper: Option<ZooKeeper>,
    logger: slog::Logger,
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
        tx: Option<mpsc::Sender<ZookeeperRequest>>,
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
            logger: crate::utils::default_logger(),
            ..ZooKeeperClient::default()
        }
    }

    pub async fn connect(&mut self) -> Result<(), ZooKeeperClientError> {
        let handle = tokio::spawn(async move {
            ZooKeeper::connect(&self.connect_string, Duration::from_millis(self.connection_timeout_ms.into()), LoggingWatcher).await
        });
        match handle.await? {
            Ok(zk) => {
                // RAFKA TODO: A "default watcher is returned on the connection, figure out what to
                // do with it
                info!(self.logger, "Connection to zookeeper successful");
                self.zookeeper = Some(zk);
                Ok(())
            },
            Err(err) => {
                error!(self.logger, "Unable to connect to zookeeper: {:?}", err);
                Err(format!("Unable to connect to zookeeper: {:?}", err))
            },
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
            metric_group: String::from(""),
            metric_type: String::from(""),
            name: None,
            zk_client_config: None,
            logger: crate::utils::default_logger(),
            zookeeper: None,
        }
    }
}

pub trait ZNodeChangeHandler {
    type Path;
    fn handle_creation();
    fn handle_deletion();
    fn handle_data_change();
}

pub trait ZNodeChildChangeHandler {
    type Path;
    fn handle_child_change();
}
