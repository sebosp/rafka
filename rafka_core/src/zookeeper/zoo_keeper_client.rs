//! A ZooKeeper client that encourages pipelined requests.
//! core/src/main/scala/kafka/zookeeper/ZooKeeperClient.scala

// RAFKA TODO: Check if we can do the "pipeline" with the rust libraries
use crate::utils::kafka_scheduler::KafkaScheduler;
use futures::future::lazy;
use std::collections::HashMap;
/// RAFKA Specific:
/// - While the library uses re-entrant locks and concurrent structures extensively, this crate
///   will rather use mpsc channels to communicate back and forth and have a main loop.
/// - The Kafkascheduler is not used, it creates an executor and schedules tasks, rather, the
///   tokio scheduler will be used.
/// - Need to figure out reconnection to zookeeper
/// (https://docs.rs/tokio-zookeeper/0.1.3/tokio_zookeeper/struct.ZooKeeper.html)
use std::time::SystemTime;
use tokio::prelude::*;
use tokio::sync::mpsc;
use tokio_zookeeper::*;

use slog::{error, info};

// ZKClientConfig comes from
// https://zookeeper.apache.org/doc/r3.5.4-beta/api/org/apache/zookeeper/client/ZKClientConfig.html
// and seems to provide TLS related config. For now we will just provide an empty struct.
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
    time: SystemTime,
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
    zookeeper: Option<tokio_zookeeper::ZooKeeper>,
    logger: slog::Logger,
}

impl ZooKeeperClient {
    pub fn new(
        connect_string: String,
        session_timeout_ms: u32,
        connection_timeout_ms: u32,
        max_in_flight_requests: u32,
        time: SystemTime,
        tx: Option<mpsc::Sender<ZookeeperRequest>>,
        metric_group: String,
        metric_type: String,
    ) -> Self {
        ZooKeeperClient {
            connect_string,
            session_timeout_ms,
            connection_timeout_ms,
            max_in_flight_requests,
            time,
            metric_group,
            metric_type,
            name: None,
            zk_client_config: None,
            // expiry_scheduler_handler: KafkaScheduler { tx, ..KafkaScheduler::default() },
            // zNodeChangeHandlers: HashMap::new(),
            // zNodeChildChangeHandlers: HashMap::new(),
            zookeeper: None,
            logger: crate::utils::default_logger(),
        }
    }

    pub async fn connect(&mut self) -> Result<(), String> {
        let logger = self.logger.clone();
        let handle = tokio::spawn(async {ZooKeeper::connect(&self.connect_string.parse().unwrap())});
        match handle.await {
            Ok(val) => {
                // A "default watcher is returned on the connection
                info!(self.logger, "Connection to zookeeper successful");
                self.zookeeper = Some(val);
                Ok(())
            },
            Err(err) => Err(String::from("Unable to connect to zookeeper")),
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
            time: SystemTime::now(),
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
    fn handleCreation();
    fn handleDeletion();
    fn handleDataChange();
}

pub trait ZNodeChildChangeHandler {
    type Path;
    fn handleChildChange();
}
