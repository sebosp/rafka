//! Core KafkaServer
//! core/src/main/scala/kafka/server/KafkaServer.scala
//! Changes:
//! - All fields that were initially null have been coverted to Option<T>, This is probably a bad
//!   idea, let's see how far we can go

use crate::majordomo::{AsyncTask, AsyncTaskError, CoordinatorTask, ZookeeperAsyncTask};
use crate::server::broker_metadata_checkpoint::BrokerMetadataCheckpoint;
use crate::server::broker_states::BrokerState;
use crate::server::dynamic_config_manager::DynamicConfigManager;
use crate::server::dynamic_config_manager::{ConfigEntityName, ConfigType};
use crate::server::finalize_feature_change_listener::FinalizedFeatureChangeListener;
use crate::server::kafka_config::KafkaConfig;
use crate::utils::kafka_scheduler::KafkaScheduler;
use crate::zk::kafka_zk_client::KafkaZkClient;
use crate::zookeeper::zoo_keeper_client::ZKClientConfig;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info};
use tracing_attributes::instrument;
#[derive(Debug)]
pub struct CountDownLatch(u8);
#[derive(Debug)]
pub struct Metrics;
#[derive(Debug)]
pub struct KafkaApis;
#[derive(Debug)]
pub struct Authorizer;
#[derive(Debug)]
pub struct SocketServer;
#[derive(Debug)]
pub struct KafkaRequestHandlerPool;
#[derive(Debug)]
pub struct LogManager;
#[derive(Debug)]
pub struct LogDirFailureChannel;
#[derive(Debug)]
pub struct ReplicaManager;
#[derive(Debug)]
pub struct AdminManager;
#[derive(Debug)]
pub struct DelegationTokenManager;
#[derive(Debug)]
pub struct ConfigHandler;
#[derive(Debug)]
pub struct GroupCoordinator;
#[derive(Debug)]
pub struct TransactionCoordinator;
#[derive(Debug)]
pub struct KafkaController;
#[derive(Debug)]
pub struct MetadataCache;
#[derive(Debug)]
struct BrokerTopicStats;

#[derive(Debug)]
pub struct KafkaServer {
    // startup_complete: Arc<AtomicBool>, // false
    // is_shutting_down: Arc<AtomicBool>, // false
    // is_starting_up: Arc<AtomicBool>,   // false
    //
    // shutdown_latch: CountDownLatch, // (1)
    pub metrics: Option<Metrics>, // was null, changed to Option<>
    // TODO: BrokerState is volatile, nede to make sure we can make its changes sync through the
    // threads
    pub broker_state: BrokerState,

    pub data_plane_request_processor: Option<KafkaApis>, // was null, changed to Option<>
    pub control_plane_request_processor: Option<KafkaApis>, // was null, changed to Option<>

    pub authorizer: Option<Authorizer>, // was null, changed to Option<>
    pub socket_server: Option<SocketServer>, // was null, changed to Option<>
    pub data_plane_request_handler_pool: Option<KafkaRequestHandlerPool>, /* was null, changed to
                                         * Option<> */
    pub control_plane_request_handler_pool: Option<KafkaRequestHandlerPool>, /* was null, changed to
                                                                              * Option<> */
    pub log_manager: Option<LogManager>, // was null, changed to Option<>
    pub log_dir_failure_channel: Option<LogDirFailureChannel>, // was null, changed to Option<>

    pub replica_manager: Option<ReplicaManager>, // was null, changed to Option<>
    pub admin_manager: Option<AdminManager>,     // was null, changed to Option<>
    pub token_manager: Option<DelegationTokenManager>, // was null, changed to Option<>

    pub dynamic_config_handlers: HashMap<String, ConfigHandler>,
    pub dynamic_config_manager: DynamicConfigManager,

    pub group_coordinator: Option<GroupCoordinator>, // was null, changed to Option<>

    pub transaction_coordinator: Option<TransactionCoordinator>, // was null, changed to Option<>

    pub kafka_controller: Option<KafkaController>, // was null, changed to Option<>

    pub kafka_scheduler: Option<KafkaScheduler>, // was null, changed to Option<>

    pub metadata_cache: Option<MetadataCache>, // was null, changed to Option<>
    pub init_time: Instant,
    pub zk_client_config: ZKClientConfig, /* = KafkaServer.
                                           * zkClientConfigFromKafkaConfig(config).
                                           * getOrElse(new ZKClientConfig()) */
    _zk_client: KafkaZkClient,

    pub correlation_id: AtomicU32, /* = new AtomicInteger(0) TODO: Can this be a U32? Maybe less
                                    * capacity? */
    pub broker_meta_props_file: String, // = "meta.properties"
    pub broker_metadata_checkpoints: HashMap<String, BrokerMetadataCheckpoint>,
    // = config.logDirs.map(logDir => (logDir, new BrokerMetadataCheckpoint(new File(logDir +
    // File.separator + brokerMetaPropsFile)))).toMap
    _cluster_id: Option<String>, // was null, changed to Option<>
    _broker_topic_stats: Option<BrokerTopicStats>, // was null, changed to Option<>$

    feature_change_listener: Option<FinalizedFeatureChangeListener>, /* was null, changed to
                                                                      * Option<> */
    pub kafka_config: KafkaConfig,

    /// `async_task_tx` contains a handle to send taskt to the majordomo async_coordinator
    pub async_task_tx: mpsc::Sender<AsyncTask>,

    pub shutdown_rx: oneshot::Receiver<()>,
}

// RAFKA Unimplemented:
// private var logContext: LogContext = null
// var kafkaYammerMetrics: KafkaYammerMetrics = null
// credentialProvider: CredentialProvider = null
// tokenCache: DelegationTokenCache = null
// quotaManagers: Option<QuotaFactory.QuotaManagers>, // was null, changed to Option<>, TODO: figure
// out what to do with this

impl Default for KafkaServer {
    fn default() -> Self {
        // TODO: Consider removing this implementation in favor of new() as the channel is basically
        // unusable
        let (tx, _rx) = mpsc::channel(4_096); // TODO: Magic number removal
        let (_shutdown_tx, shutdown_rx) = oneshot::channel(); // TODO: Magic number removal
        KafkaServer {
            // startup_complete: Arc::new(AtomicBool::new(false)),
            // is_shutting_down: Arc::new(AtomicBool::new(false)),
            // is_starting_up: Arc::new(AtomicBool::new(false)),
            // shutdown_latch: CountDownLatch(1),
            metrics: None,
            broker_state: BrokerState::default(),
            data_plane_request_processor: None,
            control_plane_request_processor: None,
            authorizer: None,
            socket_server: None,
            data_plane_request_handler_pool: None,
            control_plane_request_handler_pool: None,
            log_manager: None,
            log_dir_failure_channel: None,
            replica_manager: None,
            admin_manager: None,
            token_manager: None,
            dynamic_config_handlers: HashMap::new(),
            dynamic_config_manager: DynamicConfigManager::default(),
            group_coordinator: None,
            transaction_coordinator: None,
            kafka_controller: None,
            kafka_scheduler: None,
            metadata_cache: None,
            zk_client_config: ZKClientConfig::default(),
            _zk_client: KafkaZkClient::default(),
            correlation_id: AtomicU32::new(0),
            broker_meta_props_file: String::from("meta.properties"),
            broker_metadata_checkpoints: HashMap::new(),
            _cluster_id: None,
            _broker_topic_stats: None,
            feature_change_listener: None,
            init_time: Instant::now(),
            kafka_config: KafkaConfig::default(),
            async_task_tx: tx,
            shutdown_rx,
        }
    }
}

impl KafkaServer {
    /// `new` creates a new instance, expects a parsed config
    pub fn new(
        config: KafkaConfig,
        time: std::time::Instant,
        async_task_tx: mpsc::Sender<AsyncTask>,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> Self {
        let mut kafka_server = KafkaServer { async_task_tx, shutdown_rx, ..KafkaServer::default() };
        // TODO: In the future we can implement SSL/etc.
        // In the kotlin code, zkClientConfigFromKafkaConfig is used to build an
        // Option<ZkClientConfig>, it returns None if there's no SSL setup, we are not using SSL so
        // we can bypass that and return just the default() value
        kafka_server.zk_client_config = ZKClientConfig::default();
        let broker_meta_props_file = String::from("meta.properties");
        // Using File.separator as "." since this is going to just work on Linux.
        for bmc_log_dir in &config.log_dirs {
            let filename = format!("{}.{}", bmc_log_dir, broker_meta_props_file);
            kafka_server
                .broker_metadata_checkpoints
                .insert(filename.clone(), BrokerMetadataCheckpoint::new(&filename));
        }
        kafka_server.init_time = time;
        kafka_server.kafka_config = config;
        kafka_server
    }

    /// `startup` initializes local and zookeeper resources
    #[instrument]
    pub async fn startup(&mut self) -> Result<(), AsyncTaskError> {
        info!("Starting");
        // These series of if might be pointless and we might get away by using mpsc from a
        // coordinator thread instead of this memory sharing.
        // NOTE: All these Ordering::Relaxed are not currently checked.
        // if self.is_shutting_down.clone().load(Ordering::Relaxed) {
        // panic!("Kafka server is still shutting down, cannot re-start!");
        // }
        // if self.startup_complete.clone().load(Ordering::Relaxed) {
        // return;
        // }
        // let can_startup = self.is_starting_up.compare_and_swap(false, true, Ordering::Relaxed);
        // if can_startup {

        // setup zookeeper
        self.broker_state = BrokerState::Starting;
        self.async_task_tx.send(AsyncTask::Zookeeper(ZookeeperAsyncTask::Init)).await?;
        // TODO: Either move this to the async-coordinator or use the tx field to create the
        // listeners for FinalizedFeatureChangeListener
        // self.featureChangeListener = Some(FinalizedFeatureChangeListener::new(zkClient));
        // if (config.isFeatureVersioningEnabled) {
        //    _featureChangeListener.initOrThrow(config.zkConnectionTimeoutMs)
        //}

        //}
        Ok(())
    }

    #[instrument]
    pub async fn init_zk_client(&mut self) -> Result<(), AsyncTaskError> {
        // NOTE: This has been moved to crate::majordomo::async_coordinator as first step before
        // starting to process messages
        info!("Sending Connect to zookeeper on {:?}", self.kafka_config.zk_connect);
        Ok(())
    }

    /// Creates a new zk_client for the zk_connect parameter
    fn create_zk_client(&self, zk_connect: &str) -> KafkaZkClient {
        KafkaZkClient::build(
            zk_connect,
            &self.kafka_config,
            Some(String::from("Kafka server")),
            self.init_time,
            Some(self.zk_client_config),
        )
    }

    /// `wait_for_shutdown` waits for Ctrl-C to shutdown and clean  resources
    /// This function is used for testing the shutdown channels when Ctrl-C, not part of kafka.
    #[instrument]
    pub async fn wait_for_shutdown(&mut self) {
        self.shutdown_rx.recv().await.unwrap();
        self.async_task_tx.send(AsyncTask::Coordinator(CoordinatorTask::Shutdown)).await.unwrap();
        info!("Received shutdown signal");
    }
}
