//! Core KafkaServer
//! core/src/main/scala/kafka/server/KafkaServer.scala
//! Changes:
//! - All fields that were initially null have been coverted to Option<T>, This is probably a bad
//!   idea, let's see how far we can go

use crate::server::broker_metadata_checkpoint::BrokerMetadataCheckpoint;
use crate::server::broker_states::BrokerState;
use crate::server::dynamic_config_manager::{ConfigEntityName, ConfigType};
use crate::server::kafka_config::KafkaConfig;
use crate::utils::kafka_scheduler::KafkaScheduler;
use crate::zk::kafka_zk_client::KafkaZkClient;
use crate::zookeeper::zoo_keeper_client::ZKClientConfig;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info};
pub struct CountDownLatch(u8);
pub struct Metrics;
pub struct KafkaApis;
pub struct Authorizer;
pub struct SocketServer;
pub struct KafkaRequestHandlerPool;
pub struct LogManager;
pub struct LogDirFailureChannel;
pub struct ReplicaManager;
pub struct AdminManager;
pub struct DelegationTokenManager;
pub struct ConfigHandler;
pub struct DynamicConfigManager;
pub struct GroupCoordinator;
pub struct TransactionCoordinator;
pub struct KafkaController;
pub struct MetadataCache;
struct BrokerTopicStats;
struct FinalizedFeatureChangeListener;
pub struct KafkaServer {
    startup_complete: Arc<AtomicBool>, // false
    is_shutting_down: Arc<AtomicBool>, // false
    is_starting_up: Arc<AtomicBool>,   // false

    shutdown_latch: CountDownLatch, // (1)

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

    _feature_change_listener: Option<FinalizedFeatureChangeListener>, /* was null, changed to
                                                                       * Option<> */
    pub kafka_config: KafkaConfig,
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
        KafkaServer {
            startup_complete: Arc::new(AtomicBool::new(false)),
            is_shutting_down: Arc::new(AtomicBool::new(false)),
            is_starting_up: Arc::new(AtomicBool::new(false)),
            shutdown_latch: CountDownLatch(1),
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
            dynamic_config_manager: DynamicConfigManager, // RAFKA TODO: ::default(),
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
            _feature_change_listener: None,
            init_time: Instant::now(),
            kafka_config: KafkaConfig::default(),
        }
    }
}

impl KafkaServer {
    /// `new` creates a new instance, expects a parsed config
    pub fn new(config: KafkaConfig, time: std::time::Instant) -> Self {
        let mut kafka_server = KafkaServer::default();
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

    /// Main entry point for this class
    pub fn startup(&mut self) {
        info!("Starting");
        // These series of if might be pointless and we might get away by using mpsc from a
        // coordinator thread instead of this memory sharing.
        // NOTE: All these Ordering::Relaxed are not currently checked.
        if self.is_shutting_down.clone().load(Ordering::Relaxed) {
            panic!("Kafka server is still shutting down, cannot re-start!");
        }
        if self.startup_complete.clone().load(Ordering::Relaxed) {
            return;
        }
        let can_startup = self.is_starting_up.compare_and_swap(false, true, Ordering::Relaxed);
        if can_startup {
            self.broker_state = BrokerState::Starting;
            self.init_zk_client();
        }
    }

    pub fn init_zk_client(&mut self) {
        info!("Connecting to zookeeper on {:?}", self.kafka_config.zk_connect);
        match self.kafka_config.zk_connect.find('/') {
            Some(chroot_index) => {
                // make sure chroot path exists
                let (zk_connect_host_port, zk_chroot) =
                    self.kafka_config.zk_connect.split_at(chroot_index);
                debug!("Zookeeper host:port list: {}, chroot: {}", zk_connect_host_port, zk_chroot);
                let zk_client = self.create_zk_client(zk_connect_host_port);
                zk_client.make_sure_persistent_path_exists(zk_chroot);
                info!("Created zookeeper path {}", zk_chroot);
                // zk_client.close(); TODO Check Drop may be implicit
            },
            None => {
                debug!(
                    "Zookeeper path does not contain chroot, no need to make sure the path exists"
                );
            },
        };
        // _zkClient = createZkClient(config.zkConnect, secureAclsEnabled)
        // _zkClient.createTopLevelPaths()
    }

    /// Creates a new zk_client for the zk_connect parameter
    fn create_zk_client(&self, zk_connect: &str) -> KafkaZkClient {
        KafkaZkClient::build(
            zk_connect,
            self.kafka_config.zk_session_timeout_ms,
            self.kafka_config.zk_connection_timeout_ms,
            self.kafka_config.zk_max_in_flight_requests,
            self.init_time,
            Some(String::from("Kafka server")),
            Some(self.zk_client_config),
        )
    }
}
