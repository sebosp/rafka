//! Core KafkaServer
//! from core/src/main/scala/kafka/server/KafkaServer.scala
//! Changes:
//! - All fields that were initially null have been coverted to Option<T>, This is probably a bad
//!   idea, let's see how far we can go

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32};
struct CountDownLatch(u8);
struct Metrics;
struct BrokerState;
struct KafkaApis;
struct Authorizer;
struct SocketServer;
struct KafkaRequestHandlerPool;
struct ReplicaManager;
struct AdminManager;
struct DelegationTokenManager;
struct ConfigHandler;
struct DynamicConfigManager;
struct GroupCoordinator;
struct TransactionCoordinator;
struct KafkaController;
struct KafkaScheduler;
struct MetadataCache;
struct ZKClientConfig;
struct KafkaZkClient;
struct BrokerMetadataCheckpoint;
struct BrokerTopicStats;
struct FinalizedFeatureChangeListener;
struct KafkaServer {
    startup_complete: AtomicBool, // false
    is_shutting_down: AtomicBool, // false
    is_starting_up: AtomicBool,   // false

    shutdown_latch: CountDownLatch, // (1)

    // properties for MetricsContext
    metrics_prefix: String,   // "kafka.server"
    kafka_cluster_id: String, // "kafka.cluster.id"
    kafka_broker_id: String,  // "kafka.broker.id"

    pub metrics: Option<Metrics>, // was null, changed to Option<>
    pub broker_state: BrokerState,

    pub data_plane_request_processor: Option<KafkaApis>, // was null, changed to Option<>
    pub control_plane_request_processor: Option<KafkaApis>, // was null, changed to Option<>

    pub authorizer: Option<Authorizer>, // was null, changed to Option<>
    pub socket_server: Option<SocketServer>, // was null, changed to Option<>
    pub data_plane_request_handler_pool: Option<KafkaRequestHandlerPool>, /* was null, changed to
                                         * Option<> */
    pub control_plane_request_handler_pool: Option<KafkaRequestHandlerPool>, /* was null, changed to
                                                                              * Option<> */

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
    pub zk_client_config: ZKClientConfig,      /* = KafkaServer.
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
}
// Unimplemented:
// private var logContext: LogContext = null
// var kafkaYammerMetrics: KafkaYammerMetrics = null
// logManager: LogManager = null
// logDirFailureChannel: LogDirFailureChannel = null,
// credentialProvider: CredentialProvider = null
// tokenCache: DelegationTokenCache = null
// quotaManagers: Option<QuotaFactory.QuotaManagers>, // was null, changed to Option<>, TODO: figure
// out what to do with this.
