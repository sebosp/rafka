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
    startupComplete: AtomicBool, // false
    isShuttingDown: AtomicBool,  // false
    isStartingUp: AtomicBool,    // false

    shutdownLatch: CountDownLatch, // (1)

    // properties for MetricsContext
    metricsPrefix: String,    // "kafka.server"
    KAFKA_CLUSTER_ID: String, // "kafka.cluster.id"
    KAFKA_BROKER_ID: String,  // "kafka.broker.id"

    pub metrics: Option<Metrics>, // was null, changed to Option<>
    pub brokerState: BrokerState,

    pub dataPlaneRequestProcessor: Option<KafkaApis>, // was null, changed to Option<>
    pub controlPlaneRequestProcessor: Option<KafkaApis>, // was null, changed to Option<>

    pub authorizer: Option<Authorizer>, // was null, changed to Option<>
    pub socketServer: Option<SocketServer>, // was null, changed to Option<>
    pub dataPlaneRequestHandlerPool: Option<KafkaRequestHandlerPool>, /* was null, changed to
                                         * Option<> */
    pub controlPlaneRequestHandlerPool: Option<KafkaRequestHandlerPool>, /* was null, changed to
                                                                          * Option<> */

    pub replicaManager: Option<ReplicaManager>, // was null, changed to Option<>
    pub adminManager: Option<AdminManager>,     // was null, changed to Option<>
    pub tokenManager: Option<DelegationTokenManager>, // was null, changed to Option<>

    pub dynamicConfigHandlers: HashMap<String, ConfigHandler>,
    pub dynamicConfigManager: DynamicConfigManager,

    pub groupCoordinator: Option<GroupCoordinator>, // was null, changed to Option<>

    pub transactionCoordinator: Option<TransactionCoordinator>, // was null, changed to Option<>

    pub kafkaController: Option<KafkaController>, // was null, changed to Option<>

    pub kafkaScheduler: Option<KafkaScheduler>, // was null, changed to Option<>

    pub metadataCache: Option<MetadataCache>, // was null, changed to Option<>
    pub zkClientConfig: ZKClientConfig,       /* = KafkaServer.
                                               * zkClientConfigFromKafkaConfig(config).
                                               * getOrElse(new ZKClientConfig()) */
    _zkClient: KafkaZkClient,

    pub correlationId: AtomicU32, /* = new AtomicInteger(0) TODO: Can this be a U32? Maybe less
                                   * capacity? */
    pub brokerMetaPropsFile: String, // = "meta.properties"
    pub brokerMetadataCheckpoints: HashMap<String, BrokerMetadataCheckpoint>,
    // = config.logDirs.map(logDir => (logDir, new BrokerMetadataCheckpoint(new File(logDir +
    // File.separator + brokerMetaPropsFile)))).toMap
    _clusterId: Option<String>, // was null, changed to Option<>
    _brokerTopicStats: Option<BrokerTopicStats>, // was null, changed to Option<>$

    _featureChangeListener: Option<FinalizedFeatureChangeListener>, /* was null, changed to
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
