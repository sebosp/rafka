//! Core KafkaServer
//! core/src/main/scala/kafka/server/KafkaServer.scala
//! Rafka Changes:
//! - All fields that were initially null have been coverted to Option<T>, This is probably a bad
//!   idea, let's see how far we can go
//! - In the original code, KafkaServer has a field of type KafkaConfig that contains a
//!   DynamicBrokerConfig that in turn has a reference to the parent KafkaConfig. In this version,
//!   KafkaServer has a Dynamic Broker Config field that owns the KafkaConfig (inversed)

use crate::common::cluster_resource::ClusterResource;
use crate::log::log_manager::LogManager;
use crate::majordomo::{AsyncTask, AsyncTaskError};
use crate::server::broker_metadata_checkpoint::{BrokerMetadata, BrokerMetadataCheckpoint};
use crate::server::broker_states::BrokerState;
use crate::server::dynamic_broker_config::DynamicBrokerConfig;
use crate::server::dynamic_config_manager::DynamicConfigManager;
use crate::server::finalize_feature_change_listener::FinalizedFeatureChangeListener;
use crate::server::kafka_config::KafkaConfig;
use crate::server::quota_manager::{QuotaFactory, QuotaManagers};
use crate::utils::kafka_scheduler::KafkaScheduler;
use crate::zk::kafka_zk_client::{KafkaZkClient, KafkaZkClientAsyncTask};
use crate::zookeeper::zoo_keeper_client::ZKClientConfig;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicU32;
use std::time::Instant;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info};
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
/// `BrokerTopicStats` contains metrics per topic, bytes in, bytes out, messages in, etc.
#[derive(Debug)]
pub struct BrokerTopicStats;

#[derive(Debug)]
pub enum KafkaServerAsyncTask {
    Shutdown,
}

#[derive(Debug)]
pub struct KafkaServer {
    // startup_complete: Arc<AtomicBool>, // false
    // is_shutting_down: Arc<AtomicBool>, // false
    // is_starting_up: Arc<AtomicBool>,   // false
    //
    // shutdown_latch: CountDownLatch, // (1)
    pub cluster_id: Option<String>,
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

    pub replica_manager: Option<ReplicaManager>, // was null, changed to Option<>
    pub admin_manager: Option<AdminManager>,     // was null, changed to Option<>
    pub token_manager: Option<DelegationTokenManager>, // was null, changed to Option<>

    pub dynamic_config_handlers: HashMap<String, ConfigHandler>,
    pub dynamic_config_manager: DynamicConfigManager,

    pub group_coordinator: Option<GroupCoordinator>, // was null, changed to Option<>

    pub transaction_coordinator: Option<TransactionCoordinator>, // was null, changed to Option<>

    pub kafka_controller: Option<KafkaController>, // was null, changed to Option<>

    pub kafka_scheduler: KafkaScheduler, // was null, changed to Option<>

    pub metadata_cache: Option<MetadataCache>, // was null, changed to Option<>
    pub init_time: Instant,
    pub zk_client_config: ZKClientConfig,
    _zk_client: KafkaZkClient,

    pub correlation_id: AtomicU32, /* = new AtomicInteger(0) TODO: Can this be a U32? Maybe less
                                    * capacity? */
    pub config: KafkaConfig,
    pub broker_meta_props_file: String,
    pub broker_metadata_checkpoints: HashMap<String, BrokerMetadataCheckpoint>,
    _cluster_id: Option<String>, // was null, changed to Option<>
    broker_topic_stats: Option<BrokerTopicStats>, // was null, changed to Option<>

    feature_change_listener: Option<FinalizedFeatureChangeListener>, /* was null, changed to
                                                                      * Option<> */
    dynamic_broker_config: DynamicBrokerConfig,
    /// `async_task_tx` contains a handle to send taskt to the majordomo async_coordinator
    pub async_task_tx: mpsc::Sender<AsyncTask>,

    pub rx: mpsc::Receiver<KafkaServerAsyncTask>,

    pub quota_managers: QuotaManagers,
}

// RAFKA Unimplemented:
// private var logContext: LogContext = null
// var kafkaYammerMetrics: KafkaYammerMetrics = null
// credentialProvider: CredentialProvider = null
// tokenCache: DelegationTokenCache = null

impl Default for KafkaServer {
    fn default() -> Self {
        // TODO: Consider removing this implementation in favor of new() as the channel is basically
        // unusable, maybe this is usable for testing
        let (majordomo_tx, _majordomo_rx) = mpsc::channel(4_096); // TODO: Magic number removal
        let majordomo_tx_cp = majordomo_tx.clone();
        let (_main_tx, main_rx) = mpsc::channel(4_096); // TODO: Magic number removal
        let dynamic_broker_config = DynamicBrokerConfig::default();
        let time = Instant::now();
        KafkaServer {
            // startup_complete: Arc::new(AtomicBool::new(false)),
            // is_shutting_down: Arc::new(AtomicBool::new(false)),
            // is_starting_up: Arc::new(AtomicBool::new(false)),
            // shutdown_latch: CountDownLatch(1),
            cluster_id: None,
            metrics: None,
            broker_state: BrokerState::default(),
            data_plane_request_processor: None,
            control_plane_request_processor: None,
            authorizer: None,
            socket_server: None,
            data_plane_request_handler_pool: None,
            control_plane_request_handler_pool: None,
            log_manager: None,
            replica_manager: None,
            admin_manager: None,
            token_manager: None,
            dynamic_config_handlers: HashMap::new(),
            dynamic_config_manager: DynamicConfigManager::new(majordomo_tx_cp),
            group_coordinator: None,
            transaction_coordinator: None,
            kafka_controller: None,
            kafka_scheduler: KafkaScheduler::default(),
            metadata_cache: None,
            zk_client_config: ZKClientConfig::default(),
            _zk_client: KafkaZkClient::default(),
            correlation_id: AtomicU32::new(0),
            broker_meta_props_file: String::from("meta.properties"),
            broker_metadata_checkpoints: HashMap::new(),
            _cluster_id: None,
            broker_topic_stats: None,
            feature_change_listener: None,
            init_time: Instant::now(),
            dynamic_broker_config: dynamic_broker_config.clone(),
            config: KafkaConfig::default(),
            async_task_tx: majordomo_tx,
            rx: main_rx,
            quota_managers: QuotaFactory::instantiate(&dynamic_broker_config.kafka_config, time),
        }
    }
}

impl KafkaServer {
    /// `new` creates a new instance, expects a parsed config
    pub fn new(
        config: KafkaConfig,
        time: std::time::Instant,
        async_task_tx: mpsc::Sender<AsyncTask>,
        rx: mpsc::Receiver<KafkaServerAsyncTask>,
    ) -> Self {
        let mut kafka_server = KafkaServer { async_task_tx, rx, ..KafkaServer::default() };
        // TODO: In the future we can implement SSL/etc.
        // In the kotlin code, zkClientConfigFromKafkaConfig is used to build an
        // Option<ZkClientConfig>, it returns None if there's no SSL setup, we are not using SSL so
        // we can bypass that and return just the default() value
        kafka_server.zk_client_config = ZKClientConfig::default();
        let broker_meta_props_file = String::from("meta.properties");
        // Using File.separator as "/" since this is going to just work on Linux.
        for bmc_log_dir in &config.log.log_dirs {
            let filename = format!("{}/{}", bmc_log_dir, broker_meta_props_file);
            kafka_server
                .broker_metadata_checkpoints
                .insert(bmc_log_dir.clone(), BrokerMetadataCheckpoint::new(&filename));
        }
        kafka_server.init_time = time;
        kafka_server.dynamic_broker_config = DynamicBrokerConfig::new(config.clone());
        kafka_server.config = config;
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
        // RAFKA NOTE: The majordomo thread now owns the shared state, it contains the
        // FinalizedFeatureChangeListener struct.
        // initOrThrow(config.zkConnectionTimeoutMs)

        // Get or create cluster_id
        let cluster_id = self.get_or_generate_cluster_id().await?;
        info!("Cluster ID = {}", cluster_id);
        // read medatada
        let (broker_metadata_set, broker_metadata_found, initial_offline_dirs) =
            self.read_broker_metadata_and_offline_dirs();
        // load metadata
        let preloaded_broker_metadata_checkpoint =
            self.load_broker_metadata(broker_metadata_set, broker_metadata_found)?;
        debug!(
            "Preloaded Broker Metadata Checkpoint: {:?}, Initial Offline Dirs: {:?}",
            preloaded_broker_metadata_checkpoint, initial_offline_dirs
        );
        // check cluster id
        if let Some(metadata_cluster_id) = &preloaded_broker_metadata_checkpoint.cluster_id {
            if *metadata_cluster_id != cluster_id {
                return Err(AsyncTaskError::KafkaServer(KafkaServerError::InconsistentClusterId(
                    cluster_id,
                    metadata_cluster_id.to_string(),
                )));
            }
        }
        self.cluster_id = Some(cluster_id);
        self.dynamic_broker_config.kafka_config.general.broker_id =
            self.get_or_generate_broker_id(preloaded_broker_metadata_checkpoint).await?;
        debug!("KafkaServer id = {}", self.dynamic_broker_config.kafka_config.general.broker_id);

        // initialize dynamic broker configs from ZooKeeper. Any updates made after this will be
        // applied after DynamicConfigManager starts.
        self.dynamic_broker_config.initialize(self.async_task_tx.clone()).await?;
        self.notify_cluster_listeners().await?;

        self.log_manager = Some(
            LogManager::new(
                self.config.clone(),
                initial_offline_dirs.clone(),
                &self.broker_state,
                self.kafka_scheduler.clone(),
                self.init_time, // should this be re-calculated?
                //&self.broker_topic_stats,
                self.async_task_tx.clone(), /* RAFKA TODO: log_dir_failure_channel needs to be
                                             * acquaired through the async_task_tx */
            )
            .await?,
        );

        Ok(())
    }

    /// `notify_cluster_listeners` seems to be used by metrics to enrich the output with the
    /// current cluster_id as well as Metadata to provide the cluster context (probably)
    async fn notify_cluster_listeners(&self) -> Result<(), AsyncTaskError> {
        self.async_task_tx
            .send(AsyncTask::ClusterResource(ClusterResource::new(self.cluster_id.clone())))
            .await?;
        Ok(())
    }

    /// Reads the BrokerMetadata from the directories.
    /// Returns a tuple containing:
    /// - A HashSet of unique BrokerMetadata loaded from the files
    /// - A String containing the log.dir -> BrokerMetadata to help debug InconsistentBrokerMetadata
    /// and see which log.dir contains which BrokerMetadata
    /// - The log directories whose meta.properties can not be accessed due to IO Errors will be
    ///   returned to the caller
    pub fn read_broker_metadata_and_offline_dirs(
        &self,
    ) -> (HashSet<BrokerMetadata>, String, Vec<String>) {
        let mut broker_metadata_set: HashSet<BrokerMetadata> = HashSet::new();
        let mut broker_metadata_found = String::from("");
        let mut offline_dirs: Vec<String> = vec![];

        for log_dir in &self.dynamic_broker_config.kafka_config.log.log_dirs {
            if let Some(checkpoint_dir) = self.broker_metadata_checkpoints.get(log_dir) {
                match checkpoint_dir.read() {
                    Ok(res) => {
                        broker_metadata_found
                            .push_str(format!("- {} -> {}\n", log_dir, res).as_str());
                        broker_metadata_set.insert(res.clone());
                    },
                    Err(err) => {
                        offline_dirs.push(log_dir.to_string());
                        error!(
                            "Failed to read {} under log directory {}: {:?}",
                            self.broker_meta_props_file, log_dir, err
                        );
                    },
                }
            }
        }
        (broker_metadata_set, broker_metadata_found, offline_dirs)
    }

    /// Loads the BrokerMetadata. If the BrokerMetadata doesn't match in all the log.dirs,
    /// InconsistentBrokerMetadataerror is returned
    /// Returns the BrokerMetadata if consistent, otherwise retuns Error
    pub fn load_broker_metadata(
        &self,
        broker_metadata_set: HashSet<BrokerMetadata>,
        broker_metadata_found: String,
    ) -> Result<BrokerMetadata, KafkaServerError> {
        if broker_metadata_set.len() > 1 {
            Err(KafkaServerError::InconsistentBrokerMetadata(broker_metadata_found))
        } else if let Some(some_entry) = broker_metadata_set.iter().next() {
            // If here there's only one item in the Vec
            Ok(some_entry.clone())
        } else {
            Ok(BrokerMetadata::new(-1, None))
        }
    }

    /// Request the cluster ID from Zookeeper, if the cluster ID does not exist, it would be
    /// created
    #[instrument]
    async fn get_or_generate_cluster_id(&mut self) -> Result<String, AsyncTaskError> {
        let (tx, rx) = oneshot::channel();
        self.async_task_tx
            .send(AsyncTask::Zookeeper(KafkaZkClientAsyncTask::GetOrGenerateClusterId(tx)))
            .await?;
        Ok(rx.await?)
    }

    // pub async fn init_zk_client(&mut self)
    // RAFKA NOTE: This has been moved to crate::majordomo::async_coordinator as first step before
    // starting to process messages

    // Creates a new zk_client for the zk_connect parameter
    // fn create_zk_client(&self, zk_connect: &str)
    // RAFKA NOTE: This has been moved to rafka/src/main.rs where KafkaZkClient is created and its
    // channel endpoints shared with majordomo
    //

    /// `process_message_queue` receives KafkaServerAsyncTask requests from clients
    /// If a client wants a response it may use a oneshot::channel for it
    #[instrument]
    pub async fn process_message_queue(&mut self) -> Result<(), AsyncTaskError> {
        // For now only the shutdown signal exists. so just wait for it and exit
        let shutdown_task = self.rx.recv().await;
        info!("KafkaServer shutdown task received: {:?}", shutdown_task);
        Ok(())
    }

    /// Return a sequence id generated by updating the broker sequence id path in ZK.
    /// Users can provide brokerId in the config. To avoid conflicts between ZK generated
    /// sequence id and configured brokerId, we increment the generated sequence id by
    /// KafkaConfig.MaxReservedBrokerId.
    #[instrument]
    async fn generate_broker_id(&mut self) -> Result<i32, AsyncTaskError> {
        let (tx, rx) = oneshot::channel();
        self.async_task_tx
            .send(AsyncTask::Zookeeper(KafkaZkClientAsyncTask::GenerateBrokerId(tx)))
            .await?;
        Ok(rx.await? + self.dynamic_broker_config.kafka_config.general.reserved_broker_max_id)
    }

    /// Generates new broker_id if enabled or reads from meta.properties based on following
    /// conditions:
    ///
    /// - config has no broker.id provided and broker.id.generation.enabled is true, generates a
    ///   broker.id based on Zookeeper's sequence
    /// - config has broker.id and meta.properties contains broker.id if they don't match return
    ///   Error InconsistentBrokerId
    /// - config has broker.id and there is no meta.properties file, creates new meta.properties and
    ///   stores broker.id
    #[instrument]
    async fn get_or_generate_broker_id(
        &mut self,
        broker_metadata: BrokerMetadata,
    ) -> Result<i32, AsyncTaskError> {
        let broker_id = self.dynamic_broker_config.kafka_config.general.broker_id;

        if broker_id >= 0
            && broker_metadata.broker_id >= 0
            && broker_metadata.broker_id != broker_id
        {
            Err(AsyncTaskError::KafkaServer(KafkaServerError::InconsistentBrokerId(
                broker_id,
                broker_metadata.broker_id,
            )))
        } else if broker_metadata.broker_id < 0
            && broker_id < 0
            && self.dynamic_broker_config.kafka_config.general.broker_id_generation_enable
        {
            // generate a new brokerId from Zookeeper
            self.generate_broker_id().await
        } else if broker_metadata.broker_id >= 0 {
            // pick broker.id from meta.properties
            Ok(broker_metadata.broker_id)
        } else {
            Ok(broker_id)
        }
    }

    /// Sends the shutdown signal to the KafkaServer loop
    pub async fn shutdown(tx: mpsc::Sender<KafkaServerAsyncTask>) {
        tx.send(KafkaServerAsyncTask::Shutdown).await.unwrap();
    }
}

#[derive(Debug, Error)]
pub enum KafkaServerError {
    #[error(
        "BrokerMetadata is not consistent across log.dirs. This could happen if multiple brokers \
         shared a log directory (log.dirs) or partial data was manually copied from another \
         broker. Found: {0:?}"
    )]
    InconsistentBrokerMetadata(String),
    #[error(
        "The Cluster ID {0} doesn't match stored clusterId {1} in meta.properties. The broker is \
         trying to join the wrong cluster. Configured zookeeper.connect may be wrong."
    )]
    InconsistentClusterId(String, String),
    #[error(
        "Configured broker.id {0} doesn't match stored broker.id {1} in meta.properties. If you \
         moved your data, make sure your configured broker.id matches. If you intend to create a \
         new broker, you should remove all data in your data directories (log.dirs)."
    )]
    InconsistentBrokerId(i32, i32),
}

#[cfg(test)]
mod tests {
    use crate::server::kafka_config::general::GeneralConfig;

    use super::*;
    // #[test_env_log::test]
    #[test]
    fn it_loads_brokermetadata() {
        let mut good_broker_metadata_set: HashSet<BrokerMetadata> = HashSet::new();
        good_broker_metadata_set.insert(BrokerMetadata::new(1, None));
        good_broker_metadata_set.insert(BrokerMetadata::new(1, None));
        let broker_metadata_found = String::from("test");
        let kafka_server = KafkaServer::default();
        assert!(kafka_server
            .load_broker_metadata(good_broker_metadata_set, broker_metadata_found.clone())
            .is_ok());
        let empty_broker_metadata_set: HashSet<BrokerMetadata> = HashSet::new();
        assert!(kafka_server
            .load_broker_metadata(empty_broker_metadata_set, broker_metadata_found.clone())
            .is_ok());
        let mut different_cluster_ids_broker_metadata_set: HashSet<BrokerMetadata> = HashSet::new();
        different_cluster_ids_broker_metadata_set
            .insert(BrokerMetadata::new(1, Some(String::from("cluster1"))));
        different_cluster_ids_broker_metadata_set
            .insert(BrokerMetadata::new(1, Some(String::from("cluster2"))));
        assert!(kafka_server
            .load_broker_metadata(
                different_cluster_ids_broker_metadata_set,
                broker_metadata_found.clone()
            )
            .is_err());
        let mut different_cluster_data_broker_metadata_set: HashSet<BrokerMetadata> =
            HashSet::new();
        different_cluster_data_broker_metadata_set
            .insert(BrokerMetadata::new(1, Some(String::from("cluster1"))));
        different_cluster_data_broker_metadata_set.insert(BrokerMetadata::new(1, None));
        assert!(kafka_server
            .load_broker_metadata(
                different_cluster_data_broker_metadata_set,
                broker_metadata_found.clone()
            )
            .is_err());
        let mut different_broker_ids_broker_metadata_set: HashSet<BrokerMetadata> = HashSet::new();
        different_broker_ids_broker_metadata_set
            .insert(BrokerMetadata::new(1, Some(String::from("cluster1"))));
        different_broker_ids_broker_metadata_set
            .insert(BrokerMetadata::new(2, Some(String::from("cluster1"))));
        assert!(kafka_server
            .load_broker_metadata(
                different_broker_ids_broker_metadata_set,
                broker_metadata_found.clone()
            )
            .is_err());
    }
    #[tokio::test]
    async fn it_gets_or_generates_broker_id() {
        // NOTE: Even tho function is asynchronous, it won't hit the path where it needs to
        // actually perform an async call. If it does, this test will never finish...
        let bm_b1 = BrokerMetadata::new(1, None);
        let bm_b2 = BrokerMetadata::new(2, None);
        let bm_b_unset = BrokerMetadata::new(-1, None);
        let general = GeneralConfig { broker_id: 1, ..GeneralConfig::default() };
        let kafka_config = KafkaConfig { general, ..KafkaConfig::default() };
        let mut ks_b1 = KafkaServer {
            dynamic_broker_config: DynamicBrokerConfig::new(kafka_config),
            ..KafkaServer::default()
        };
        let same_broker_id = ks_b1.get_or_generate_broker_id(bm_b1.clone()).await;
        assert!(same_broker_id.is_ok());
        let same_broker_id = same_broker_id.unwrap();
        assert_eq!(same_broker_id, 1);
        let diff_broker_id = ks_b1.get_or_generate_broker_id(bm_b2).await;
        assert!(diff_broker_id.is_err());
        let with_bm_b_unset = ks_b1.get_or_generate_broker_id(bm_b_unset.clone()).await;
        assert!(with_bm_b_unset.is_ok());
        let with_bm_b_unset = with_bm_b_unset.unwrap();
        assert_eq!(with_bm_b_unset, 1);
        let mut ks_b_unset = KafkaServer::default();
        let broker_1 = ks_b_unset.get_or_generate_broker_id(bm_b1).await.unwrap();
        assert_eq!(broker_1, 1);
        // NOTE: We cannot test with both KafaServer.config.broker_id being -1 and
        // BrokerMetadata.broker_id being -1 because that actually requires calling zookeeper
    }
}
