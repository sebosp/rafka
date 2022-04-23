/// From core/src/main/scala/kafka/server/QuotaFactory.scala
///
/// RAFKA specific:
/// - There's no clientQuotaCallback for now, which seems to be a "plugin" class for managing
///   client quotas: client.quota.callback.class
use crate::server::client_quota_manager::{ClientQuotaManager, ClientQuotaManagerConfig};
use crate::server::kafka_config::KafkaConfig;
use std::time::Instant;
#[derive(Debug)]
pub struct QuotaFactory;
#[derive(Debug)]
pub enum QuotaType {
    Fetch,
    Produce,
    Request,
    LeaderReplication,
    FollowerReplication,
    AlterLogDirsReplication,
}

#[derive(Debug)]
pub struct QuotaManagers {
    pub fetch: ClientQuotaManager,
    pub produce: ClientQuotaManager,
    /* pub request: ClientRequestQuotaManager,
     * pub leader: ReplicationQuotaManager,
     * pub follower: ReplicationQuotaManager,
     * pub alter_log_dirs: ReplicationQuotaManager, */
}
impl QuotaManagers {
    pub fn new(cfg: &KafkaConfig, time: Instant) -> Self {
        Self {
            fetch: ClientQuotaManager::new(
                QuotaFactory::client_fetch_config(cfg),
                QuotaType::Fetch,
                time,
            ),
            produce: ClientQuotaManager::new(
                QuotaFactory::client_produce_config(cfg),
                QuotaType::Produce,
                time,
            ),
            /* request: ClientRequestQuotaManager::new(clientRequestConfig(cfg), time),
             * leader: ReplicationQuotaManager::new(replicationConfig(cfg), LeaderReplication,
             * time), follower: ReplicationQuotaManager::new(
             * replicationConfig(cfg),
             * FollowerReplication,
             * time,
             * ),
             * alter_log_dirs: ReplicationQuotaManager::new(
             * alterLogDirsReplicationConfig(cfg),
             * AlterLogDirsReplication,
             * time,
             * ), */
        }
    }
}
impl QuotaFactory {
    pub fn instantiate(cfg: &KafkaConfig, time: Instant) -> QuotaManagers {
        QuotaManagers::new(cfg, time)
    }

    pub fn client_fetch_config(cfg: &KafkaConfig) -> ClientQuotaManagerConfig {
        ClientQuotaManagerConfig::new(
            cfg.quota.consumer_quota_bytes_per_second_default,
            cfg.quota.quota_window_size_seconds,
        )
    }

    pub fn client_produce_config(cfg: &KafkaConfig) -> ClientQuotaManagerConfig {
        ClientQuotaManagerConfig::new(
            cfg.quota.consumer_quota_bytes_per_second_default,
            cfg.quota.quota_window_size_seconds,
        )
    }
}
