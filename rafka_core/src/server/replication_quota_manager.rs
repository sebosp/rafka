/// From core/src/main/scala/kafka/server/ReplicationQuotaManager.scala

pub struct ReplicationQuotaManagerConfig {
    pub quota_bytes_per_second_default: u64,
}

impl Default for ReplicationQuotaManagerConfig {
    fn default() -> Self {
        Self { quota_bytes_per_second_default: u64::max_value() }
    }
}
