/// From core/src/main/scala/kafka/server/ClientQuotaManager.scala

/// Configuration settings for quota management
pub struct ClientQuotaManagerConfig {
    /// The default bytes per second quota allocated to any client-id if
    /// dynamic defaults or user quotas are not set
    pub quota_bytes_per_second_default: i64,
    // pub num_quota_samples: i32, These are values used for metrics and throttling
    /// The time span of each sample, used f or throttling
    pub quota_window_size_seconds: i32,
}

// impl Default for ClientQuotaManagerConfig {
// fn default() -> Self {
// Self {
// quota_bytes_per_second_default: i64::MAX,
// quota_window_size_seconds: 1,
// }
// }
// }

impl ClientQuotaManagerConfig {
    pub fn new(quota_bytes_per_second_default: i64, quota_window_size_seconds: i32) -> Self {
        Self { quota_bytes_per_second_default, quota_window_size_seconds }
    }
}

pub struct ClientQuotaManager;
