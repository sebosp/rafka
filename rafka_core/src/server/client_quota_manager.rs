/// From core/src/main/scala/kafka/server/ClientQuotaManager.scala
use crate::server::quota_manager::QuotaType;
use std::time::Instant;

/// Configuration settings for quota management
#[derive(Debug)]
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

/// Provides several ways to throttle the users, and the clients, the configs for them are stored
/// in zookeeper with this precedence:
/// - /config/users/<user>/clients/<client-id>
/// - /config/users/<user>/clients/<default>
/// - /config/users/<user>
/// - /config/users/<default>/clients/<client-id>
/// - /config/users/<default>/clients/<default>
/// - /config/users/<default>
/// - /config/clients/<client-id>
/// - /config/clients/<default>
#[derive(Debug)]
pub struct ClientQuotaManager {
    pub config: ClientQuotaManagerConfig,
    pub quota_type: QuotaType,
    pub time: Instant,
}

impl ClientQuotaManager {
    pub fn new(config: ClientQuotaManagerConfig, quota_type: QuotaType, time: Instant) -> Self {
        // A thread is spawned for controlling quotas, handling delays and throttling.
        // Not sure this would at all be added.
        Self { config, quota_type, time }
    }
}
