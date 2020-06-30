//! A ZooKeeper client that encourages pipelined requests.
//! core/src/main/scala/kafka/zookeeper/ZooKeeperClient.scala

// RAFKA TODO: Check if we can do the "pipeline" with the rust libraries

use crate::zookeeper::ZKClientConfig;
use std::time::SystemTime;
pub struct ZooKeeperClient {
    /// connectString comma separated host:port pairs, each corresponding to a zk server
    connect_string: String,
    /// sessionTimeoutMs session timeout in milliseconds
    sessionTimeoutMs: u32,
    /// connectionTimeoutMs connection timeout in milliseconds
    connectionTimeoutMs: u32,
    /// maxInFlightRequests maximum number of unacknowledged requests the client will send before
    /// blocking
    maxInFlightRequests: u32,
    /// name name of the client instance
    name: Option<String>,
    time: SystemTime,
    /// monitoring related fields
    metricGroup: String,
    /// zkClientConfig ZooKeeper client configuration, for TLS configs if desired
    metricType: String,
    /// zkClientConfig ZooKeeper client configuration, for TLS configs if desired
    zk_client_config: Option<ZKClientConfig>,
}
