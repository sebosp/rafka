//! A ZooKeeper client that encourages pipelined requests.
//! core/src/main/scala/kafka/zookeeper/ZooKeeperClient.scala

// RAFKA TODO: Check if we can do the "pipeline" with the rust libraries

use std::time::SystemTime;
// This ZKClientConfig comes from
// https://zookeeper.apache.org/doc/r3.5.4-beta/api/org/apache/zookeeper/client/ZKClientConfig.html
// and seems to provide TLS related config. For now we will just provide an empty struct.
pub struct ZKClientConfig;
pub struct ZooKeeperClient {
    /// `connect_string` comma separated host:port pairs, each corresponding to a zk server
    connect_string: String,
    /// `session_timeout_ms` session timeout in milliseconds
    session_timeout_ms: u32,
    /// `connection_timeout_ms` connection timeout in milliseconds
    connection_timeout_ms: u32,
    /// `max_in_flight_requests` maximum number of unacknowledged requests the client will send
    /// before blocking
    max_in_flight_requests: u32,
    /// name name of the client instance
    name: Option<String>,
    time: SystemTime,
    /// monitoring related fields
    metric_group: String,
    metric_type: String,
    // zkClientConfig ZooKeeper client configuration, for TLS configs if desired
    zk_client_config: Option<ZKClientConfig>,
}

impl ZooKeeperClient {
    pub fn new(
        connect_string: String,
        session_timeout_ms: u32,
        connection_timeout_ms: u32,
        max_in_flight_requests: u32,
        time: SystemTime,
        metric_group: String,
        metric_type: String,
    ) -> Self {
        ZooKeeperClient {
            connect_string,
            session_timeout_ms,
            connection_timeout_ms,
            max_in_flight_requests,
            time,
            metric_group,
            metric_type,
            name: None,
            zk_client_config: None,
        }
    }
}
