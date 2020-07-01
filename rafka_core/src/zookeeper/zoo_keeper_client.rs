//! A ZooKeeper client that encourages pipelined requests.
//! core/src/main/scala/kafka/zookeeper/ZooKeeperClient.scala

// RAFKA TODO: Check if we can do the "pipeline" with the rust libraries
use std::collections::HashMap;
/// RAFKA Specific:
/// While the library uses re-entrant locks and concurrent structures extensively, this crate
/// will rather use mpsc channels to communicate back and forth and have a main loop.
use std::time::SystemTime;

// ZKClientConfig comes from
// https://zookeeper.apache.org/doc/r3.5.4-beta/api/org/apache/zookeeper/client/ZKClientConfig.html
// and seems to provide TLS related config. For now we will just provide an empty struct.
pub struct ZKClientConfig;
pub struct ZooKeeperClient<N, C>
where
    N: ZNodeChangeHandler,
    C: ZNodeChildChangeHandler,
{
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
    // RAFKA unimplemented:
    // private val initializationLock = new ReentrantReadWriteLock()
    // private val isConnectedOrExpiredLock = new ReentrantLock()
    // private val isConnectedOrExpiredCondition = isConnectedOrExpiredLock.newCondition()
    // inFlightRequests = new Semaphore(maxInFlightRequests) private val stateChangeHandlers
    // = new ConcurrentHashMap[String, StateChangeHandler]().asScala private[zookeeper] val
    // expiryScheduler = new KafkaScheduler(threads = 1, "zk-session-expiry-handler")
    zNodeChangeHandlers: HashMap<String, N>,
    zNodeChildChangeHandlers: HashMap<String, C>,
}

impl<N, C> ZooKeeperClient<N, C> {
    pub fn new(
        connect_string: String,
        session_timeout_ms: u32,
        connection_timeout_ms: u32,
        max_in_flight_requests: u32,
        time: SystemTime,
        metric_group: String,
        metric_type: String,
    ) -> Self
    where
        N: ZNodeChangeHandler,
        C: ZNodeChildChangeHandler,
    {
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
            zNodeChangeHandlers: HashMap::new(),
            zNodeChildChangeHandlers: HashMap::new(),
        }
    }
}

pub trait ZNodeChangeHandler {
    type Path;
    fn handleCreation();
    fn handleDeletion();
    fn handleDataChange();
}

pub trait ZNodeChildChangeHandler {
    type Path;
    fn handleChildChange();
}
