//! The ClusterResource metadata for a Kafka cluster.
//! From clients/src/main/java/org/apache/kafka/common/ClusterResource.java

use std::fmt::Display;
#[derive(PartialEq, Debug, Hash, Clone)]
pub struct ClusterResource {
    pub cluster_id: Option<String>,
}
impl ClusterResource {
    /// The cluster.id may be None (null on java), if the metadata request was sent to a cluster
    /// prior to version 0.10.1.0.
    pub fn new(cluster_id: Option<String>) -> Self {
        Self { cluster_id }
    }
}

impl Display for ClusterResource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "klusterResource(clusterId={:?})", self.cluster_id)
    }
}
