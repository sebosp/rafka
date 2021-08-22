/// From clients/src/main/java/org/apache/kafka/common/internals/ClusterResourceListeners.java
// Rafka differences:
// In the original code, an interface `onUpdate` is used to get notified of cluster
// differences. On this version, an mpsc::Sender<> vector would be used. Maybe a Trait could be
// used?
use crate::common::cluster_resource::ClusterResource;
use crate::majordomo::AsyncTaskError;
use tokio::sync::mpsc;

pub struct ClusterResourceListeners {
    cluster_resource_listeners: Vec<mpsc::Sender<()>>,
}

impl ClusterResourceListeners {
    pub fn new() -> Self {
        Self { cluster_resource_listeners: vec![] }
    }

    /// Originally called `maybeAdd`, in this version no check for Trait impl is needed.
    pub fn add(&mut self, tx: mpsc::Sender<()>) {
        self.cluster_resource_listeners.push(tx);
    }

    /// Originally called `maybeAddAll`, in this version no check for Trait impl is needed.
    pub fn add_all(&mut self, txs: Vec<mpsc::Sender<()>>) {
        for tx in txs {
            self.add(tx);
        }
    }

    /// Notifies all tx about the updated cluster metadata
    pub async fn on_update(&self, cluster: &ClusterResource) -> Result<(), AsyncTaskError> {
        for tx in self.cluster_resource_listeners {
            // TODO: This should be tokio::spawn'ed maybe, as there are multiple receivers and they
            // may be busy.
            tx.send(cluster).await?;
        }
        Ok(())
    }
}
