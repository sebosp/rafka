// From kafka/server/FinalizedFeatureCache.scala
//
// Helper class that represents finalized features along with an epoch value.
use crate::common::feature::features::Features;
use crate::common::feature::finalized_version_range::FinalizedVersionRange;
use crate::majordomo::AsyncTaskError;
use std::fmt;
use tokio::sync::mpsc;
use tracing::info;
use tracing_attributes::instrument;

/// Represents finalized features along with an epoch value #[derive(Debug)]
#[derive(Debug)]
pub struct FinalizedFeaturesAndEpoch {
    features: Features,
    epoch: i32,
}

impl FinalizedFeaturesAndEpoch {
    pub fn new(features: Features, epoch: i32) -> Self {
        Self { features, epoch }
    }
}

impl fmt::Display for FinalizedFeaturesAndEpoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FinalizedFeaturesAndEpoch(features={}, epoch={})", self.features, self.epoch)
    }
}

/// A common cache containing the latest finalized features and epoch. By default the contents of
/// the cache are empty. This cache needs to be populated at least once for its contents to become
/// non-empty. Currently the main reader of this cache is the read path that serves an
/// ApiVersionsRequest, returning the features information in the response.
#[derive(Debug)]
pub struct FinalizedFeatureCache {
    features_and_epoch: Option<FinalizedFeaturesAndEpoch>,
    /// A clonable tx to send messages to the control loop
    pub tx: mpsc::Sender<FinalizedFeatureCacheAsyncTask>,
    /// A receiver of tasks
    rx: mpsc::Receiver<FinalizedFeatureCacheAsyncTask>,
}

impl FinalizedFeatureCache {
    pub fn new() -> Self {
        // TODO: Magic number removal
        let (tx, rx) = tokio::sync::mpsc::channel(4_096);
        FinalizedFeatureCache { features_and_epoch: None, tx, rx }
    }

    /// Returns the latest known FinalizedFeaturesAndEpoch or empty if not defined in the
    /// cache.
    pub fn get(&self) -> Option<FinalizedFeaturesAndEpoch> {
        // RAFKA TODO is this Copy?
        self.features_and_epoch
    }

    pub fn is_empty(&self) -> bool {
        self.features_and_epoch.is_none()
    }

    /// Clears all existing finalized features and epoch from the cache.
    pub fn clear(&mut self) {
        info!("Cleared cache");
        self.features_and_epoch = None;
    }

    #[instrument]
    pub async fn async_coordinator(&mut self) {
        while let Some(message) = self.rx.recv().await {
            info!("FinalizedFeatureCache::async_coordinator: message: {:?}", message);
            match message {
                FinalizedFeatureCacheAsyncTask::Clear => self.features_and_epoch = None,
            }
        }
    }
}

#[derive(Debug)]
pub enum FinalizedFeatureCacheAsyncTask {
    Clear,
}

impl FinalizedFeatureCacheAsyncTask {
    #[instrument]
    pub async fn process_tasks(
        cache: &mut FinalizedFeatureCache,
        task: Self,
    ) -> Result<(), AsyncTaskError> {
        match task {
            Self::Clear => cache.clear(),
        }
        Ok(())
    }
}
