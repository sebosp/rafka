// From kafka/server/FinalizedFeatureCache.scala
//
// Helper class that represents finalized features along with an epoch value.
use crate::common::feature::features::Features;
use crate::majordomo::AsyncTaskError;
use crate::server::finalize_feature_change_listener::FeatureCacheUpdaterError;
use std::fmt;
use tracing::info;
use tracing_attributes::instrument;

/// Represents finalized features along with an epoch value #[derive(Debug)]
#[derive(Debug, Clone)]
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
}

impl Default for FinalizedFeatureCache {
    fn default() -> Self {
        FinalizedFeatureCache { features_and_epoch: None }
    }
}

impl FinalizedFeatureCache {
    /// Returns the latest known FinalizedFeaturesAndEpoch or empty if not defined in the
    /// cache.
    pub fn get(&self) -> Option<FinalizedFeaturesAndEpoch> {
        self.features_and_epoch.clone()
    }

    pub fn is_empty(&self) -> bool {
        self.features_and_epoch.is_none()
    }

    /// Clears all existing finalized features and epoch from the cache.
    pub fn clear(&mut self) {
        info!("Cleared cache");
        self.features_and_epoch = None;
    }

    /// Updates the cache to the latestFeatures, and updates the existing epoch to latestEpoch.
    /// Expects that the latestEpoch should be always greater than the existing epoch (when the
    /// existing epoch is defined).
    pub fn update_or_throw(
        &mut self,
        latest_features: Features,
        latest_epoch: i32,
    ) -> Result<(), AsyncTaskError> {
        let latest = FinalizedFeaturesAndEpoch::new(latest_features, latest_epoch);
        match self.features_and_epoch {
            Some(val) => {
                if val.epoch > latest.epoch {
                    return Err(AsyncTaskError::FeatureCacheUpdater(
                        FeatureCacheUpdaterError::InvalidEpoch(
                            format!("{}", latest),
                            format!("{}", val),
                        ),
                    ));
                }
            },
            None => String::from("<empty>"),
        };
    }
}

/// Majordomo Coordinator handling of async tasks
#[derive(Debug)]
pub enum FinalizedFeatureCacheAsyncTask {
    Clear,
}

impl FinalizedFeatureCacheAsyncTask {
    #[instrument]
    pub async fn process_task(
        cache: &mut FinalizedFeatureCache,
        task: Self,
    ) -> Result<(), AsyncTaskError> {
        match task {
            Self::Clear => cache.clear(),
        }
        Ok(())
    }
}
