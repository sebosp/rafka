// From kafka/server/FinalizedFeatureCache.scala
//
use crate::common::feature::features::Features;
use crate::majordomo::AsyncTaskError;
use crate::server::finalize_feature_change_listener::FeatureCacheUpdaterError;
use crate::server::supported_features::SupportedFeatures;
use std::fmt;
use tracing::info;

/// Represents finalized features along with an epoch value
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
        supported_features: &mut SupportedFeatures,
        latest_features: Features,
        latest_epoch: i32,
    ) -> Result<(), AsyncTaskError> {
        // FeatureCacheUpdateException if the cache update operation fails
        // due to invalid parameters or incompatibilities with the broker's
        // supported features. In such a case, the existing cache contents
        // are not modified.
        let latest = FinalizedFeaturesAndEpoch::new(latest_features, latest_epoch);
        let old_feature_and_epoch =
            self.features_and_epoch.as_ref().map_or(String::from("<empty>"), |val| val.to_string());
        if let Some(val) = &self.features_and_epoch {
            if val.epoch > latest.epoch {
                return Err(AsyncTaskError::FeatureCacheUpdater(
                    FeatureCacheUpdaterError::InvalidEpoch(latest.to_string(), val.to_string()),
                ));
            }
        }
        let incompatible_features = supported_features.incompatible_features(&latest.features);
        if incompatible_features.is_empty() {
            info!(
                "Updated cache from existing finalized {} to latest finalized {}",
                old_feature_and_epoch, latest
            );
            self.features_and_epoch = Some(latest);
            Ok(())
        } else {
            Err(AsyncTaskError::FeatureCacheUpdater(FeatureCacheUpdaterError::Incompatible(
                supported_features.get().to_string(),
                latest.to_string(),
            )))
        }
    }
}
