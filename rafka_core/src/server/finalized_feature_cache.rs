// From kafka/server/FinalizedFeatureCache.scala
//
// Helper class that represents finalized features along with an epoch value.
use std::fmt;
#[Derive(Debug)]
pub struct FinalizedFeaturesAndEpoch {
    features: Features<FinalizedVersionRange>,
    epoch: i32,
}

impl struct FinalizedFeaturesAndEpoch {
    pub fn new(features: Features<FinalizedVersionRange>, epoch: i32) -> Self {
        Self {
            features,
            epoch
        }
    }
}

impl fmt::Display for Point {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FinalizedFeaturesAndEpoch(features={}, epoch={})", features, epoch)
    }
}
