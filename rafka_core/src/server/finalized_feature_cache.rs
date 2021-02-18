// From kafka/server/FinalizedFeatureCache.scala
//
// Helper class that represents finalized features along with an epoch value.
use std::fmt;
#[derive(Debug)]
pub struct FinalizedFeaturesAndEpoch {
    features: Features<FinalizedVersionRange>,
    epoch: i32,
}

impl FinalizedFeaturesAndEpoch {
    pub fn new(features: Features<FinalizedVersionRange>, epoch: i32) -> Self {
        Self { features, epoch }
    }
}

impl fmt::Display for FinalizedFeaturesAndEpoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FinalizedFeaturesAndEpoch(features={}, epoch={})", self.features, self.epoch)
    }
}
