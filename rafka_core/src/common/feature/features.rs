// From clients/src/main/java/org/apache/kafka/common/feature/Features.java

use super::base_version_range::{BaseVersionRange, BaseVersionRangeError};
use super::finalized_version_range::FinalizedVersionRange;
use super::supported_version_range::SupportedVersionRange;
use std::collections::HashMap;
use std::fmt;
use thiserror::Error;
use tracing::debug;

#[derive(Debug)]
pub enum VersionRangeType {
    Supported(HashMap<String, SupportedVersionRange>),
    Finalized(HashMap<String, FinalizedVersionRange>),
}

/// Represents an immutable dictionary with key being feature name, and value being
/// either Supported or Finalized VersionRanges
#[derive(Debug)]
pub struct Features {
    features: VersionRangeType,
}

#[derive(Debug, Error)]
pub enum FeaturesError {
    #[error("Provided features can not be empty")]
    EmptyVersionRangeType,
}

impl Features {
    /// Creates a Features struct of type SupportedVersionRange
    pub fn supported_features(
        features: HashMap<String, SupportedVersionRange>,
    ) -> Result<Self, FeaturesError> {
        Self::new(VersionRangeType::Supported(features))
    }

    /// Creates a Features struct of type FinalizedVersionRange
    pub fn finalized_features(features: HashMap<String, FinalizedVersionRange>) -> Self {
        Self::new(VersionRangeType::Finalized(features))
    }

    pub fn is_empty(&self) {
        self.features.0.is_empty();
    }

    /// Looks up an item from the features HashMap and returns an Optional found VersionRangeType
    pub fn get(&self, feature: &str) -> Option<VersionRangeType> {
        self.get(feature)
    }
}

impl<T> fmt::Display for Features<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Features{}",
            &self
                .features
                .map(|(key, val)| format!("({} -> {})", key, val))
                .collect::<String>()
                .join(", ")
        )
    }
}
