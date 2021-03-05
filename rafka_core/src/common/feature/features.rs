// From clients/src/main/java/org/apache/kafka/common/feature/Features.java

use super::base_version_range::{BaseVersionRange, BaseVersionRangeError};
use super::finalized_version_range::FinalizedVersionRange;
use super::supported_version_range::SupportedVersionRange;
use std::collections::HashMap;
use std::fmt;
use thiserror::Error;
use tracing::debug;

#[derive(Debug, Clone)]
pub enum VersionRangeType {
    Supported(HashMap<String, SupportedVersionRange>),
    Finalized(HashMap<String, FinalizedVersionRange>),
}

impl fmt::Display for VersionRangeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let collection_str: String = match &self {
            // RAFKA TODO: Consider printing which type of Version Range is being used.
            Self::Supported(supported) => supported
                .iter()
                .map(|(key, val)| format!("({} -> {})", key, val))
                .collect::<Vec<String>>()
                .join(", "),
            Self::Finalized(finalized) => finalized
                .iter()
                .map(|(key, val)| format!("({} -> {})", key, val))
                .collect::<Vec<String>>()
                .join(", "),
        };
        write!(f, "{}", collection_str)
    }
}

impl VersionRangeType {
    /// Returns the internal HashMap is empty status
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Supported(supported) => supported.is_empty(),
            Self::Finalized(finalized) => finalized.is_empty(),
        }
    }
}

/// Represents an immutable dictionary with key being feature name, and value being
/// either Supported or Finalized VersionRanges
#[derive(Debug, Clone)]
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
    pub fn supported_features(features: HashMap<String, SupportedVersionRange>) -> Self {
        Self { features: VersionRangeType::Supported(features) }
    }

    /// Creates a Features struct of type FinalizedVersionRange
    pub fn finalized_features(features: HashMap<String, FinalizedVersionRange>) -> Self {
        Self { features: VersionRangeType::Finalized(features) }
    }

    pub fn is_empty(&self) {
        self.features.is_empty();
    }

    /// Looks up an item from the features HashMap and returns an Optional found VersionRangeType
    pub fn get(&self, feature: &str) -> Option<VersionRangeType> {
        self.get(feature)
    }
}

impl fmt::Display for Features {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Features{}", &self.features)
    }
}
