// From clients/src/main/java/org/apache/kafka/common/feature/Features.java

use super::base_version_range::{BaseVersionRange, BaseVersionRangeError};
use super::finalized_version_range::FinalizedVersionRange;
use super::supported_version_range::SupportedVersionRange;
use std::collections::HashMap;
use std::fmt;
use thiserror::Error;
use tracing::debug;

pub trait VersionRangeType {
    /// Attempts to create an instance from a HashMap, panics if the min/max labels do not exist as
    /// keys in the HashMap
    fn from_map(version_range_map: HashMap<String, i16>) -> Self {
        debug!("Attempting to build from HashMap: {:?}", version_range_map);
        match Self::try_from_map(version_range_map) {
            Ok(val) => val,
            Err(err) => panic!(err),
        }
    }

    /// Attempts to create an instance from a HashMap, returns error if keys do not exist
    fn try_from_map(version_range_map: HashMap<String, i16>)
        -> Result<Self, BaseVersionRangeError>;

    fn min(&self) -> i16 {
        self.version_range.min()
    }

    fn max(&self) -> i16 {
        self.version_range.max()
    }
}

/// Represents an immutable dictionary with key being feature name, and value being
/// something that implements VersionRangeType
#[derive(Debug)]
pub struct Features<T>
where
    T: VersionRangeType,
{
    features: HashMap<String, T>,
}

#[derive(Error)]
pub enum FeaturesError {
    #[error("Provided features can not be empty")]
    EmptyVersionRangeType,
}

impl Features {
    /// Attempts to create a new instance of Features
    fn new<T>(features: HashMap<String, T>) -> Result<Self, FeaturesError>
    where
        T: VersionRangeType,
    {
        if features.is_empty() {
            Err(FeaturesError::EmptyVersionRangeType)
        } else {
            Self { features }
        }
    }

    /// Creates a Features struct of type SupportedVersionRange
    pub fn supported_features(
        features: HashMap<String, SupportedVersionRange>,
    ) -> Self<SupportedVersionRange> {
        Self::new(features);
    }

    /// Creates a Features struct of type FinalizedVersionRange
    pub fn finalized_features(
        features: HashMap<String, FinalizedVersionRange>,
    ) -> Self<FinalizedVersionRange> {
        Self::new(features);
    }

    pub fn is_empty(&self) {
        self.features.isEmpty();
    }

    /// Returns the Some(VersionRangeType) corresponding to the feature name, or None if the feature
    /// is absent
    pub fn get<T>(&self, feature: &str) -> Option<T>
    where
        T: VersionRangeType,
    {
        self.get(feature)
    }
}

impl fmt::Display for Features {
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
