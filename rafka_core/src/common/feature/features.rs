// From clients/src/main/java/org/apache/kafka/common/feature/Features.java

use super::base_version_range::{BaseVersionRange, BaseVersionRangeError};
use super::finalized_version_range::FinalizedVersionRange;
use super::supported_version_range::SupportedVersionRange;
use std::collections::HashMap;
use std::fmt;
use thiserror::Error;
use tracing::debug;

#[derive(Debug, Clone, PartialEq)]
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

    /// Attempts to return the a VersionRangeType from a string containing json
    pub fn try_from_json_string_as_finalized(input: &str) -> Result<Self, BaseVersionRangeError> {
        debug!("Attempting to build VersionRangeType from String: {}", input);
        match serde_json::from_str(input)? {
            serde_json::Value::Object(data) => {
                let mut res: HashMap<String, FinalizedVersionRange> = HashMap::new();
                for (feature_name, feature_map) in &data {
                    debug!("Processing feature {}", feature_name);
                    res.insert(
                        feature_name.to_string(),
                        FinalizedVersionRange::try_from_json(feature_map)?,
                    );
                }
                Ok(Self::Finalized(res))
            },
            _ => Err(BaseVersionRangeError::IncorrectJsonFormat),
        }
    }
}

/// Represents an immutable dictionary with key being feature name, and value being
/// either Supported or Finalized VersionRanges
#[derive(Debug, Clone, PartialEq)]
pub struct Features {
    pub features: VersionRangeType,
}

#[derive(Debug, Error)]
pub enum FeaturesError {
    #[error("Provided features can not be empty")]
    EmptyVersionRangeType,
    #[error("Features map can not be absent in: {0}")]
    FeaturesMapEmpty(String),
    #[error("Features map is invalid, .features value is malformed.")]
    FeaturesMapInvalid(String),
    #[error("BaseVersionRange {0:?}")]
    BaseVersionRange(#[from] BaseVersionRangeError),
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

    /// Attemps to parse the "features" value, which contains an internal JSON that should map into
    /// the features vector
    pub fn parse_finalized_features_json_value(
        input: &serde_json::Value,
        features_key: &str,
    ) -> Result<Self, FeaturesError> {
        match input[features_key].as_str() {
            Some(val) => {
                debug!("Decoding features value from json");
                Ok(Features {
                    features: VersionRangeType::try_from_json_string_as_finalized(&val)?,
                })
            },
            None => Err(FeaturesError::FeaturesMapEmpty(input.to_string())),
        }
    }
}

impl fmt::Display for Features {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Features{}", &self.features)
    }
}
