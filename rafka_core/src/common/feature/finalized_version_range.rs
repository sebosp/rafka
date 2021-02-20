// From clients/src/main/java/org/apache/kafka/common/feature/FinalizedVersionRange.java
// Represents the min/max versions for finalized features.
//
use std::collections::HashMap;
use crate::common::feature::feature::{BaseVersionRange, BaseVersionRangeError};
#[derive(Debug)]
pub struct FinalizedVersionRange {
    version_range: BaseVersionRange,
}

// Label for the min version key, that's used only to convert to/from a map.
const MIN_VERSION_LEVEL_KEY_LABEL: &'static str = "min_version_level";
// Label for the max version key, that's used only to convert to/from a map.
const MAX_VERSION_LEVEL_KEY_LABEL: &'static str= "max_version_level";

impl FinalizedVersionRange {
    pub fn new(min_version_level: i16, max_version_level: i16) -> Self {
        Self {
            version_range: BaseVersionRange::new(
                                MIN_VERSION_LEVEL_KEY_LABEL.to_string(),
                                min_version_level, 
                                MAX_VERSION_LEVEL_KEY_LABEL.to_string(),
                                max_version_level, 
                           )
        }
    }

    /// Tries to create an object from a HashMap, returns error if keys do not exist 
    pub fn try_from_map(version_range_map: HashMap<String, i16>) -> Result<Self, BaseVersionRangeError> {
        Ok(FinalizedVersionRange::new(
            BaseVersionRange::from(MIN_VERSION_LEVEL_KEY_LABEL, version_range_map)?,
            BaseVersionRange::from(MAX_VERSION_LEVEL_KEY_LABEL, version_range_map)?)
        )
    }

    /// Tries to create an object from a HashMap, panics if keys do not exist 
    pub fn from_map(version_range_map: HashMap<String, i16>) -> Result<Self, BaseVersionRangeError> {
        match try_from_map(version_range_map) {
            Ok(val) => val,
            Err(err) => panic!(err),
        }
    }

    pub fn min(&self) -> i16 {
        self.version_range.min()
    }

    pub fn max(&self) -> i16 {
        self.version_range.max()
    }

    /// Checks if the [min, max] version level range of this object does *NOT* fall within the
    /// [min, max] version range of the provided SupportedVersionRange parameter.
    pub is_incompatible_with(&self, supported_version_range: supportedVersionRange) -> bool {
        self.min() < supported_version_range.min() || self.max() > supported_version_range.max();
    }
}

