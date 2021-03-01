// From clients/src/main/java/org/apache/kafka/common/feature/FinalizedVersionRange.java
// Represents the min/max versions for finalized features.
//
use super::base_version_range::{BaseVersionRange, BaseVersionRangeError};
use super::supported_version_range::SupportedVersionRange;
use std::collections::HashMap;
use std::fmt;
use tracing::debug;
#[derive(Debug)]
pub struct FinalizedVersionRange {
    version_range: BaseVersionRange,
}

// Label for the min version key, that's used only to convert to/from a map.
const MIN_VERSION_LEVEL_KEY_LABEL: &str = "min_version_level";
// Label for the max version key, that's used only to convert to/from a map.
const MAX_VERSION_LEVEL_KEY_LABEL: &str = "max_version_level";

impl FinalizedVersionRange {
    /// Attempts to create an instance from a HashMap, panics if the min/max labels do not exist as
    /// keys in the HashMap (Initially was
    fn from_map(version_range_map: &HashMap<String, i16>) -> Self {
        debug!("Attempting to build from HashMap: {:?}", version_range_map);
        match Self::try_from_map(&version_range_map) {
            Ok(val) => val,
            Err(err) => panic!(err),
        }
    }

    pub fn new(
        min_version_level: i16,
        max_version_level: i16,
    ) -> Result<Self, BaseVersionRangeError> {
        Ok(Self {
            version_range: BaseVersionRange::new(
                MIN_VERSION_LEVEL_KEY_LABEL.to_string(),
                min_version_level,
                MAX_VERSION_LEVEL_KEY_LABEL.to_string(),
                max_version_level,
            )?,
        })
    }

    /// Checks if the [min, max] version level range of this object does *NOT* fall within the
    /// [min, max] version range of the provided SupportedVersionRange parameter.
    pub fn is_incompatible_with(&self, supported_version_range: &SupportedVersionRange) -> bool {
        self.min() < supported_version_range.min() || self.max() > supported_version_range.max()
    }

    /// Attempts to create an instance from a HashMap, returns error if keys do not exist
    fn try_from_map(
        version_range_map: &HashMap<String, i16>,
    ) -> Result<Self, BaseVersionRangeError> {
        debug!("Attempting to build FinalizedVersionRange from HashMap: {:?}", version_range_map);
        FinalizedVersionRange::new(
            // Consider using try_get_value() with `?`
            BaseVersionRange::from(MIN_VERSION_LEVEL_KEY_LABEL, version_range_map),
            BaseVersionRange::from(MAX_VERSION_LEVEL_KEY_LABEL, version_range_map),
        )
    }

    /// Provides access to the minimum value from the version range
    pub fn min(&self) -> i16 {
        self.version_range.min()
    }

    /// Provides access to the maximum value from the version range
    pub fn max(&self) -> i16 {
        self.version_range.max()
    }
}

impl fmt::Display for FinalizedVersionRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "FinalizedVersionRange[{}:{}, {},{}]",
            MIN_VERSION_LEVEL_KEY_LABEL,
            self.min(),
            MAX_VERSION_LEVEL_KEY_LABEL,
            self.max()
        )
    }
}
