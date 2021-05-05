// From clients/src/main/java/org/apache/kafka/common/feature/SupportedVersionRange.java
use super::base_version_range::{BaseVersionRange, BaseVersionRangeError};
/// Represents the min/max versions for supported features.
use std::collections::HashMap;
use std::fmt;
use tracing::debug;

#[derive(Debug, Clone, PartialEq)]
pub struct SupportedVersionRange {
    version_range: BaseVersionRange,
}
// Label for the min version key, that's used only to convert to/from a map.
const MIN_VERSION_KEY_LABEL: &str = "min_version";
// Label for the max version key, that's used only to convert to/from a map.
const MAX_VERSION_KEY_LABEL: &str = "max_version";

impl SupportedVersionRange {
    pub fn new(
        min_version_level: i16,
        max_version_level: i16,
    ) -> Result<Self, BaseVersionRangeError> {
        Ok(Self {
            version_range: BaseVersionRange::new(
                MIN_VERSION_KEY_LABEL.to_string(),
                min_version_level,
                MAX_VERSION_KEY_LABEL.to_string(),
                max_version_level,
            )?,
        })
    }

    /// Attempts to create an instance from a HashMap, returns error if keys do not exist
    fn try_from_map(
        version_range_map: &HashMap<String, i16>,
    ) -> Result<Self, BaseVersionRangeError> {
        debug!("Attempting to build SupportedVersionRange from HashMap: {:?}", version_range_map);
        SupportedVersionRange::new(
            BaseVersionRange::from(MIN_VERSION_KEY_LABEL, version_range_map),
            BaseVersionRange::from(MAX_VERSION_KEY_LABEL, version_range_map),
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

impl fmt::Display for SupportedVersionRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SupportedVersionRange[{}:{}, {},{}]",
            MIN_VERSION_KEY_LABEL,
            self.min(),
            MAX_VERSION_KEY_LABEL,
            self.max()
        )
    }
}
