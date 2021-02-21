// From clients/src/main/java/org/apache/kafka/common/feature/FinalizedVersionRange.java
// Represents the min/max versions for finalized features.
//
use super::base_version_range::{BaseVersionRange, BaseVersionRangeError};
use super::features::VersionRangeType;
use super::supported_version_range::SupportedVersionRange;
use std::collections::HashMap;
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
    pub fn new(min_version_level: i16, max_version_level: i16) -> Self {
        Self {
            version_range: BaseVersionRange::new(
                MIN_VERSION_LEVEL_KEY_LABEL.to_string(),
                min_version_level,
                MAX_VERSION_LEVEL_KEY_LABEL.to_string(),
                max_version_level,
            ),
        }
    }

    /// Checks if the [min, max] version level range of this object does *NOT* fall within the
    /// [min, max] version range of the provided SupportedVersionRange parameter.
    pub fn is_incompatible_with(&self, supported_version_range: &SupportedVersionRange) -> bool {
        self.min() < supported_version_range.min() || self.max() > supported_version_range.max();
    }
}
impl VersionRangeType for FinalizedVersionRange {
    /// Attempts to create an instance from a HashMap, returns error if keys do not exist
    fn try_from_map(
        version_range_map: HashMap<String, i16>,
    ) -> Result<Self, BaseVersionRangeError> {
        Ok(FinalizedVersionRange::new(
            BaseVersionRange::from(MIN_VERSION_LEVEL_KEY_LABEL, version_range_map)?,
            BaseVersionRange::from(MAX_VERSION_LEVEL_KEY_LABEL, version_range_map)?,
        ))
    }
}
