// From clients/src/main/java/org/apache/kafka/common/feature/Features.java

use super::base_version_range::{BaseVersionRange, BaseVersionRangeError};
use std::collections::HashMap;
use tracing::debug;

// Represents an immutable dictionary with key being feature name, and value being
// BaseVersionRange.

#[derive(Debug)]
pub struct Features<T>
where
    T: VersionRangeType,
{
    pub features: HashMap<String, T>,
}

impl Features {}

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
