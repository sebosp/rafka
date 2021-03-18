//! A common immutable object used in the Broker to define the latest features supported by the
//! Broker. Also provides API to check for incompatibilities between the latest features
//! supported by the Broker and cluster-wide finalized features.
//!
//! NOTE: the update() and clear() APIs of this class should be used only for testing purposes.
use crate::common::feature::features::Features;
use crate::common::feature::features::VersionRangeType;
use crate::common::feature::finalized_version_range::FinalizedVersionRange;
use std::collections::HashMap;
use tracing::warn;

#[derive(Debug)]
pub struct SupportedFeatures {
    /// This is the latest features supported by the Broker.
    /// This is currently empty, but in the future as we define supported features, this map should
    /// be populated.
    supported_features: Features,
}

impl Default for SupportedFeatures {
    fn default() -> Self {
        Self { supported_features: Features::empty_supported_features() }
    }
}

impl SupportedFeatures {
    /// Returns the set of feature names found to be 'incompatible'.
    /// A feature incompatibility is a version mismatch between the latest feature supported by the
    /// Broker, and the provided finalized feature. This can happen because a provided finalized
    /// feature:
    ///  1) Does not exist in the Broker (i.e. it is unknown to the Broker).
    ///           [OR]
    ///  2) Exists but the FinalizedVersionRange does not match with the supported feature's
    /// SupportedVersionRange.
    ///
    /// @param finalized   The finalized features against which incompatibilities need to be checked
    /// for.
    ///
    /// @return The subset of input features which are incompatible. If the returned
    /// object is empty, it means there were no feature incompatibilities
    /// found.
    pub fn incompatible_features(&self, finalized: &Features) -> Features {
        let incompatibilities = Features::empty_finalized_features();
        let mut res: HashMap<String, FinalizedVersionRange> = HashMap::new();
        if let VersionRangeType::Finalized(finalized) = finalized.features {
            let incompatible_human_readable = String::from("");
            for (feature, version_levels) in finalized.iter() {
                match self.supported_features.get_supported(feature) {
                    None => {
                        res.insert(feature.to_string(), version_levels.clone());
                        incompatible_human_readable.push_str(format!(
                            "{{feature={}, reason='Unsupported feature'}}",
                            feature
                        ));
                    },
                    Some(supported_versions) => {
                        if version_levels.is_incompatible_with(supported_versions) {
                            res.insert(feature.to_string(), version_levels.clone());
                            incompatible_human_readable.push_str(format!(
                                "{{feature={}, reason='{} is incompatible with {}'}}",
                                feature, version_levels, supported_versions
                            ));
                        }
                    },
                }
            }
            if incompatible_human_readable.len() > 0 {
                warn!("Feature incompatibilities seen: {}", incompatible_human_readable);
            }
        }
        Features::finalized_features(res)
    }

    // For testing only.
    fn update(&mut self, new_features: Features) {
        self.supported_features = new_features;
    }

    // For testing only.
    fn clear(&mut self) {
        self.supported_features = Features::empty_supported_features();
    }
}
