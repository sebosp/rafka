//! This class is used for specifying the set of expected configurations.
//! From clients/src/main/java/org/apache/kafka/common/config/ConfigDef.java
use crate::server::kafka_config::KafkaConfigError;
use std::fmt;
use std::str::FromStr;
use tracing::{error, info};

/// Perform  validation of configuration read from .properties, zookeeper, etc
pub trait Validator {
    type Value;
    fn ensure_valid(name: &str, value: Self::Value) -> Result<(), KafkaConfigError> {
        Ok(())
    }
}

/// `ConfigDefImportance` provides the levels of importance that different java_properties
/// have.
#[derive(Debug, PartialEq, Clone)]
pub enum ConfigDefImportance {
    High,
    Medium,
    Low,
}

/// `ConfigDef` defines the configuration properties, how they can be resolved from other
/// values and their defaults This should be later transformed into a derivable from something like
/// DocOpt.
pub struct ConfigDef<T> {
    /// The configuration key that is used to apply this value
    pub key: String,
    /// How important the configuration definition is
    importance: ConfigDefImportance,
    /// `default` of the value, this would be parsed and transformed into each field type from
    /// KafkaConfig
    default: Option<T>,
    /// The documentation of the field, used for showing errors
    doc: String,
    /// Whether or not this variable was provided by the configuration file.
    provided: bool,
    /// The current value, be it the default or overwritten by config
    value: Option<T>,
    /// A validator to ensure the new field value is correct
    validator: Option<Box<dyn Fn(Option<&T>) -> Result<(), KafkaConfigError>>>,
}

impl<T> fmt::Debug for ConfigDef<T>
where
    T: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConfigDef")
            .field("key", &self.key)
            .field("importance", &self.importance)
            .field("default", &self.default)
            .field("doc", &self.doc)
            .field("provided", &self.provided)
            .field("value", &self.value)
            .field("validator_exists", &self.validator.is_some())
            .finish()
    }
}

impl<T> PartialEq for ConfigDef<T>
where
    T: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
            && self.importance == other.importance
            && self.default == other.default
            && self.doc == other.doc
            && self.provided == other.provided
            && self.value == other.value
    }
}

impl<T> Default for ConfigDef<T> {
    fn default() -> Self {
        Self {
            importance: ConfigDefImportance::Low,
            doc: String::from("TODO: Missing Docs"),
            key: String::from("unset.key"),
            default: None,
            provided: false,
            value: None,
            validator: None,
        }
    }
}

impl<T> ConfigDef<T>
where
    T: FromStr,
    KafkaConfigError: From<<T as FromStr>::Err>,
    <T as FromStr>::Err: std::fmt::Display,
    T: std::fmt::Debug,
{
    pub fn with_importance(mut self, importance: ConfigDefImportance) -> Self {
        self.importance = importance;
        self
    }

    /// Sets the `key` value, this comes from const &str values in the calling modules
    pub fn with_key(mut self, key: &str) -> Self {
        self.key = key.to_string();
        self
    }

    pub fn with_doc(mut self, doc: String) -> Self {
        self.doc = doc;
        self
    }

    pub fn with_default(mut self, default: T) -> Self
    where
        T: Clone,
    {
        self.value = Some(default.clone());
        self.default = Some(default);
        self
    }

    pub fn with_parseable_default(mut self, default: String) -> Self
    where
        T: Clone,
    {
        // Pre-fill the value with the default, if it doesn't parse we should panic as that means
        // a bug in our code, not the config params
        // DEPRECATED: This is no longer used as we no longer take String'ed numbers/etc
        match default.parse::<T>() {
            Ok(val) => {
                self.value = Some(val.clone());
                self.default = Some(val);
            },
            Err(err) => {
                error!("Unable to parse default property for {:?}: {}", self.key, err);
                panic!();
            },
        }
        self
    }

    pub fn with_validator(
        mut self,
        validator: Box<dyn Fn(Option<&T>) -> Result<(), KafkaConfigError>>,
    ) -> Self {
        self.validator = Some(validator);
        self
    }

    pub fn set_value(&mut self, value: T) {
        self.value = Some(value);
        self.provided = true;
    }

    pub fn try_set_parsed_value(&mut self, value: &str) -> Result<(), KafkaConfigError> {
        match value.parse::<_>() {
            Ok(val) => {
                self.set_value(val);
                Ok(())
            },
            Err(err) => {
                error!("Unable to parse property {:?} : {}. Doc: {}", value, err, self.doc);
                Err(KafkaConfigError::from(err))
            },
        }
    }

    pub fn value_in_list(
        data: Option<&T>,
        valid_list: Vec<&T>,
        key: &str,
    ) -> Result<(), KafkaConfigError>
    where
        T: PartialEq + fmt::Display,
    {
        match data {
            Some(val) => {
                if valid_list.iter().any(|item| val == *item) {
                    Err(KafkaConfigError::InvalidValue(format!(
                        "{}: '{}' should be in list {:?}",
                        key, val, valid_list
                    )))
                } else {
                    Ok(())
                }
            },
            None => {
                error!("Running value_in_list() with no value provided for ConfigDef {:?}", data);
                Err(KafkaConfigError::ComparisonOnNone(key.to_string()))
            },
        }
    }

    pub fn at_least(data: Option<&T>, rhs: &T, key: &str) -> Result<(), KafkaConfigError>
    where
        T: PartialEq + PartialOrd + fmt::Display,
    {
        match data {
            Some(val) => {
                if val < rhs {
                    Err(KafkaConfigError::InvalidValue(format!(
                        "{}: '{}' should be at least {}",
                        key, val, rhs
                    )))
                } else {
                    Ok(())
                }
            },
            None => {
                error!("Running at_least() with no value provided for ConfigDef {:?}", data);
                Err(KafkaConfigError::ComparisonOnNone(key.to_string()))
            },
        }
    }

    pub fn get_value(&self) -> Option<&T> {
        self.value.as_ref()
    }

    pub fn get_importance(&self) -> &ConfigDefImportance {
        &self.importance
    }

    pub fn is_provided(&self) -> bool {
        self.provided
    }

    pub fn has_default(&self) -> bool {
        self.default.is_some()
    }

    pub fn validate(&self) -> Result<(), KafkaConfigError> {
        match &self.validator {
            Some(validator) => (validator)(self.value.as_ref()),
            None => Ok(()),
        }
    }

    /// build() usually consumes self, however in this case, the ConfigDef should live on, a
    /// property may be read initially from .properties file and later on be re-configured via
    /// admin cli or KV-store (zkp, raft), if we consume self, the validator, the default, etc
    /// would be gone and couldn't be re-used.
    pub fn build(&mut self) -> Result<T, KafkaConfigError>
    where
        T: Clone,
    {
        self.validate()?;
        match &self.value {
            Some(value) => Ok(value.clone()),
            None => Err(KafkaConfigError::MissingKey(self.key.to_string())),
        }
    }

    /// `resolve` attempts to resolve the current value, if the current value is None it tries to
    /// find the value from a fallback property. If the fallback property is None, a
    /// KafkaConfigError is returned.
    pub fn resolve(&mut self, fallback: &Self) -> Result<(), KafkaConfigError>
    where
        T: Clone,
    {
        if let Some(_) = &self.value {
            Ok(())
        } else {
            info!(
                "Unspecified property {}: attempting to use fallback property {} as value",
                self.key, fallback.key
            );
            match &fallback.value {
                Some(_) => {
                    self.value = fallback.value.clone();
                    Ok(())
                },
                None => {
                    error!(
                        "Unspecified property {}: fallback property {} has no value",
                        self.key, fallback.key
                    );
                    Err(KafkaConfigError::MissingKey(fallback.key.to_string()))
                },
            }
        }
    }
}
