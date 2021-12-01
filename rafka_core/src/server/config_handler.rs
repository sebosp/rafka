//! From core/src/main/scala/kafka/server/ConfigHandler.scala

use super::kafka_config::KafkaConfigError;
use crate::common::config_def::Validator;
use lazy_static::lazy_static;
use regex::Regex;
use std::fmt;

pub struct ThrottledReplicaListValidator {}

impl ThrottledReplicaListValidator {
    pub fn ensure_valid_string(name: &str, value: &str) -> Result<(), KafkaConfigError> {
        Self::ensure_valid(name, value.split(",").map(|val| val.trim().to_string()).collect())
    }
}

impl Validator for ThrottledReplicaListValidator {
    type Value = Vec<String>;

    fn ensure_valid(name: &str, proposed: Self::Value) -> Result<(), KafkaConfigError> {
        lazy_static! {
            static ref PARTITION_BROKER_REGEX: Regex = Regex::new(r"([0-9]+:[0-9]+)?").unwrap();
        }
        let is_wildcard: bool = proposed.len() == 1 && proposed.first().unwrap() == "*";
        if is_wildcard
            || !proposed.iter().all(|val| PARTITION_BROKER_REGEX.is_match(val.to_string().trim()))
        {
            return Err(KafkaConfigError::InvalidValue(format!(
                "{} must be the literal '*' or a list of replicas in the following format: \
                 [partitionId]:[brokerId],[partitionId]:[brokerId],...",
                name
            )));
        }
        Ok(())
    }
}

impl fmt::Display for ThrottledReplicaListValidator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[partitionId]:[brokerId],[partitionId]:[brokerId],...")
    }
}
