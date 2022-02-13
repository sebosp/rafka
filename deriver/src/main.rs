use rafka_core::common::config_def::{ConfigDef, ConfigDefImportance, PartialConfigDef};
use rafka_core::server::kafka_config::{ConfigSet, KafkaConfigError, TrySetProperty};
use rafka_derive::ConfigDef;
use std::str::FromStr;

pub const LOG_DIR_PROP: &str = "log.dir";
pub const LOG_DIRS_PROP: &str = "log.dirs";
pub const LOG_ROLL_TIME_MILLIS_PROP: &str = "log.roll.ms";
pub const LOG_DIR_DOC: &str = "Some Docs";
pub const A_BOOL: &str = "a.bool";
pub const CUST_TYPE_PROP: &str = "Meh";

#[derive(Debug, Clone)]
pub struct CustType {
    data: Vec<String>,
}

impl FromStr for CustType {
    type Err = KafkaConfigError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        Ok(Self { data: input.split(',').map(|x| x.to_string()).collect() })
    }
}

#[derive(ConfigDef)]
pub struct Test1Properties {
    #[config_def(key = LOG_DIR_PROP,
        default = "/tmp/kafka-dir",
        importance = "High",
        doc = LOG_DIR_DOC,
        with_validator_fn,
        no_default_resolver)]
    pub log_dir: ConfigDef<String>,
    #[config_def(key = LOG_DIRS_PROP, default = "something with spaces", importance = "High")]
    pub log_dirs: ConfigDef<String>,
    #[config_def(key = CUST_TYPE_PROP, with_default_fn, importance = "High")]
    pub custom_type: ConfigDef<CustType>,
    #[config_def(key = LOG_ROLL_TIME_MILLIS_PROP, default = -32)]
    pub log_roll_time_millis: ConfigDef<i64>,
    #[config_def(key = A_BOOL, default = true)]
    pub a_bool: ConfigDef<bool>,
}

impl Test1Properties {
    pub fn validate_log_dir(&self) -> Result<(), KafkaConfigError> {
        Ok(())
    }

    pub fn resolve_log_dir(&mut self) -> Result<String, KafkaConfigError> {
        Ok(String::from("meh"))
    }

    pub fn default_custom_type() -> CustType {
        CustType { data: vec![] }
    }
}

fn main() {
    let meh = String::from_str("aoeu").unwrap();
    println!("meh: {}", meh);
    let props = Test1Properties::default();
    props.init();
}
