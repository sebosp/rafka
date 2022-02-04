use rafka_core::common::config_def::{ConfigDef, ConfigDefImportance, PartialConfigDef};
use rafka_core::server::kafka_config::{ConfigSet, KafkaConfigError};
use rafka_derive::ConfigDef;

#[derive(ConfigDef)]
pub struct Test1Properties {
    #[config_def(key = "log.dir", default = "/tmp/kafka-dir", importance = "High")]
    pub log_dir: ConfigDef<String>,
    #[config_def(key = "log.dirs", default = "")]
    pub log_dirs: ConfigDef<String>,
    #[config_def(key = "log.roll.ms", default = 32.0)]
    pub log_roll_time_millis: ConfigDef<i64>,
}

// impl Test1Properties {
// pub fn new() -> Self {
// Self { log_dir: String::from("/"), log_dirs: String::from("") }
// }
// }

fn main() {
    let props = Test1Properties::default();
    props.init();
}
