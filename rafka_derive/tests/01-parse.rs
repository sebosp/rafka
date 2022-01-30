use rafka_derive::ConfigDef;
#[derive(ConfigDef)]
pub struct Test1Properties {
    #[config_def(key = "log.dir", default = "")]
    pub log_dir: u8,
}

fn main() {
    let props = Test1Properties::properties_builder();
    props.init();
}
