/// Dynamic Broker Configurations
/// core/src/main/scala/kafka/server/DynamicBrokerConfig.scala
/// Dynamic broker configurations are stored in ZooKeeper and may be defined at two levels:
/// - Per-broker configs persisted at `/configs/brokers/{brokerId}`: These can be
///   described/altered using AdminClient using the resource name brokerId.
/// - Cluster-wide defaults persisted at `/configs/brokers/<default>`: These can be
///   described/altered using AdminClient using an empty resource name.
/// The order of precedence for broker configs is:
///   - DYNAMIC_BROKER_CONFIG: stored in ZK at /configs/brokers/{brokerId}
///   - DYNAMIC_DEFAULT_BROKER_CONFIG: stored in ZK at /configs/brokers/<default>
///   - STATIC_BROKER_CONFIG: properties that broker is started up with, typically from
///     server.properties file
///   - DEFAULT_CONFIG: Default configs defined in KafkaConfig
/// Log configs use topic config overrides if defined and fallback to broker defaults using the
/// order of precedence above. Topic config overrides may use a different config name from the
/// default broker config. See [[kafka.log.LogConfig#TopicConfigSynonyms]] for the mapping.
///
/// AdminClient returns all config synonyms in the order of precedence when configs are
/// described with `includeSynonyms`. In addition to configs that may be defined with the same
/// name at different levels, some configs have additional synonyms.
/// - Listener configs may be defined using the prefix
///   `listener.name.{listenerName}.{configName}`. These may be configured as dynamic or static
///   broker configs. Listener configs have higher precedence than the base configs that don't
///   specify the listener name. Listeners without a listener config use the base config. Base
///   configs may be defined only as STATIC_BROKER_CONFIG or DEFAULT_CONFIG and cannot be
///   updated dynamically.
/// - Some configs may be defined using multiple properties. For example, `log.roll.ms` and
///   `log.roll.hours` refer to the same config that may be defined in milliseconds or hours.
///   The order of precedence of these synonyms is described in the docs of these configs in
///   [[kafka.server.KafkaConfig]].
use crate::majordomo::{AsyncTask, AsyncTaskError};
use crate::server::dynamic_config::{DynamicBrokerConfigDefs, DynamicConfig};
use crate::server::kafka_config::{self, KafkaConfig, KafkaConfigError};
use crate::zk::admin_zk_client::AdminZkClient;
use lazy_static::lazy_static;
use regex::Regex;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tracing::{debug, error};

fn listener_config_regex_captures(text: &str) -> Option<String> {
    lazy_static! {
        static ref LISTENER_CONFIG_REGEX: Regex =
            Regex::new(r"listener\.name\.[^.]*\.(.*)").unwrap();
    }
    match LISTENER_CONFIG_REGEX.captures(text) {
        Some(val) => Some(val[1].to_string()),
        None => None,
    }
}

#[derive(Debug, PartialEq, Clone, Default)]
pub struct DynamicBrokerConfig {
    pub kafka_config: KafkaConfig,
    dynamic_default_configs: HashMap<String, String>,
    dynamic_broker_configs: HashMap<String, String>,
    per_broker_configs: Vec<String>,
    cluster_level_listener_configs: Vec<String>,
    static_broker_configs: HashMap<String, String>,
}

impl DynamicBrokerConfig {
    pub fn new(kafka_config: KafkaConfig) -> Self {
        let cluster_level_listener_configs = vec![kafka_config::MAX_CONNECTIONS_PROP.to_string()];
        let (_, per_broker_configs): (Vec<_>, Vec<_>) =
            DynamicListenerConfig::reconfigurable_configs()
                .into_iter()
                .partition(|e| (cluster_level_listener_configs.contains(&e)));
        Self {
            kafka_config,
            dynamic_default_configs: HashMap::new(),
            dynamic_broker_configs: HashMap::new(),
            per_broker_configs,
            cluster_level_listener_configs,
            // In the original code, originalsFromThisConfig is located in KafkaConfig and is used
            // only in DynamicBrokerConfig
            static_broker_configs: HashMap::new(),
        }
    }

    pub async fn initialize(&mut self, tx: mpsc::Sender<AsyncTask>) -> Result<(), AsyncTaskError> {
        let admin_zk_client = AdminZkClient::new(tx.clone());
        let default_broker_config = admin_zk_client.fetch_default_broker_config().await?;
        self.update_default_config(default_broker_config)?;
        let props =
            admin_zk_client.fetch_specific_broker_config(self.kafka_config.broker_id).await?;
        self.update_broker_config(self.kafka_config.broker_id, props)?;
        Ok(())
    }

    /// `validate_config_types` Creates a copy of `props` with the basename loaded from the
    /// listener.name.<name>.BASE_NAME and validates its values
    fn validate_config_types(props: &HashMap<String, String>) -> Result<(), KafkaConfigError> {
        let mut base_props: HashMap<String, String> = HashMap::new();
        for (prop_key, prop_value) in props {
            if let Some(base_name) = listener_config_regex_captures(prop_key) {
                base_props.insert(base_name, prop_value.to_string());
            } else {
                base_props.insert(prop_key.to_string(), prop_value.to_string());
            }
        }
        DynamicBrokerConfigDefs::validate(base_props)?;
        Ok(())
    }

    /// `remove_invalid_configs` ignores config keys that are invalid and returns a HashMap of
    /// valid keys
    fn remove_invalid_configs(
        props: &HashMap<String, String>,
        per_broker_config: bool,
    ) -> HashMap<String, String> {
        match Self::validate_config_types(&props) {
            Ok(_) => props.clone(),
            Err(err) => {
                let mut valid_props = props.clone();
                let mut invalid_props = HashMap::new();
                for (prop_key, prop_value) in props {
                    let mut props1: HashMap<String, String> = HashMap::new();
                    props1.insert(prop_key.to_string(), prop_value.to_string());
                    match Self::validate_config_types(&props1) {
                        Ok(()) => {
                            debug!("Property {} is valid", prop_key);
                        },
                        Err(_) => {
                            debug!("Property {} is invalid, will ignore value", prop_key);
                            valid_props.remove(prop_key);
                            invalid_props.insert(prop_key.to_string(), prop_value.to_string());
                        },
                    };
                }
                let config_source = if per_broker_config { "broker" } else { "default cluster" };
                error!(
                    "Dynamic {} config contains invalid values: {:?}, these configs will be \
                     ignored. {:?}",
                    config_source, invalid_props, err
                );
                valid_props
            },
        }
    }

    /// `non_dynamic_configs` Returns a list of config keys that are not dynamic (can only be set
    /// at startup)
    pub fn non_dynamic_configs(props: &HashMap<String, String>) -> Vec<String> {
        let res: Vec<String> = props.keys().cloned().collect();
        let non_dynamic_props = DynamicConfig::default().broker.non_dynamic_props;
        let (res, _): (Vec<_>, Vec<_>) =
            res.into_iter().partition(|e| non_dynamic_props.contains(&e));
        res
    }

    /// `security_configs_without_listener_prefix` TODO.
    fn security_configs_without_listener_prefix(_: HashMap<String, String>) -> Vec<String> {
        unimplemented!()
    }

    /// `remove_invalid_props` removes a Vec of invalid_prop_name keys from a property HashMap
    fn remove_invalid_props(
        properties: &mut HashMap<String, String>,
        invalid_prop_names: Vec<String>,
        error_message: String,
    ) {
        if !invalid_prop_names.is_empty() {
            for invalid_prop_name in &invalid_prop_names {
                properties.remove(invalid_prop_name);
            }
            error!("{}: {:?}", error_message, invalid_prop_names);
        }
    }

    fn per_broker_configs(
        props: &HashMap<String, String>,
        per_broker_configs: &[String],
        cluster_level_listener_configs: &[String],
    ) -> Vec<String> {
        let config_names: Vec<String> = props.keys().cloned().collect();
        // Find the intersection between config_names and per_broker_configs
        // On the config_names, capture the listener name using LISTENER_CONFIG_REGEX and retain
        // the entries that are not in the cluster_level_listener_configs
        let (intersect, _): (Vec<_>, Vec<_>) =
            config_names.into_iter().partition(|e| per_broker_configs.contains(&e));
        let (mut filtered, _): (Vec<_>, Vec<_>) = intersect.into_iter().partition(|e| {
            if let Some(base_name) = listener_config_regex_captures(&e) {
                !cluster_level_listener_configs.contains(&base_name)
            } else {
                false
            }
        });
        filtered.sort();
        // In t he original code, ++ operator performs concat which also dedups()
        filtered.dedup();
        filtered
    }

    fn from_persistent_props(
        &self,
        persistent_props: &HashMap<String, String>,
        per_broker_config: bool,
    ) -> HashMap<String, String> {
        // Remove all invalid configs from `props`
        let mut props = Self::remove_invalid_configs(&persistent_props, per_broker_config);
        let non_dynamic_props = Self::non_dynamic_configs(&props);
        Self::remove_invalid_props(
            &mut props,
            non_dynamic_props,
            String::from("Non-dynamic configs configured in ZooKeeper will be ignored"),
        );
        // Self::remove_invalid_props(&mut props,
        // Self::security_configs_without_listener_prefix(props), "Security configs can be
        // dynamically updated only using listener prefix, base configs will be ignored");
        let per_broker_configs = Self::per_broker_configs(
            &props,
            &self.per_broker_configs,
            &self.cluster_level_listener_configs,
        );
        if !per_broker_config {
            Self::remove_invalid_props(
                &mut props,
                per_broker_configs,
                String::from("Per-broker configs defined at default cluster level will be ignored"),
            )
        }
        props
    }

    fn broker_config_synonyms(name: &str, match_listener_override: bool) -> Vec<String> {
        let res = match name {
            kafka_config::LOG_ROLL_TIME_MILLIS_PROP | kafka_config::LOG_ROLL_TIME_HOURS_PROP => {
                vec![
                    kafka_config::LOG_ROLL_TIME_MILLIS_PROP,
                    kafka_config::LOG_ROLL_TIME_HOURS_PROP,
                ]
            },
            kafka_config::LOG_ROLL_TIME_JITTER_MILLIS_PROP
            | kafka_config::LOG_ROLL_TIME_JITTER_HOURS_PROP => vec![
                kafka_config::LOG_ROLL_TIME_JITTER_MILLIS_PROP,
                kafka_config::LOG_ROLL_TIME_JITTER_HOURS_PROP,
            ],
            kafka_config::LOG_FLUSH_INTERVAL_MS_PROP =>
            // LogFlushSchedulerIntervalMsProp is used as default
            {
                vec![
                    kafka_config::LOG_FLUSH_INTERVAL_MS_PROP,
                    kafka_config::LOG_FLUSH_SCHEDULER_INTERVAL_MS_PROP,
                ]
            },
            kafka_config::LOG_RETENTION_TIME_MILLIS_PROP
            | kafka_config::LOG_RETENTION_TIME_MINUTES_PROP
            | kafka_config::LOG_RETENTION_TIME_HOURS_PROP => vec![
                kafka_config::LOG_RETENTION_TIME_MILLIS_PROP,
                kafka_config::LOG_RETENTION_TIME_MINUTES_PROP,
                kafka_config::LOG_RETENTION_TIME_HOURS_PROP,
            ],
            _ => {
                if match_listener_override {
                    if let Some(base_name) = listener_config_regex_captures(name) {
                        // `ListenerMechanismConfigs` are specified as
                        // listenerPrefix.mechanism.<configName>
                        // and other listener configs are specified as listenerPrefix.<configName>
                        // Add <configName> as a synonym in both cases. RAFKA NOTE: This is all
                        // about SASL and Auth which is not the target for
                        // this yet.
                        error!(
                            "broker_config_synonyms: Property name {} may not be supported, no \
                             sasl config is done yet",
                            name
                        );
                        if !base_name.contains("sasl\\.") {
                            return vec![name.to_string(), base_name];
                        }
                    }
                }
                vec![name]
            },
        };
        res.into_iter().map(|val| val.to_string()).collect()
    }

    /// Updates the values in `props` with the new values from `props_override`.
    /// The Synonyms of updated configs are removed from `props` so that config with higher
    /// precedence is applied. For instance, generally `log.roll.ms` takes precedence over
    /// `log.roll.hours` if configured in the same context, but, if:
    /// 1 - server.properties contains `log.roll.ms`
    /// 2 - dynamic config sets `log.roll.hours`
    /// Then:
    /// 1 - `log.roll.hours` from the dynamic configuration will be used
    /// 2 - `log.roll.ms` will be removed from `props`
    fn override_props(
        &self,
        props: &mut HashMap<String, String>,
        props_override: &HashMap<String, String>,
    ) {
        for (override_key, override_value) in props_override {
            // TODO: disable `matchListenerOverride` so that base configs corresponding to listener
            // configs are not removed. TODO: Base configs should not be removed since
            // they may be used by other listeners. It is ok to retain them in `props`
            // since base configs cannot be dynamically updated and listener-specific configs have
            // the higher precedence.
            for synonym in Self::broker_config_synonyms(&override_key, false) {
                props.remove(&synonym);
            }
            props.insert(override_key.to_string(), override_value.to_string());
        }
    }

    fn process_reconfiguration(
        &self,
        _new_props: HashMap<String, String>,
        _validate_only: bool,
    ) -> (KafkaConfig, Vec<String>) {
        unimplemented!();
    }

    /// `update_current_config` gathers static broker configs, then adds the default broker
    /// properties and then adds the broker-specific properties, if there are  differences to the
    /// current config, then a reconfiguration event must happen, at this moment this is not
    /// handled.
    fn update_current_config(&mut self) -> Result<(), String> {
        let mut new_props = self.static_broker_configs.clone();
        self.override_props(&mut new_props, &self.dynamic_default_configs);
        self.override_props(&mut new_props, &self.dynamic_broker_configs);
        let old_config = self.kafka_config.clone();
        let (new_config, broker_reconfigurables_to_update) =
            self.process_reconfiguration(new_props, false);
        if new_config != self.kafka_config {
            self.kafka_config = new_config;

            for reconfigurable in &broker_reconfigurables_to_update {
                // RAFKA TODO: The broker_reconfigurables_to_update should be a
                // Vec<BrokerReconfigurable> and then for each item that needs to be updated, we
                // should iterate so that it can move from previouus config to new config
                error!(
                    "NOT implemented: Should reconfigure {} from {:?} to {:?}",
                    reconfigurable, old_config, self.kafka_config
                );
            }
        }
        Ok(())
    }

    fn update_default_config(
        &mut self,
        persistent_props: HashMap<String, String>,
    ) -> Result<(), AsyncTaskError> {
        let per_broker_config = false;
        let props = self.from_persistent_props(&persistent_props, per_broker_config);
        self.dynamic_default_configs.clear();
        for (prop_key, prop_value) in props {
            self.dynamic_default_configs.insert(prop_key, prop_value);
        }
        match self.update_current_config() {
            Ok(()) => Ok(()),
            Err(err) => {
                // RAFKA TODO: Is this an AsyncTaskError ? Who should act on it?
                error!(
                    "Cluster default configs could not be applied: {:?}: {:?}",
                    persistent_props, err
                );
                Ok(())
            },
        }
    }

    fn update_broker_config(
        &mut self,
        broker_id: i32,
        persistent_props: HashMap<String, String>,
    ) -> Result<(), AsyncTaskError> {
        let per_broker_config = true;
        let props = self.from_persistent_props(&persistent_props, per_broker_config);
        self.dynamic_default_configs.clear();
        for (prop_key, prop_value) in props {
            self.dynamic_default_configs.insert(prop_key, prop_value);
        }
        match self.update_current_config() {
            Ok(()) => Ok(()),
            Err(err) => {
                error!(
                    "Per-broker configs of {} could not be applied: {:?}: {:?}",
                    broker_id, persistent_props, err
                );
                Ok(())
            },
        }
    }
}

pub struct DynamicListenerConfig {}

impl DynamicListenerConfig {
    pub fn reconfigurable_configs() -> Vec<String> {
        vec![
            // Listener configs
            kafka_config::ADVERTISED_LISTENERS_PROP.to_string(),
            kafka_config::LISTENERS_PROP.to_string(),
            // RAFKA NOTE: No SSL, No SASL
            // Connection limit
            kafka_config::MAX_CONNECTIONS_PROP.to_string(),
        ]
    }
}

pub trait BrokerReconfigurable {
    fn reconfigurable_configs(&self) -> Vec<String>;

    fn validate_reconfiguration(&self, new_config: KafkaConfig);

    fn reconfigure(&self, old_config: KafkaConfig, new_config: KafkaConfig);
}
