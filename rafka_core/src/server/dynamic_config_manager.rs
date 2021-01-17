//! Dynamic Config Manager
//! core/src/main/scala/kafka/server/DynamicConfigManager.scala
//!
//! This class initiates and carries out config changes for all entities defined in ConfigType.
//!
//!  It works as follows.
//!
//!  Config is stored under the path: /config/entityType/entityName
//!    E.g. /config/topics/<topic_name> and /config/clients/<clientId>
//!  This znode stores the overrides for this entity in properties format with defaults stored using
//! entityName "<default>".  Multiple entity names may be specified (eg. <user, client-id> quotas)
//! using a hierarchical path:    E.g. /config/users/<user>/clients/<clientId>
//!
//!  To avoid watching all topics for changes instead we have a notification path
//!    /config/changes
//!  The DynamicConfigManager has a child watch on this path.
//!
//!  To update a config we first update the config properties. Then we create a new sequential
//!  znode under the change path which contains the name of the entityType and entityName that was
//! updated, say    /config/changes/config_change_13321
//!  The sequential znode contains data in this format: {"version" : 1,
//! "entity_type":"topic/client", "entity_name" : "topic_name/client_id"}  This is just a
//! notification--the actual config change is stored only once under the
//! /config/entityType/entityName path.  Version 2 of notifications has the format: {"version" : 2,
//! "entity_path":"entity_type/entity_name"}  Multiple entities may be specified as a hierarchical
//! path (eg. users/<user>/clients/<clientId>).
//!
//!  This will fire a watcher on all brokers. This watcher works as follows. It reads all the config
//! change notifications.  It keeps track of the highest config change suffix number it has applied
//! previously. For any previously applied change it finds  it checks if this notification is larger
//! than a static expiration time (say 10mins) and if so it deletes this notification.  For any new
//! changes it reads the new configuration, combines it with the defaults, and updates the existing
//! config.
//!
//!  Note that config is always read from the config path in zk, the notification is just a trigger
//! to do so. So if a broker is  down and misses a change that is fine--when it restarts it will be
//! loading the full config anyway. Note also that  if there are two consecutive config changes it
//! is possible that only the last one will be applied (since by the time the  broker reads the
//! config the both changes may have been made). In this case the broker would needlessly refresh
//! the config twice,  but that is harmless.
//!
//!  On restart the config manager re-processes all notifications. This will usually be wasted work,
//! but avoids any race conditions  on startup where a change might be missed between the initial
//! config load and registering for change notifications.

use crate::server::kafka_server::ConfigHandler;
use crate::zk::admin_zk_client::AdminZkClient;
use crate::zk::kafka_zk_client::KafkaZkClient;
use std::collections::HashMap;
use std::time::SystemTime;

/// Represents all the entities that can be configured via ZK
#[derive(Debug)]
pub struct ConfigType {
    pub topic: &'static str,
    pub client: &'static str,
    pub user: &'static str,
    pub broker: &'static str,
}

impl ConfigType {
    // There's a sequence created for this
    // val all = Seq(Topic, Client, User, Broker)
    pub fn all(&self) -> Vec<&str> {
        vec![self.topic, self.client, self.user, self.broker]
    }
}

impl Default for ConfigType {
    fn default() -> Self {
        ConfigType { topic: "topics", client: "clients", user: "users", broker: "brokers" }
    }
}

#[derive(Debug)]
pub struct ConfigEntityName {
    default: &'static str,
}
impl Default for ConfigEntityName {
    fn default() -> Self {
        ConfigEntityName { default: "<default>" }
    }
}
#[derive(Debug)]
pub struct DynamicConfigManager {
    zk_client: KafkaZkClient,
    config_handlers: HashMap<String, ConfigHandler>,
    change_expiration_ms: u32,      // Long = 15*60*1000,
    time: SystemTime,               // = Time.SYSTEM
    admin_zk_client: AdminZkClient, // = new AdminZkClient(zkClient)
}
