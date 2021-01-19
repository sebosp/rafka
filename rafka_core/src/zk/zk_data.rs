//! This file contains objects for encoding/decoding data stored in ZooKeeper nodes (znodes).
//! core/src/main/scala/kafka/zk/ZkData.scala

use crate::server::dynamic_config_manager::ConfigType;
use zookeeper_async::Acl;
// NOTE: Maybe all of this could be moved into a hashmap or something?

/// A ZNode contains a known path or parent path of a node that could be stored in ZK
#[derive(Debug)]
pub struct ZNode {
    path: &'static str,
}

#[derive(Debug)]
pub struct ZkData {
    // old consumer path znode
    consumer_path_znode: ZNode,
    config_entity_type_user: ZNode,
    config_entity_type_broker: ZNode,
    delegation_token_auth_znode: ZNode,
    delegation_tokens_znode: ZNode,
    /* admin_znode: ZNode,
     * brokers_znode: ZNode,
     * cluster_znode: ZNode,
     * config_znode: ZNode,
     * controller_znode: ZNode,
     * controller_epoch_znode: ZNode,
     * isr_change_notification_znode: ZNode,
     * producer_id_block_znode: ZNode,
     * log_dir_event_notification_znode: ZNode,
     * extended_acl_znode: ZNode,
     * broker_ids_znode: ZNode,
     * topics_znode: ZNode,
     * config_entity_change_notification_znode: ZNode,
     * delete_topics_znode: ZNode,
     * broker_sequence_id_znode: ZNode,
     * config_types: ConfigType, */
}

impl ZkData {
    pub fn default_acls(&self, path: &str) -> Vec<Acl> {
        // NOTE: For now not caring about secure setup
        let is_secure = false;
        let mut acls: Vec<Acl> = vec![];
        // Old Consumer path is kept open as different consumers will write under this node.
        if self.consumer_path_znode.path != path && is_secure {
            acls.extend_from_slice(Acl::creator_all());
            if !self.is_sensitive_path(path) {
                acls.extend_from_slice(Acl::read_unsafe());
            }
        } else {
            acls.extend_from_slice(Acl::open_unsafe());
        }
        acls
    }

    pub fn is_sensitive_path(&self, path: &str) -> bool {
        path != "" // This used to be path != null in scala code
            && self.sensitive_root_paths().iter().any(|sensitive_path| path.starts_with(sensitive_path))
    }

    // Important: it is necessary to add any new top level Zookeeper path to the Seq
    pub fn secure_root_paths(&self) -> Vec<&str> {
        unimplemented!();
        // let mut paths = vec![
        // self.admin_znode.path,
        // self.brokers_znode.path,
        // self.cluster_znode.path,
        // self.config_znode.path,
        // self.controller_znode.path,
        // self.controller_epoch_znode.path,
        // self.isr_change_notification_znode.path,
        // self.producer_id_block_znode.path,
        // self.log_dir_event_notification_znode.path,
        // self.delegation_token_auth_znode.path,
        // self.extended_acl_znode.path,
        // ];
        // paths.extend_from_slice(&ZkAclStore.securePaths);
        // paths
    }

    // These are persistent ZK paths that should exist on kafka broker startup.
    pub fn persistent_zk_paths(&self) -> Vec<&str> {
        unimplemented!();
        // let mut paths = vec![
        // self.consumer_path_znode.path, // old consumer path
        // self.broker_ids_znode.path,
        // self.topics_znode.path,
        // self.config_entity_change_notification_znode.path,
        // self.delete_topics_znode.path,
        // self.broker_sequence_id_znode.path,
        // self.isr_change_notification_znode.path,
        // self.producer_id_block_znode.path,
        // self.log_dir_event_notification_znode.path,
        // ];
        // paths.extend_from_slice(ConfigType::all().iter().map(|x|ConfigEntityTypeZNode::path(x)));
        // paths
    }

    pub fn sensitive_root_paths(&self) -> Vec<&str> {
        vec![
            self.config_entity_type_user.path,
            self.config_entity_type_broker.path,
            self.delegation_tokens_znode.path,
        ]
    }
}

// old consumer path znode
#[derive(Debug)]
pub struct ConsumerPathZNode;
impl ConsumerPathZNode {
    pub fn path() -> &'static str {
        "/consumers"
    }
}

#[derive(Debug)]
pub struct ConfigZNode;
impl ConfigZNode {
    pub fn path() -> &'static str {
        "/config"
    }
}

#[derive(Debug)]
pub struct ConfigEntityTypeZNode;
impl ConfigEntityTypeZNode {
    pub fn path(entity_type: &str) -> String {
        format!("{}/{}", ConfigZNode::path(), entity_type)
    }
}

impl Default for ZkData {
    fn default() -> Self {
        let mut secure_root_paths = vec![];
        let mut persistent_zk_paths = vec![];
        let delegation_token_auth_znode = ZNode { path: "/delegation_token" };
        let delegation_tokens_znode =
            ZNode { path: format!("{}/tokens", delegation_token_auth_znode.path) };
        let config_type = ConfigType::default();
        ZkData {
            config_entity_type_user: ZNode {
                path: &ConfigEntityTypeZNode::path(&config_type.user),
            },
            config_entity_type_broker: ZNode {
                path: &ConfigEntityTypeZNode::path(&config_type.broker),
            },
            delegation_token_auth_znode,
            delegation_tokens_znode,
            consumer_path_znode: ZNode { path: ConsumerPathZNode::path() },
        }
    }
}
