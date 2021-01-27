//! This file contains objects for encoding/decoding data stored in ZooKeeper nodes (znodes).
//! core/src/main/scala/kafka/zk/ZkData.scala

use crate::server::dynamic_config_manager::ConfigType;
use zookeeper_async::Acl;
// NOTE: Maybe all of this could be moved into a hashmap or something?

/// `ZNode` contains a known path or parent path of a node that could be stored in ZK
#[derive(Debug)]
pub struct ZNode {
    path: String,
}

pub trait ZNodeData {
    type Data;
    fn path() -> String;
    fn decode(input: &str) -> Self;
    fn encode(self) -> String;
}

/// `ZkData`  contains a set of known paths in zookeeper
#[derive(Debug)]
pub struct ZkData {
    /// old consumer path znode
    consumer_path_znode: ZNode,
    config_entity_type_user: ZNode,
    config_entity_type_broker: ZNode,
    delegation_token_auth_znode: ZNode,
    delegation_tokens_znode: ZNode,
    broker_ids_znode: ZNode,
    topics_znode: ZNode,
    config_entity_change_notification_znode: ZNode,
    delete_topics_znode: ZNode,
    broker_sequence_id_znode: ZNode,
    isr_change_notification_znode: ZNode,
    producer_id_block_znode: ZNode,
    log_dir_event_notification_znode: ZNode,
    config_types: ConfigType,
    /* admin_znode: ZNode,
     * brokers_znode: ZNode,
     * cluster_znode: ZNode,
     * config_znode: ZNode,
     * controller_znode: ZNode,
     * controller_epoch_znode: ZNode,
     * extended_acl_znode: ZNode,
     */
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
        !path.is_empty() // This used to be path != null in scala code
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
        let mut paths: Vec<&str> = vec![
            &self.consumer_path_znode.path, // old consumer path
            &self.broker_ids_znode.path,
            &self.topics_znode.path,
            &self.config_entity_change_notification_znode.path,
            &self.delete_topics_znode.path,
            &self.broker_sequence_id_znode.path,
            &self.isr_change_notification_znode.path,
            &self.producer_id_block_znode.path,
            &self.log_dir_event_notification_znode.path,
        ];
        for path in self.config_types.all() {
            paths.push(&path);
        }
        paths
    }

    pub fn sensitive_root_paths(&self) -> Vec<&str> {
        vec![
            &self.config_entity_type_user.path,
            &self.config_entity_type_broker.path,
            &self.delegation_tokens_znode.path,
        ]
    }
}

// old consumer path znode
#[derive(Debug)]
pub struct ConsumerPathZNode;
impl ConsumerPathZNode {
    pub fn path() -> String {
        String::from("/consumers")
    }
}

#[derive(Debug)]
pub struct ConfigZNode;
impl ConfigZNode {
    pub fn path() -> String {
        String::from("/config")
    }
}

#[derive(Debug)]
pub struct BrokersZNode;
impl BrokersZNode {
    pub fn path() -> String {
        String::from("/brokers")
    }
}

#[derive(Debug)]
pub struct BrokerIdZNode;
impl BrokerIdZNode {
    pub fn path() -> String {
        format!("{}/ids", BrokersZNode::path())
    }
}

#[derive(Debug)]
pub struct TopicsZNode;
impl TopicsZNode {
    pub fn path() -> String {
        format!("{}/topics", BrokersZNode::path())
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
        let delegation_token_auth_znode = ZNode { path: String::from("/delegation_token") };
        let delegation_tokens_znode =
            ZNode { path: format!("{}/tokens", delegation_token_auth_znode.path) };
        let config_type = ConfigType::default();
        let broker_ids_znode = BrokerIdZNode::path();
        let topics_znode = TopicsZNode::path();
        let config_entity_change_notification_znode = ZNode { path: String::from("unset") };
        let delete_topics_znode = ZNode { path: String::from("unset") };
        let broker_sequence_id_znode = ZNode { path: String::from("unset") };
        let isr_change_notification_znode = ZNode { path: String::from("unset") };
        let producer_id_block_znode = ZNode { path: String::from("unset") };
        let log_dir_event_notification_znode = ZNode { path: String::from("unset") };
        let config_types = ConfigType::default();
        ZkData {
            config_entity_type_user: ZNode { path: ConfigEntityTypeZNode::path(&config_type.user) },
            config_entity_type_broker: ZNode {
                path: ConfigEntityTypeZNode::path(&config_type.broker),
            },
            delegation_token_auth_znode,
            delegation_tokens_znode,
            consumer_path_znode: ZNode { path: ConsumerPathZNode::path() },
            broker_ids_znode,
            topics_znode,
            config_entity_change_notification_znode,
            delete_topics_znode,
            broker_sequence_id_znode,
            isr_change_notification_znode,
            producer_id_block_znode,
            log_dir_event_notification_znode,
            config_types,
        }
    }
}
