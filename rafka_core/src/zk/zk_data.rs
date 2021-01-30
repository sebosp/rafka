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

impl Default for ZNode {
    fn default() -> Self {
        Self { path: String::from("unset") }
    }
}

pub trait ZNodeData {
    fn path(&self) -> &str;
    // fn decode(input: &str) -> Self;
    // fn encode(self) -> String;
}

impl ZNodeData for ZNode {
    fn path(&self) -> &str {
        &self.path
    }
}

/// `ZkData`  contains a set of known paths in zookeeper
#[derive(Debug)]
pub struct ZkData {
    /// old consumer path znode
    consumer_path: ConsumerPathZNode,
    config_entity_type_user: ConfigEntityTypeUserZNode,
    config_entity_type_broker: ConfigEntityTypeBrokerZNode,
    delegation_token_auth: DelegationTokenAuthZNode,
    delegation_tokens: DelegationTokensZNode,
    broker_ids: ZNode,
    topics: ZNode,
    config_entity_change_notification: ConfigEntityChangeNotificationZNode,
    delete_topics: DeleteTopicsZNode,
    broker_sequence_id: BrokerSequenceIdZNode,
    isr_change_notification: IsrChangeNotificationZNode,
    producer_id_block: ZNode,
    log_dir_event_notification: ZNode,
    config_types: ConfigType,
    admin: AdminZNode,
    /* brokers: ZNode,
     * cluster: ZNode,
     * config: ZNode,
     * controller: ZNode,
     * controller_epoch: ZNode,
     * extended_acl: ZNode,
     */
}

impl ZkData {
    pub fn default_acls(&self, path: &str) -> Vec<Acl> {
        // NOTE: For now not caring about secure setup
        let is_secure = false;
        let mut acls: Vec<Acl> = vec![];
        // Old Consumer path is kept open as different consumers will write under this node.
        if self.consumer_path.0.path() != path && is_secure {
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
        // self.admin.path,
        // self.brokers.path,
        // self.cluster.path,
        // self.config.path,
        // self.controller.path,
        // self.controller_epoch.path,
        // self.isr_change_notification.path,
        // self.producer_id_block.path,
        // self.log_dir_event_notification.path,
        // self.delegation_token_auth.path,
        // self.extended_acl.path,
        // ];
        // paths.extend_from_slice(&ZkAclStore.securePaths);
        // paths
    }

    // These are persistent ZK paths that should exist on kafka broker startup.
    pub fn persistent_zk_paths(&self) -> Vec<&str> {
        let mut paths: Vec<&str> = vec![
            &self.consumer_path.0.path, // old consumer path
            &self.broker_ids.path,
            &self.topics.path,
            &self.config_entity_change_notification.0.path(),
            &self.delete_topics.0.path(),
            &self.broker_sequence_id.0.path(),
            &self.isr_change_notification.0.path(),
            &self.producer_id_block.path,
            &self.log_dir_event_notification.path,
        ];
        for path in self.config_types.all() {
            paths.push(&path);
        }
        paths
    }

    pub fn sensitive_root_paths(&self) -> Vec<&str> {
        vec![
            &self.config_entity_type_user.0.path,
            &self.config_entity_type_broker.0.path,
            &self.delegation_tokens.0.path(),
        ]
    }
}

// source line: 70
#[derive(Debug)]
pub struct ConfigZNode(ZNode);
impl Default for ConfigZNode {
    fn default() -> Self {
        Self(ZNode { path: String::from("/config") })
    }
}

// source line: 74
#[derive(Debug)]
pub struct BrokersZNode(ZNode);
impl Default for BrokersZNode {
    fn default() -> Self {
        Self(ZNode { path: String::from("/brokers") })
    }
}

// source line: 78
#[derive(Debug)]
pub struct BrokerIdZNode(ZNode);
impl BrokerIdZNode {
    pub fn build(brokers_znode: &BrokersZNode) -> Self {
        Self(ZNode { path: format!("{}/ids", brokers_znode.0.path()) })
    }
}

// source line: 281
#[derive(Debug)]
pub struct TopicsZNode(ZNode);
impl TopicsZNode {
    pub fn build(brokers_znode: &BrokersZNode) -> Self {
        Self(ZNode { path: format!("{}/topics", brokers_znode.0.path()) })
    }
}

// source line: 361 (based on)
#[derive(Debug)]
pub struct ConfigEntityTypeUserZNode(ZNode);
impl ConfigEntityTypeUserZNode {
    pub fn build(config_znode: &ConfigZNode, config_type: &ConfigType) -> Self {
        Self(ZNode { path: format!("{}/{}", config_znode.0.path(), config_type.user) })
    }
}

// source line: 361 (based on)
#[derive(Debug)]
pub struct ConfigEntityTypeBrokerZNode(ZNode);
impl ConfigEntityTypeBrokerZNode {
    pub fn build(config_znode: &ConfigZNode, config_type: &ConfigType) -> Self {
        Self(ZNode { path: format!("{}/{}", config_znode.0.path(), config_type.broker) })
    }
}

// source line: 382
#[derive(Debug)]
pub struct ConfigEntityChangeNotificationZNode(ZNode);
impl ConfigEntityChangeNotificationZNode {
    pub fn build(config_znode: &ConfigZNode) -> Self {
        Self(ZNode { path: format!("{}/changes", config_znode.0.path()) })
    }
}

// source line: 393
#[derive(Debug)]
pub struct IsrChangeNotificationZNode(ZNode);
impl Default for IsrChangeNotificationZNode {
    fn default() -> Self {
        Self(ZNode { path: String::from("/isr_change_notification") })
    }
}

// source line: 436
#[derive(Debug)]
pub struct AdminZNode(ZNode);
impl Default for AdminZNode {
    fn default() -> Self {
        Self(ZNode { path: String::from("/admin") })
    }
}

// source line: 440
#[derive(Debug)]
pub struct DeleteTopicsZNode(ZNode);
impl DeleteTopicsZNode {
    pub fn build(admin_znode: &AdminZNode) -> Self {
        Self(ZNode { path: format!("{}/delete_topics", admin_znode.0.path()) })
    }
}

// old consumer path znode
// source line: 511
#[derive(Debug)]
pub struct ConsumerPathZNode(ZNode);
impl Default for ConsumerPathZNode {
    fn default() -> Self {
        Self(ZNode { path: String::from("/consumers") })
    }
}

// source line: 762
#[derive(Debug)]
pub struct DelegationTokenAuthZNode(ZNode);
impl Default for DelegationTokenAuthZNode {
    fn default() -> Self {
        Self(ZNode { path: String::from("/delegation_token") })
    }
}

// source line: 754
#[derive(Debug)]
pub struct BrokerSequenceIdZNode(ZNode);
impl BrokerSequenceIdZNode {
    pub fn build(brokers_znode: &BrokersZNode) -> Self {
        Self(ZNode { path: format!("{}/seqid", brokers_znode.0.path()) })
    }
}

// source line: 778
#[derive(Debug)]
pub struct DelegationTokensZNode(ZNode);
impl DelegationTokensZNode {
    pub fn build(delegation_token_auth: &DelegationTokenAuthZNode) -> Self {
        Self(ZNode { path: format!("{}/tokens", delegation_token_auth.0.path()) })
    }
}

impl Default for ZkData {
    fn default() -> Self {
        let config_types = ConfigType::default();
        let config_znode = ConfigZNode::default();
        let brokers_znode = BrokersZNode::default();
        let admin_znode = AdminZNode::default();
        let delegation_token_auth = DelegationTokenAuthZNode::default();
        let delegation_tokens = DelegationTokensZNode::build(&delegation_token_auth);
        let broker_ids = BrokerIdZNode::build(&brokers_znode);
        let topics = TopicsZNode::build(&brokers_znode);
        let config_entity_change_notification =
            ConfigEntityChangeNotificationZNode::build(&config_znode);
        let delete_topics = DeleteTopicsZNode::build(&admin_znode);
        let broker_sequence_id = BrokerSequenceIdZNode::build(&brokers_znode);
        let isr_change_notification = IsrChangeNotificationZNode::default();
        let producer_id_block = ZNode { path: String::from("unset") };
        let log_dir_event_notification = ZNode { path: String::from("unset") };
        let config_entity_type_user =
            ConfigEntityTypeUserZNode::build(&config_znode, &config_types);
        let config_entity_type_broker =
            ConfigEntityTypeBrokerZNode::build(&config_znode, &config_types);
        ZkData {
            config_entity_type_user,
            config_entity_type_broker,
            delegation_token_auth,
            delegation_tokens,
            admin: admin_znode,
            consumer_path: ConsumerPathZNode::default(),
            broker_ids: broker_ids.0,
            topics: topics.0,
            config_entity_change_notification,
            delete_topics,
            broker_sequence_id,
            isr_change_notification,
            producer_id_block,
            log_dir_event_notification,
            config_types,
        }
    }
}
