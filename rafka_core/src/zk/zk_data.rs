//! This file contains objects for encoding/decoding data stored in ZooKeeper nodes (znodes).
//! core/src/main/scala/kafka/zk/ZkData.scala

use crate::server::dynamic_config_manager::ConfigType;
use rafka_derive::{SubZNodeHandle, ZNodeHandle};
use zookeeper_async::Acl;
// NOTE: Maybe all of this could be moved into a hashmap or something?

/// `ZNode` contains a known path or parent path of a node that could be stored in ZK
#[derive(Debug, ZNodeHandle)]
pub struct ZNode {
    path: String,
}

impl Default for ZNode {
    fn default() -> Self {
        Self { path: String::from("unset") }
    }
}

pub trait ZNodeHandle {
    fn path(&self) -> &str;
    // fn decode(input: &str) -> Self;
    // fn encode(self) -> String;
}

/// `ZkData`  contains a set of known paths in zookeeper
#[derive(Debug)]
pub struct ZkData {
    /// old consumer path znode
    consumer_path: ConsumerPathZNode,
    config: ConfigZNode,
    config_types: ConfigType,
    topics: TopicsZNode,
    broker_ids: BrokerIdsZNode,
    delegation_token_auth: DelegationTokenAuthZNode,
    delegation_tokens: DelegationTokensZNode,
    config_entity_change_notification: ConfigEntityChangeNotificationZNode,
    delete_topics: DeleteTopicsZNode,
    broker_sequence_id: BrokerSequenceIdZNode,
    isr_change_notification: IsrChangeNotificationZNode,
    producer_id_block: ProducerIdBlockZNode,
    log_dir_event_notification: LogDirEventNotificationZNode,
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
        if self.consumer_path.path() != path && is_secure {
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
            &self.consumer_path.path(), // old consumer path
            &self.broker_ids.path(),
            &self.topics.path(),
            &self.config_entity_change_notification.path(),
            &self.delete_topics.path(),
            &self.broker_sequence_id.path(),
            &self.isr_change_notification.path(),
            &self.producer_id_block.path(),
            &self.log_dir_event_notification.path(),
        ];
        // ConfigType.all.map(ConfigEntityTypeZNode.path)
        // NOTE: This depends on config_znode, but it's not obvious here... Maybe it should be
        // refactored.
        for path in self.config_types.all() {
            paths.push(path);
        }
        paths
    }

    pub fn sensitive_root_paths(&self) -> Vec<&str> {
        vec![&self.config_types.user, &self.config_types.broker, &self.delegation_tokens.path()]
    }
}

// source line: 70
#[derive(Debug, SubZNodeHandle)]
pub struct ConfigZNode(ZNode);
impl ConfigZNode {
    pub fn build() -> Self {
        Self(ZNode { path: String::from("/config") })
    }
}

// source line: 74
#[derive(Debug, SubZNodeHandle)]
pub struct BrokersZNode(ZNode);
impl BrokersZNode {
    pub fn build() -> Self {
        Self(ZNode { path: String::from("/brokers") })
    }
}

// source line: 78
#[derive(Debug, SubZNodeHandle)]
pub struct BrokerIdsZNode(ZNode);
impl BrokerIdsZNode {
    pub fn build(brokers_znode: &BrokersZNode) -> Self {
        Self(ZNode { path: format!("{}/ids", brokers_znode.path()) })
    }
}

// source line: 277 this may defeat the purpose of the below User/Broker structs that I think are
// also present in DynamicConfigManager...
#[derive(Debug, SubZNodeHandle)]
pub struct TopicsZNode(ZNode);
impl TopicsZNode {
    pub fn build(brokers_znode: &BrokersZNode) -> Self {
        Self(ZNode { path: format!("{}/topics", brokers_znode.path()) })
    }
}

// source line: 361
#[derive(Debug, SubZNodeHandle)]
pub struct ConfigEntityTypeZNode(ZNode);
impl ConfigEntityTypeZNode {
    pub fn build(config_znode: &ConfigZNode, entity_type: &str) -> Self {
        Self(ZNode { path: format!("{}/{}", config_znode.path(), entity_type) })
    }
}

// source line: 382
#[derive(Debug, SubZNodeHandle)]
pub struct ConfigEntityChangeNotificationZNode(ZNode);
impl ConfigEntityChangeNotificationZNode {
    pub fn build(config_znode: &ConfigZNode) -> Self {
        Self(ZNode { path: format!("{}/changes", config_znode.path()) })
    }
}

// source line: 393
#[derive(Debug, SubZNodeHandle)]
pub struct IsrChangeNotificationZNode(ZNode);
impl IsrChangeNotificationZNode {
    pub fn build() -> Self {
        Self(ZNode { path: String::from("/isr_change_notification") })
    }
}

// source line: 419
#[derive(Debug, SubZNodeHandle)]
pub struct LogDirEventNotificationZNode(ZNode);
impl LogDirEventNotificationZNode {
    pub fn build() -> Self {
        Self(ZNode { path: String::from("/log_dir_event_notification") })
    }
}

// source line: 436
#[derive(Debug, SubZNodeHandle)]
pub struct AdminZNode(ZNode);
impl AdminZNode {
    pub fn build() -> Self {
        Self(ZNode { path: String::from("/admin") })
    }
}

// source line: 440
#[derive(Debug, SubZNodeHandle)]
pub struct DeleteTopicsZNode(ZNode);
impl DeleteTopicsZNode {
    pub fn build(admin_znode: &AdminZNode) -> Self {
        Self(ZNode { path: format!("{}/delete_topics", admin_znode.path()) })
    }
}

// old consumer path znode
// source line: 511
#[derive(Debug, SubZNodeHandle)]
pub struct ConsumerPathZNode(ZNode);
impl ConsumerPathZNode {
    pub fn build() -> Self {
        Self(ZNode { path: String::from("/consumers") })
    }
}

// source line: 754
#[derive(Debug, SubZNodeHandle)]
pub struct BrokerSequenceIdZNode(ZNode);
impl BrokerSequenceIdZNode {
    pub fn build(brokers_znode: &BrokersZNode) -> Self {
        Self(ZNode { path: format!("{}/seqid", brokers_znode.path()) })
    }
}

// source line: 758
#[derive(Debug, SubZNodeHandle)]
pub struct ProducerIdBlockZNode(ZNode);
impl ProducerIdBlockZNode {
    pub fn build() -> Self {
        Self(ZNode { path: String::from("/latest_producer_id_block") })
    }
}

// source line: 762
#[derive(Debug, SubZNodeHandle)]
pub struct DelegationTokenAuthZNode(ZNode);
impl DelegationTokenAuthZNode {
    pub fn build() -> Self {
        Self(ZNode { path: String::from("/delegation_token") })
    }
}

// source line: 778
#[derive(Debug, SubZNodeHandle)]
pub struct DelegationTokensZNode(ZNode);
impl DelegationTokensZNode {
    pub fn build(delegation_token_auth: &DelegationTokenAuthZNode) -> Self {
        Self(ZNode { path: format!("{}/tokens", delegation_token_auth.path()) })
    }
}

/// Represents the status of the FeatureZNode.
///
/// Enabled  -> This status means the feature versioning system (KIP-584) is enabled, and, the
///             finalized features stored in the FeatureZNode are active. This status is written by
///             the controller to the FeatureZNode only when the broker IBP config is greater than
///             or equal to KAFKA_2_7_IV0.
///
/// Disabled -> This status means the feature versioning system (KIP-584) is disabled, and, the
///             the finalized features stored in the FeatureZNode is not relevant. This status is
///             written by the controller to the FeatureZNode only when the broker IBP config
///             is less than KAFKA_2_7_IV0.
///
/// The purpose behind the FeatureZNodeStatus is that it helps differentiates between the following
/// cases:
///
/// 1. New cluster bootstrap:
///    For a new Kafka cluster (i.e. it is deployed first time), we would like to start the cluster
///    with all the possible supported features finalized immediately. The new cluster will almost
///    never be started with an old IBP config that’s less than KAFKA_2_7_IV0. In such a case, the
///    controller will start up and notice that the FeatureZNode is absent in the new cluster.
///    To handle the requirement, the controller will create a FeatureZNode (with enabled status)
///    containing the entire list of supported features as its finalized features.
///
/// 2. Cluster upgrade:
///    Imagine there is an existing Kafka cluster with IBP config less than KAFKA_2_7_IV0, but
///    the Broker binary has been upgraded to a state where it supports the feature versioning
///    system (KIP-584). This means the user is upgrading from an earlier version of the Broker
///    binary. In this case, we want to start with no finalized features and allow the user to
/// enable    them whenever they are ready i.e. in the future whenever the user sets IBP config
///    to be greater than or equal to KAFKA_2_7_IV0. The reason is that enabling all the possible
///    features immediately after an upgrade could be harmful to the cluster.
///    In such a case:
///      - Before the Broker upgrade (i.e. IBP config set to less than KAFKA_2_7_IV0), the
///        controller will start up and check if the FeatureZNode is absent. If true, then it will
///        react by creating a FeatureZNode with disabled status and empty features.
///      - After the Broker upgrade (i.e. IBP config set to greater than or equal to KAFKA_2_7_IV0),
///        when the controller starts up it will check if the FeatureZNode exists and whether it is
///        disabled. In such a case, it won’t upgrade all features immediately. Instead it will just
///        switch the FeatureZNode status to enabled status. This lets the user finalize the
///        features later.
///
/// 3. Cluster downgrade:
///    Imagine that a Kafka cluster exists already and the IBP config is greater than or equal to
///    KAFKA_2_7_IV0. Then, the user decided to downgrade the cluster by setting IBP config to a
///    value less than KAFKA_2_7_IV0. This means the user is also disabling the feature versioning
///    system (KIP-584). In this case, when the controller starts up with the lower IBP config, it
///    will switch the FeatureZNode status to disabled with empty features.

// source line: 837
#[derive(Debug)]
pub enum FeatureZNodeStatus {
    Disabled,
    Enabled,
}

impl FeatureZNodeStatus {
    pub fn withNameOpt(value: i32) -> Option<Self> {
        match value {
            1 => Some(FeatureZNodeStatus::Disabled),
            2 => Some(FeatureZNodeStatus::Enabled),
            _ => None,
        }
    }
}

// source line: 854
#[derive(Debug)]
pub enum FeatureZNodeVersion {
    V1(u32),
}

impl FeatureZNodeVersion {
    // V1 contains 'version', 'status' and 'features' keys.
    pub fn v1() -> Self {
        FeatureZNodeVersion::V1(1)
    }
}

// TODO: for now just an empty struct, later should be implemented from
// clients/src/main/java/org/apache/kafka/common/feature/Features.java
#[derive(Debug)]
pub struct Features;

// source line: 854
/// `FeatureZNode`  Represents the contents of the ZK node containing finalized feature information.
#[derive(Debug)]
pub struct FeatureZNode {
    path: ZNode,
    version_key: String,
    status_key: String,
    features_key: String,
    current_version: FeatureZNodeVersion,
    status: FeatureZNodeStatus,
    features: Vec<Features>,
}

impl FeatureZNode {
    pub fn build(status: FeatureZNodeStatus, features: Vec<Features>) -> Self {
        Self {
            path: ZNode { path: String::from("/feature") },
            version_key: String::from("version"),
            status_key: String::from("status"),
            features_key: String::from("features"),
            current_version: FeatureZNodeVersion::v1(),
            status,
            features,
        }
    }
}

impl Default for ZkData {
    fn default() -> Self {
        let config = ConfigZNode::build();
        let config_types = ConfigType::build(&config);
        let brokers_znode = BrokersZNode::build();
        let admin_znode = AdminZNode::build();
        let delegation_token_auth = DelegationTokenAuthZNode::build();
        let delegation_tokens = DelegationTokensZNode::build(&delegation_token_auth);
        let broker_ids = BrokerIdsZNode::build(&brokers_znode);
        let topics = TopicsZNode::build(&brokers_znode);
        let config_entity_change_notification = ConfigEntityChangeNotificationZNode::build(&config);
        let delete_topics = DeleteTopicsZNode::build(&admin_znode);
        let broker_sequence_id = BrokerSequenceIdZNode::build(&brokers_znode);
        let isr_change_notification = IsrChangeNotificationZNode::build();
        let producer_id_block = ProducerIdBlockZNode::build();
        let log_dir_event_notification = LogDirEventNotificationZNode::build();
        ZkData {
            delegation_token_auth,
            delegation_tokens,
            admin: admin_znode,
            consumer_path: ConsumerPathZNode::build(),
            broker_ids,
            topics,
            config_entity_change_notification,
            delete_topics,
            broker_sequence_id,
            isr_change_notification,
            producer_id_block,
            log_dir_event_notification,
            config_types,
            config,
        }
    }
}
