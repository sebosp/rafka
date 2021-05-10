//! This file contains objects for encoding/decoding data stored in ZooKeeper nodes (znodes).
//! core/src/main/scala/kafka/zk/ZkData.scala

use crate::common::feature::features::FeaturesError;
use crate::common::feature::features::{Features, VersionRangeType};
use crate::server::dynamic_config_manager::ConfigType;
use rafka_derive::{SubZNodeHandle, ZNodeHandle};
use serde_json::json;
use std::collections::HashMap;
use std::convert::TryFrom;
use tracing::{debug, error};
use zookeeper_async::Acl;
// NOTE: Maybe all of this could be moved into a hashmap or something?

/// `ZNode` contains a known path or parent path of a node that could be stored in ZK
#[derive(Debug, ZNodeHandle)]
pub struct ZNode {
    path: String,
}

#[derive(thiserror::Error, Debug)]
pub enum ZNodeDecodeError {
    #[error("Serde {0:?}")]
    Serde(#[from] serde_json::Error),
    #[error("TryFromInt {0:?}")]
    TryFromInt(#[from] std::num::TryFromIntError),
    #[error("KeyNotFound {0} in {1}")]
    KeyNotFound(String, String),
    #[error("Unsupported version: {0} of feature information: {1:?})")]
    UnsupportedVersion(i32, String),
    #[error("Unable to transform data to String: {0}")]
    Utf8Error(std::string::FromUtf8Error),
    #[error("Features {0:?}")]
    Features(#[from] FeaturesError),
    #[error("MalformedStatus {0} found in feature information")]
    MalformedStatus(i32, String),
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
    cluster_id: ClusterIdZNode,
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

// source line: 520
#[derive(Debug)]
pub enum ZkVersion {
    MatchAnyVersion = -1, /* if used in a conditional set, matches any version (the value
                           * should match ZooKeeper codebase) */
    UnknownVersion = -2, /* Version returned from get if node does not exist (internal
                          * constant for Kafka codebase, unused value in ZK) */
}

// source line: 526
//#[derive(Debug, PartialEq)]
// pub enum ZkStat {
//    NoStat, /* NOTE: Originally this is org.apache.zookeeper.data.Stat constructor:
//             * val NoStat = new Stat() */
//}

// source line: 736
#[derive(Debug, SubZNodeHandle)]
pub struct ClusterZNode(ZNode);
impl ClusterZNode {
    pub fn build() -> Self {
        Self(ZNode { path: String::from("/cluster") })
    }
}

// source line: 740
#[derive(Debug, SubZNodeHandle)]
pub struct ClusterIdZNode(ZNode);
impl ClusterIdZNode {
    pub fn build(cluster_znode: &ClusterZNode) -> Self {
        Self(ZNode { path: format!("{}/id", cluster_znode.path()) })
    }

    pub fn to_json(&self, id: String) -> Vec<u8> {
        json!({
            "version": "1",
            "id":  id
        })
    }

    pub fn from_json(cluster_id_json: &Vec<u8>) -> Result<String, serde_json::Error> {
        let res: serde_json::Value = serde_json::from_slice(cluster_id_json)?;
        Ok(res["id"].to_string())
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
#[derive(Debug, PartialEq)]
pub enum FeatureZNodeStatus {
    Disabled,
    Enabled,
}

impl FeatureZNodeStatus {
    pub fn with_name_opt(value: i32) -> Option<Self> {
        match value {
            0 => Some(FeatureZNodeStatus::Disabled), // RAFKA TODO: verify this is 0-based enum
            1 => Some(FeatureZNodeStatus::Enabled),
            _ => None,
        }
    }
}

// source line: 854
#[derive(Debug)]
pub enum FeatureZNodeVersion {
    V1 = 1,
}

// source line: 854
/// A helper function that builds the FeatureZNode
#[derive(Debug)]
pub struct FeatureZNodeBuilder {
    version_key: String,
    status_key: String,
    features_key: String,
}

impl Default for FeatureZNodeBuilder {
    fn default() -> Self {
        Self {
            version_key: String::from("version"),
            status_key: String::from("status"),
            features_key: String::from("features"),
        }
    }
}

impl FeatureZNodeBuilder {
    /// Attempts to create a FeatureZNode from an input Vec<u8> read from ???
    /// See the tests for the format of the data.
    pub fn build(input: Vec<u8>) -> Result<FeatureZNode, ZNodeDecodeError> {
        let data = match String::from_utf8(input) {
            Ok(val) => val,
            Err(err) => {
                error!("Unable to transform data string: {}", err);
                return Err(ZNodeDecodeError::Utf8Error(err));
            },
        };
        let decoded_data: serde_json::Value = match serde_json::from_str(&data) {
            Err(err) => {
                // Instead of using the `?` operator we do the match here to preserve the previous
                // error message. Granted, the error message does some convertion that should be
                // emulated, right after the `:` below: s"${new String(jsonBytes, UTF_8)}", e)
                error!("Failed to parse feature information: {:?} ", err);
                return Err(ZNodeDecodeError::Serde(err));
            },
            Ok(val) => val,
        };
        let builder = Self::default();
        let version = match &decoded_data[&builder.version_key].as_u64() {
            Some(val) => i32::try_from(*val)?,
            None => {
                return Err(ZNodeDecodeError::KeyNotFound(
                    builder.version_key.to_string(),
                    decoded_data.to_string(),
                ))
            },
        };
        if version < FeatureZNodeVersion::V1 as i32 {
            return Err(ZNodeDecodeError::UnsupportedVersion(version, data));
        }

        let status_int = match &decoded_data[&builder.status_key].as_u64() {
            Some(val) => i32::try_from(*val)?,
            None => {
                return Err(ZNodeDecodeError::KeyNotFound(
                    builder.version_key.to_string(),
                    decoded_data.to_string(),
                ))
            },
        };
        let status = match FeatureZNodeStatus::with_name_opt(status_int) {
            Some(val) => val,
            None => {
                return Err(ZNodeDecodeError::MalformedStatus(status_int, decoded_data.to_string()))
            },
        };
        Ok(FeatureZNode {
            path: String::from("/feature"),
            current_version: FeatureZNodeVersion::V1,
            status,
            features: Features::parse_finalized_features_json_value(
                &decoded_data,
                &builder.features_key,
            )?,
        })
    }
}
// source line: 854
/// `FeatureZNode`  Represents the contents of the ZK node containing finalized feature information.
#[derive(Debug, ZNodeHandle)]
pub struct FeatureZNode {
    path: String,
    current_version: FeatureZNodeVersion,
    pub status: FeatureZNodeStatus,
    pub features: Features,
}

impl FeatureZNode {
    pub fn decode(data: Vec<u8>) -> Result<Self, ZNodeDecodeError> {
        FeatureZNodeBuilder::build(data)
    }

    pub fn default_path() -> String {
        String::from("/feature")
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
        let cluster_znode = ClusterZNode::build();
        let cluster_id = ClusterIdZNode::build(&cluster_znode);
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
            cluster_id,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::common::feature::finalized_version_range::FinalizedVersionRange;
    use tracing::info;
    // From core/src/test/scala/kafka/zk/FeatureZNodeTest.scala
    #[test_env_log::test]
    fn feature_znode_builder_decodes() {
        let valid_features = serde_json::json!({
            "version":1,
            "status":1,
            "features": r#"{"feature1":{ "min_version_level": 1, "max_version_level": 2}, "feature2": {"min_version_level": 2, "max_version_level": 4}}"#
        });
        info!("valid_features.version: {:?}", valid_features["version"]);
        info!("features_json_str: {}", valid_features.to_string());
        let build_res = FeatureZNode::decode(valid_features.to_string().as_bytes().to_vec());
        let built_znode = build_res.unwrap();
        assert_eq!(built_znode.status, FeatureZNodeStatus::Enabled);
        let mut expected_features: HashMap<String, FinalizedVersionRange> = HashMap::new();
        expected_features
            .insert(String::from("feature1"), FinalizedVersionRange::new(1, 2).unwrap());
        expected_features
            .insert(String::from("feature2"), FinalizedVersionRange::new(2, 4).unwrap());
        assert_eq!(
            Features { features: VersionRangeType::Finalized(expected_features) },
            built_znode.features
        );
        let empty_features = serde_json::json!({
            "version":1,
            "status":1,
            "features": "{}",
        });
        let empty_features_build_res =
            FeatureZNode::decode(empty_features.to_string().as_bytes().to_vec());
        info!("Empty Feature ZNode: {:?}", empty_features_build_res);
        let empty_features_built_znode = empty_features_build_res.unwrap();
        let expected_features: HashMap<String, FinalizedVersionRange> = HashMap::new();
        assert_eq!(
            Features { features: VersionRangeType::Finalized(expected_features) },
            empty_features_built_znode.features
        );
    }
}
