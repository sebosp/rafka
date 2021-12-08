//! From core/src/main/scala/kafka/api/ApiVersion.scala
//! Contains the inter-broker versions, the brokers decide to use this version as part of an
//! upgrade. The clients however can negotiate the version.
//! The version changes are done as part of broker upgrades so that they store messages compatible
//! with brokers that are pending upgrade.

use enum_iterator::IntoEnumIterator;
use std::collections::HashMap;

#[derive(Debug)]
pub struct ApiVersionDefinition {
    id: i32,
    version: String,
    short_version: String,
    record_version: RecordVersion,
}

#[derive(Debug, IntoEnumIterator)]
pub enum KafkaApiVersion {
    Kafka0_8_0,
    Kafka0_8_1,
    Kafka0_8_2,
    Kafka0_9_0,
    // 0.10.0-IV0 is introduced for KIP-31/32 which changes the message format.
    Kafka0_10_0Iv0,
    // 0.10.0-IV1 is introduced for KIP-36(rack awareness) and KIP-43(SASL handshake).
    Kafka0_10_0Iv1,
    // introduced for JoinGroup protocol change in KIP-62
    Kafka0_10_1Iv0,
    // 0.10.1-IV1 is introduced for KIP-74(fetch response size limit).
    Kafka0_10_1Iv1,
    // introduced ListOffsetRequest v1 in KIP-79
    Kafka0_10_1Iv2,
    // introduced UpdateMetadataRequest v3 in KIP-103
    Kafka0_10_2Iv0,
    // KIP-98 (idempotent and transactional producer support)
    Kafka0_11_0Iv0,
    // introduced DeleteRecordsRequest v0 and FetchRequest v4 in KIP-107
    Kafka0_11_0Iv1,
    // Introduced leader epoch fetches to the replica fetcher via KIP-101
    Kafka0_11_0Iv2,
    // Introduced LeaderAndIsrRequest V1, UpdateMetadataRequest V4 and FetchRequest V6 via KIP-112
    Kafka1_0Iv0,
    // Introduced DeleteGroupsRequest V0 via KIP-229, plus KIP-227 incremental fetch requests,
    // and KafkaStorageException for fetch requests.
    Kafka1_1Iv0,
    // Introduced OffsetsForLeaderEpochRequest V1 via KIP-279 (Fix log divergence between leader
    // and follower after fast leader fail over)
    Kafka2_0Iv0,
    // Several request versions were bumped due to KIP-219 (Improve quota communication)
    Kafka2_0Iv1,
    // Introduced new schemas for group offset (v2) and group metadata (v2) (KIP-211)
    Kafka2_1Iv0,
    // New Fetch, OffsetsForLeaderEpoch, and ListOffsets schemas (KIP-320)
    Kafka2_1Iv1,
    // Support ZStandard Compression Codec (KIP-110)
    Kafka2_1Iv2,
    // Introduced broker generation (KIP-380), and
    // LeaderAdnIsrRequest V2, UpdateMetadataRequest V5, StopReplicaRequest V1
    Kafka2_2Iv0,
    // New error code for ListOffsets when a new leader is lagging behind former HW (KIP-207)
    Kafka2_2Iv1,
    // Introduced static membership.
    Kafka2_3Iv0,
    // Add rack_id to FetchRequest, preferred_read_replica to FetchResponse, and replica_id to
    // OffsetsForLeaderRequest
    Kafka2_3Iv1,
    // Add adding_replicas and removing_replicas fields to LeaderAndIsrRequest
    Kafka2_4Iv0,
    // Flexible version support in inter-broker APIs
    Kafka2_4Iv1,
    // No new APIs, equivalent to 2.4-IV1
    Kafka2_5Iv0,
    // Introduced StopReplicaRequest V3 containing the leader epoch for each partition (KIP-570)
    Kafka2_6Iv0,
    // Introduced feature versioning support (KIP-584)
    Kafka2_7Iv0,
}

pub struct ApiVersion {
    versions: HashMap<String, KafkaApiVersion>,
}

impl Default for ApiVersion {
    fn default() -> Self {
        let res = HashMap::new();

        Self { versions: res }
    }
}

impl ApiVersion {
    pub fn all_versions(&self) -> Vec<&KafkaApiVersion> {
        self.versions.iter().map(|(key, val)| val).collect()
    }
}
