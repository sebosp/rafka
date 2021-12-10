//! From core/src/main/scala/kafka/api/ApiVersion.scala
//! Contains the inter-broker versions, the brokers decide to use this version as part of an
//! upgrade. The clients however can negotiate the version.
//! The version changes are done as part of broker upgrades so that they store messages compatible
//! with brokers that are pending upgrade.

use crate::common::record::record_version::RecordVersion;
use enum_iterator::IntoEnumIterator;
use itertools::Itertools;
use std::collections::HashMap;
use std::convert::TryFrom;

#[derive(Debug)]
pub struct ApiVersionDefinition {
    id: i32,
    version: String,
    short_version: String,
    sub_version: Option<String>,
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

impl KafkaApiVersion {
    // Keep the IDs in order of versions
    pub fn gen_kafka_0_8_0() -> ApiVersionDefinition {
        ApiVersionDefinition {
            id: 0,
            version: String::from("0.8.0"),
            short_version: String::from("0.8.0"),
            sub_version: None,
            record_version: RecordVersion::V0,
        }
    }

    pub fn gen_kafka_0_8_1() -> ApiVersionDefinition {
        ApiVersionDefinition {
            id: 1,
            version: String::from("0.8.1"),
            short_version: String::from("0.8.1"),
            sub_version: None,
            record_version: RecordVersion::V0,
        }
    }

    pub fn gen_kafka_0_8_2() -> ApiVersionDefinition {
        ApiVersionDefinition {
            id: 2,
            version: String::from("0.8.2"),
            short_version: String::from("0.8.2"),
            sub_version: None,
            record_version: RecordVersion::V0,
        }
    }

    fn gen_kafka_0_9_0() -> ApiVersionDefinition {
        ApiVersionDefinition {
            id: 3,
            version: String::from("0.9.0"),
            short_version: String::from("0.9.0"),
            sub_version: Some(String::from("")),
            record_version: RecordVersion::V0,
        }
    }

    pub fn gen_kafka_0_10_0_iv0() -> ApiVersionDefinition {
        ApiVersionDefinition {
            id: 4,
            version: String::from("0.10.0-IV0"),
            short_version: String::from("0.10.0"),
            sub_version: Some(String::from("IV0")),
            record_version: RecordVersion::V1,
        }
    }

    pub fn gen_kafka_0_10_0_iv1() -> ApiVersionDefinition {
        ApiVersionDefinition {
            id: 5,
            version: String::from("0.10.0-IV1"),
            short_version: String::from("0.10.0"),
            sub_version: Some(String::from("IV1")),
            record_version: RecordVersion::V1,
        }
    }

    pub fn gen_kafka_0_10_1_iv0() -> ApiVersionDefinition {
        ApiVersionDefinition {
            id: 6,
            version: String::from("0.10.1-IV0"),
            short_version: String::from("0.10.1"),
            sub_version: Some(String::from("IV0")),
            record_version: RecordVersion::V1,
        }
    }

    pub fn gen_kafka_0_10_1_iv1() -> ApiVersionDefinition {
        ApiVersionDefinition {
            id: 7,
            version: String::from("0.10.1-IV1"),
            short_version: String::from("0.10.1"),
            sub_version: Some(String::from("IV1")),
            record_version: RecordVersion::V1,
        }
    }

    pub fn gen_kafka_0_10_1_iv2() -> ApiVersionDefinition {
        ApiVersionDefinition {
            id: 8,
            version: String::from("0.10.1-IV2"),
            short_version: String::from("0.10.1"),
            sub_version: Some(String::from("IV2")),
            record_version: RecordVersion::V1,
        }
    }

    pub fn gen_kafka_0_10_2_iv0() -> ApiVersionDefinition {
        ApiVersionDefinition {
            id: 9,
            version: String::from("0.10.2-IV0"),
            short_version: String::from("0.10.2"),
            sub_version: Some(String::from("IV0")),
            record_version: RecordVersion::V1,
        }
    }

    pub fn gen_kafka_0_11_0_iv0() -> ApiVersionDefinition {
        ApiVersionDefinition {
            id: 10,
            version: String::from("0.11.0-IV0"),
            short_version: String::from("0.11.0"),
            sub_version: Some(String::from("IV0")),
            record_version: RecordVersion::V2,
        }
    }

    pub fn gen_kafka_0_11_0_iv1() -> ApiVersionDefinition {
        ApiVersionDefinition {
            id: 11,
            version: String::from("0.11.0-IV1"),
            short_version: String::from("0.11.0"),
            sub_version: Some(String::from("IV1")),
            record_version: RecordVersion::V2,
        }
    }

    pub fn gen_kafka_0_11_0_iv2() -> ApiVersionDefinition {
        ApiVersionDefinition {
            id: 12,
            version: String::from("0.11.0-IV2"),
            short_version: String::from("0.11.0"),
            sub_version: Some(String::from("IV2")),
            record_version: RecordVersion::V2,
        }
    }

    pub fn gen_kafka_1_0_iv0() -> ApiVersionDefinition {
        ApiVersionDefinition {
            id: 13,
            version: String::from("1.0-IV0"),
            short_version: String::from("1.0"),
            sub_version: Some(String::from("IV0")),
            record_version: RecordVersion::V2,
        }
    }

    pub fn gen_kafka_1_1_iv0() -> ApiVersionDefinition {
        ApiVersionDefinition {
            id: 14,
            version: String::from("1.1-IV0"),
            short_version: String::from("1.1"),
            sub_version: Some(String::from("IV0")),
            record_version: RecordVersion::V2,
        }
    }

    pub fn gen_kafka_2_0_iv0() -> ApiVersionDefinition {
        ApiVersionDefinition {
            id: 15,
            version: String::from("2.0-IV0"),
            short_version: String::from("2.0"),
            sub_version: Some(String::from("IV0")),
            record_version: RecordVersion::V2,
        }
    }

    pub fn gen_kafka_2_0_iv1() -> ApiVersionDefinition {
        ApiVersionDefinition {
            id: 16,
            version: String::from("2.0-IV1"),
            short_version: String::from("2.0"),
            sub_version: Some(String::from("IV1")),
            record_version: RecordVersion::V2,
        }
    }

    pub fn gen_kafka_2_1_iv0() -> ApiVersionDefinition {
        ApiVersionDefinition {
            id: 17,
            version: String::from("2.1-IV0"),
            short_version: String::from("2.1"),
            sub_version: Some(String::from("IV0")),
            record_version: RecordVersion::V2,
        }
    }

    pub fn gen_kafka_2_1_iv1() -> ApiVersionDefinition {
        ApiVersionDefinition {
            id: 18,
            version: String::from("2.1-IV1"),
            short_version: String::from("2.1"),
            sub_version: Some(String::from("IV1")),
            record_version: RecordVersion::V2,
        }
    }

    pub fn gen_kafka_2_1_iv2() -> ApiVersionDefinition {
        ApiVersionDefinition {
            id: 19,
            version: String::from("2.1-IV2"),
            short_version: String::from("2.1"),
            sub_version: Some(String::from("IV2")),
            record_version: RecordVersion::V2,
        }
    }

    pub fn gen_kafka_2_2_iv0() -> ApiVersionDefinition {
        ApiVersionDefinition {
            id: 20,
            version: String::from("2.2-IV0"),
            short_version: String::from("2.2"),
            sub_version: Some(String::from("IV0")),
            record_version: RecordVersion::V2,
        }
    }

    pub fn gen_kafka_2_2_iv1() -> ApiVersionDefinition {
        ApiVersionDefinition {
            id: 21,
            version: String::from("2.2-IV1"),
            short_version: String::from("2.2"),
            sub_version: Some(String::from("IV1")),
            record_version: RecordVersion::V2,
        }
    }

    pub fn gen_kafka_2_3_iv0() -> ApiVersionDefinition {
        ApiVersionDefinition {
            id: 22,
            version: String::from("2.3-IV0"),
            short_version: String::from("2.3"),
            sub_version: Some(String::from("IV0")),
            record_version: RecordVersion::V2,
        }
    }

    pub fn gen_kafka_2_3_iv1() -> ApiVersionDefinition {
        ApiVersionDefinition {
            id: 23,
            version: String::from("2.3-IV1"),
            short_version: String::from("2.3"),
            sub_version: Some(String::from("IV1")),
            record_version: RecordVersion::V2,
        }
    }

    pub fn gen_kafka_2_4_iv0() -> ApiVersionDefinition {
        ApiVersionDefinition {
            id: 24,
            version: String::from("2.4-IV0"),
            short_version: String::from("2.4"),
            sub_version: Some(String::from("IV0")),
            record_version: RecordVersion::V2,
        }
    }

    pub fn gen_kafka_2_4_iv1() -> ApiVersionDefinition {
        ApiVersionDefinition {
            id: 25,
            version: String::from("2.4-IV1"),
            short_version: String::from("2.4"),
            sub_version: Some(String::from("IV1")),
            record_version: RecordVersion::V2,
        }
    }

    pub fn gen_kafka_2_5_iv0() -> ApiVersionDefinition {
        ApiVersionDefinition {
            id: 26,
            version: String::from("2.5-IV0"),
            short_version: String::from("2.5"),
            sub_version: Some(String::from("IV0")),
            record_version: RecordVersion::V2,
        }
    }

    pub fn gen_kafka_2_6_iv0() -> ApiVersionDefinition {
        ApiVersionDefinition {
            id: 27,
            version: String::from("2.6-IV0"),
            short_version: String::from("2.6"),
            sub_version: Some(String::from("IV0")),
            record_version: RecordVersion::V2,
        }
    }

    pub fn gen_kafka_2_7_iv0() -> ApiVersionDefinition {
        ApiVersionDefinition {
            id: 28,
            version: String::from("2.7-IV0"),
            short_version: String::from("2.7"),
            sub_version: Some(String::from("IV0")),
            record_version: RecordVersion::V2,
        }
    }

    pub fn all_versions() -> Vec<ApiVersionDefinition> {
        KafkaApiVersion::into_enum_iter().map(|val| val.into()).collect()
    }
}

impl From<KafkaApiVersion> for ApiVersionDefinition {
    fn from(version: KafkaApiVersion) -> Self {
        match version {
            KafkaApiVersion::Kafka0_8_0 => KafkaApiVersion::gen_kafka_0_8_0(),
            KafkaApiVersion::Kafka0_8_1 => KafkaApiVersion::gen_kafka_0_8_1(),
            KafkaApiVersion::Kafka0_8_2 => KafkaApiVersion::gen_kafka_0_8_2(),
            KafkaApiVersion::Kafka0_9_0 => KafkaApiVersion::gen_kafka_0_9_0(),
            KafkaApiVersion::Kafka0_10_0Iv0 => KafkaApiVersion::gen_kafka_0_10_0_iv0(),
            KafkaApiVersion::Kafka0_10_0Iv1 => KafkaApiVersion::gen_kafka_0_10_0_iv1(),
            KafkaApiVersion::Kafka0_10_1Iv0 => KafkaApiVersion::gen_kafka_0_10_1_iv0(),
            KafkaApiVersion::Kafka0_10_1Iv1 => KafkaApiVersion::gen_kafka_0_10_1_iv1(),
            KafkaApiVersion::Kafka0_10_1Iv2 => KafkaApiVersion::gen_kafka_0_10_1_iv2(),
            KafkaApiVersion::Kafka0_10_2Iv0 => KafkaApiVersion::gen_kafka_0_10_2_iv0(),
            KafkaApiVersion::Kafka0_11_0Iv0 => KafkaApiVersion::gen_kafka_0_11_0_iv0(),
            KafkaApiVersion::Kafka0_11_0Iv1 => KafkaApiVersion::gen_kafka_0_11_0_iv1(),
            KafkaApiVersion::Kafka0_11_0Iv2 => KafkaApiVersion::gen_kafka_0_11_0_iv2(),
            KafkaApiVersion::Kafka1_0Iv0 => KafkaApiVersion::gen_kafka_1_0_iv0(),
            KafkaApiVersion::Kafka1_1Iv0 => KafkaApiVersion::gen_kafka_1_1_iv0(),
            KafkaApiVersion::Kafka2_0Iv0 => KafkaApiVersion::gen_kafka_2_0_iv0(),
            KafkaApiVersion::Kafka2_0Iv1 => KafkaApiVersion::gen_kafka_2_0_iv1(),
            KafkaApiVersion::Kafka2_1Iv0 => KafkaApiVersion::gen_kafka_2_1_iv0(),
            KafkaApiVersion::Kafka2_1Iv1 => KafkaApiVersion::gen_kafka_2_1_iv1(),
            KafkaApiVersion::Kafka2_1Iv2 => KafkaApiVersion::gen_kafka_2_1_iv2(),
            KafkaApiVersion::Kafka2_2Iv0 => KafkaApiVersion::gen_kafka_2_2_iv0(),
            KafkaApiVersion::Kafka2_2Iv1 => KafkaApiVersion::gen_kafka_2_2_iv1(),
            KafkaApiVersion::Kafka2_3Iv0 => KafkaApiVersion::gen_kafka_2_3_iv0(),
            KafkaApiVersion::Kafka2_3Iv1 => KafkaApiVersion::gen_kafka_2_3_iv1(),
            KafkaApiVersion::Kafka2_4Iv0 => KafkaApiVersion::gen_kafka_2_4_iv0(),
            KafkaApiVersion::Kafka2_4Iv1 => KafkaApiVersion::gen_kafka_2_4_iv1(),
            KafkaApiVersion::Kafka2_5Iv0 => KafkaApiVersion::gen_kafka_2_5_iv0(),
            KafkaApiVersion::Kafka2_6Iv0 => KafkaApiVersion::gen_kafka_2_6_iv0(),
            KafkaApiVersion::Kafka2_7Iv0 => KafkaApiVersion::gen_kafka_2_7_iv0(),
        }
    }
}

impl TryFrom<String> for ApiVersionDefinition {
    // TODO: Maybe create its error enum
    type Error = String;

    /// Tries to create an `ApiVersionDefinition` instance from an input string,  formats can be
    /// like: "0.8.0", "0.8.0.x", "0.10.0", "0.10.0-IV1").
    fn try_from(input: String) -> Result<Self, Self::Error> {
        let api_versions = ApiVersion::default();
        let version_segments: Vec<&str> = input.split('.').collect();
        let num_segments = if input.starts_with(&String::from("0.")) { 3 } else { 2 };
        let key = version_segments.iter().take(num_segments).join(".");
        match api_versions.version_map.get(&key) {
            Some(val) => Ok(*val.clone()),
            None => Err(format!("Version `{}` is not a valid version", input)),
        }
    }
}

pub struct ApiVersion {
    version_map: HashMap<String, ApiVersionDefinition>,
    all_versions: Vec<ApiVersionDefinition>,
}

impl Default for ApiVersion {
    fn default() -> Self {
        let all_versions = KafkaApiVersion::all_versions();
        let mut version_map = HashMap::new();
        for version in &all_versions {
            version_map.insert(version.version, *version.clone());
        }
        for (key, group) in &all_versions.into_iter().group_by(|val| val.short_version) {
            version_map
                .insert(key, *group.collect::<Vec<ApiVersionDefinition>>().last().unwrap().clone());
        }

        Self { version_map, all_versions }
    }
}

impl ApiVersion {
    pub fn latest_version() -> ApiVersionDefinition {
        *ApiVersion::default().all_versions.last().unwrap()
    }

    // Return the minimum `KafkaApiVersion` that supports `RecordVersion`.
    pub fn min_supported_for(record_version: RecordVersion) -> KafkaApiVersion {
        match record_version {
            RecordVersion::V0 => KafkaApiVersion::Kafka0_8_0,
            RecordVersion::V1 => KafkaApiVersion::Kafka0_10_0Iv0,
            RecordVersion::V2 => KafkaApiVersion::Kafka0_11_0Iv0,
        }
    }
}
