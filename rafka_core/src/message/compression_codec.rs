//! From core/src/main/scala/kafka/message/CompressionCodec.scala

use crate::server::kafka_config::KafkaConfigError;
use core::fmt;
use std::str::FromStr;

pub const GZIP_COMPRESSION_NAME: &str = "gzip";
pub const SNAPPY_COMPRESSION_NAME: &str = "snappy";
pub const LZ4_COMPRESSION_NAME: &str = "lz4";
pub const ZSTD_COMPRESSION_NAME: &str = "zstd";
pub const NONE_COMPRESSION_NAME: &str = "none";
pub const UNCOMPRESSED_NAME: &str = "uncompressed";
pub const PRODUCER_COMPRESSION_NAME: &str = "producer";

pub const GZIP_COMPRESSION_CODEC: BrokerCompressionCodec =
    BrokerCompressionCodec::gen_gzip_compression_codec();
pub const SNAPPY_COMPRESSION_CODEC: BrokerCompressionCodec =
    BrokerCompressionCodec::gen_snappy_compression_codec();
pub const LZ4_COMPRESSION_CODEC: BrokerCompressionCodec =
    BrokerCompressionCodec::gen_lz4_compression_codec();
pub const ZSTD_COMPRESSION_CODEC: BrokerCompressionCodec =
    BrokerCompressionCodec::gen_zstd_compression_codec();
pub const NONE_COMPRESSION_CODEC: BrokerCompressionCodec =
    BrokerCompressionCodec::gen_none_compression_codec();
pub const UNCOMPRESSED_CODEC: BrokerCompressionCodec =
    BrokerCompressionCodec::gen_uncompressed_codec();
pub const PRODUCER_COMPRESSION_CODEC: BrokerCompressionCodec =
    BrokerCompressionCodec::gen_producer_compression_codec();

#[derive(Debug, Clone, PartialEq)]
pub struct BrokerCompressionCodec {
    pub codec: Option<i32>,
    pub name: &'static str,
}

impl FromStr for BrokerCompressionCodec {
    type Err = KafkaConfigError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            GZIP_COMPRESSION_NAME => Ok(Self::gen_gzip_compression_codec()),
            SNAPPY_COMPRESSION_NAME => Ok(Self::gen_snappy_compression_codec()),
            LZ4_COMPRESSION_NAME => Ok(Self::gen_lz4_compression_codec()),
            ZSTD_COMPRESSION_NAME => Ok(Self::gen_zstd_compression_codec()),
            NONE_COMPRESSION_NAME => Ok(Self::gen_none_compression_codec()),
            UNCOMPRESSED_COMPRESSION_NAME => Ok(Self::gen_uncompressed_codec()),
            PRODUCER_COMPRESSION_NAME => Ok(Self::gen_producer_compression_codec()),
            _ => Err(KafkaConfigError::InvalidBrokerCompressionCodec(input.to_string())),
        }
    }
}

impl fmt::Display for BrokerCompressionCodec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

// DefaultCompressionCodec
impl Default for BrokerCompressionCodec {
    fn default() -> Self {
        GZIP_COMPRESSION_CODEC
    }
}

impl BrokerCompressionCodec {
    pub fn broker_compression_options() -> Vec<&'static str> {
        vec![
            GZIP_COMPRESSION_CODEC.name,
            SNAPPY_COMPRESSION_CODEC.name,
            LZ4_COMPRESSION_CODEC.name,
            ZSTD_COMPRESSION_CODEC.name,
            NONE_COMPRESSION_CODEC.name,
            UNCOMPRESSED_CODEC.name,
            PRODUCER_COMPRESSION_CODEC.name,
        ]
    }

    pub fn gen_gzip_compression_codec() -> Self {
        Self { codec: Some(1), name: GZIP_COMPRESSION_NAME }
    }

    pub fn gen_snappy_compression_codec() -> Self {
        Self { codec: Some(2), name: SNAPPY_COMPRESSION_NAME }
    }

    pub fn gen_lz4_compression_codec() -> Self {
        Self { codec: Some(3), name: LZ4_COMPRESSION_NAME }
    }

    pub fn gen_zstd_compression_codec() -> Self {
        Self { codec: Some(4), name: ZSTD_COMPRESSION_NAME }
    }

    pub fn gen_none_compression_codec() -> Self {
        Self { codec: Some(0), name: NONE_COMPRESSION_NAME }
    }

    pub fn gen_uncompressed_codec() -> Self {
        Self { codec: None, name: UNCOMPRESSED_NAME }
    }

    pub fn gen_producer_compression_codec() -> Self {
        Self { codec: None, name: PRODUCER_COMPRESSION_NAME }
    }
}
