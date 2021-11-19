//! From core/src/main/scala/kafka/message/CompressionCodec.scala
use core::fmt;

#[derive(Debug)]
pub struct CompressionCodec {
    pub codec: Option<i32>,
    pub name: String,
}

#[derive(Debug)]
pub enum BrokerCompressionCodec {
    NoCompression(CompressionCodec),
    Uncompressed(CompressionCodec),
    ZStdCompression(CompressionCodec),
    LZ4Compression(CompressionCodec),
    SnappyCompression(CompressionCodec),
    GZIPCompressio(CompressionCodec),
    ProducerCompression(CompressionCodec),
}

impl fmt::Display for BrokerCompressionCodec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}
// DefaultCompressionCodec
impl Default for BrokerCompressionCodec {
    fn default() -> Self {
        Self::gen_gzip_compression_codec()
    }
}

impl BrokerCompressionCodec {
    fn codec(&self) -> Option<i32> {
        match self {
            Self::NoCompression(val) => val.codec,
            Self::Uncompressed(val) => val.codec,
            Self::ZStdCompression(val) => val.codec,
            Self::LZ4Compression(val) => val.codec,
            Self::SnappyCompression(val) => val.codec,
            Self::GZIPCompression(val) => val.codec,
            Self::ProducerCompression(val) => val.codec,
        }
    }

    fn name(&self) -> &str {
        match self {
            Self::NoCompression(_) => &self.name,
            Self::Uncompressed(_) => &self.name,
            Self::ZStdCompression(_) => &self.name,
            Self::LZ4Compression(_) => &self.name,
            Self::SnappyCompression(_) => &self.name,
            Self::GZIPCompression(_) => &self.name,
            Self::ProducerCompression(_) => &self.name,
        }
    }
}

impl BrokerCompressionCodec {
    pub fn gen_gzip_compression_codec() -> Self {
        Self::GZIPCompression(CompressionCodec { codec: Some(1), name: String::from("gzip") })
    }

    pub fn gen_snappy_compression_codec() -> Self {
        Self::SnappyCompression(CompressionCodec { codec: Some(2), name: String::from("snappy") })
    }

    pub fn gen_lz4_compression_codec() -> Self {
        Self::LZ4Compression(CompressionCodec { codec: Some(3), name: String::from("lz4") })
    }

    pub fn gen_zstd_compression_codec() -> Self {
        Self::ZStdCompression(CompressionCodec { codec: Some(4), name: String::from("zstd") })
    }

    pub fn gen_no_compression_codec() -> Self {
        Self::NoCompression(CompressionCodec { codec: Some(0), name: String::from("none") })
    }

    pub fn gen_uncompressed_codec() -> Self {
        Self::Uncompressed(CompressionCodec { codec: None, name: String::from("uncompressed") })
    }

    pub fn gen_producer_compression_codec() -> Self {
        Self::ProducerCompression(CompressionCodec { codec: None, name: String::from("producer") })
    }
}
