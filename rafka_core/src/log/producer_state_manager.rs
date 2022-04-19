//! From core/src/main/scala/kafka/log/ProducerStateManager.scala

use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
};

use crate::common::topic_partition::TopicPartition;

// RAFKA TODO:
#[derive(Debug)]
pub struct ProducerStateEntry;

// RAFKA TODO:
#[derive(Debug)]
pub struct TxnMetadata;

#[derive(Debug)]
pub struct ProducerStateManager {
    topic_partition: TopicPartition,
    log_dir: PathBuf,
    max_producer_id_expiration_ms: i64,
    log_ident: String,
    producers: HashMap<i64, ProducerStateEntry>,
    last_map_offset: i64,
    last_snap_offset: i64,
    // The ongoing txs ordere by first transaction offset
    ongoing_txns: BTreeMap<i64, TxnMetadata>,

    // completed txns whose markers are at offsets above the high watermark
    unreplicated_txns: BTreeMap<i64, TxnMetadata>,
}

impl ProducerStateManager {
    pub fn new(
        topic_partition: TopicPartition,
        log_dir: PathBuf,
        max_producer_id_expiration_ms: Option<i64>,
    ) -> Self {
        Self {
            topic_partition,
            log_dir,
            max_producer_id_expiration_ms: max_producer_id_expiration_ms.unwrap_or(60 * 60 * 1000),
            log_ident: format!(
                "[ProducerStateManager partition=${}] ",
                topic_partition.to_string()
            ),
            producers: HashMap::new(),
            last_map_offset: 0,
            last_snap_offset: 0,
            ongoing_txns: BTreeMap::new(),
            unreplicated_txns: BTreeMap::new(),
        }
    }
}
