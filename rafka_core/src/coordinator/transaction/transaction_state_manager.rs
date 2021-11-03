//! From core/src/main/scala/kafka/coordinator/transaction/TransactionStateManager.scala
//! TODO:
//! Could remove the default_ prefix to make the Default trait make more sense?

#[derive(Debug)]
pub struct TransactionStateManager {
    default_transactional_id_expiration_ms: i64,
}

impl Default for TransactionStateManager {
    fn default() -> Self {
        Self {
            // TimeUnit.DAYS.toMillis(7).toInt
            default_transactional_id_expiration_ms: 24 * 60 * 60 * 1000 * 7,
        }
    }
}
