//! Creates an executor and schedules tasks on that executor
//! core/src/main/scala/kafka/utils/KafkaScheduler.scala
//! RAFKA Specific:
//! - The tokio executor and the interval tasks will be used instead of this
//! KafkaScheduler may simply provide helpers for cancelling tasks that have been scheduled...
//! Since Tokio does all the tasks anyway
use std::time::Duration;

#[derive(Debug)]
pub struct KafkaScheludedTask;

/// RAFKA TODO: Maybe :Sync?
#[derive(Debug, Clone)]
pub struct KafkaScheduler {
    /// The name of this task
    name: String,
    /// The amount of time to wait before the first execution
    delay: Duration,
    /// The period with which to execute the task. If < 0 the task will execute only once.
    period: i32,
}
// RAFKA Unimplemented:
// unit The unit for the preceding times, the callers of this function will have to use the
// Duration unit when calling the module

impl Default for KafkaScheduler {
    fn default() -> Self {
        Self {
            name: String::from("Kafka-Scheduler-unnamed"),
            delay: Duration::from_millis(0),
            period: -1i32,
        }
    }
}
