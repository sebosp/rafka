//! Creates an executor and schedules tasks on that executor
//! core/src/main/scala/kafka/utils/KafkaScheduler.scala
//! RAFKA Specific:
//! - The tokio executor and the interval tasks will be used instead of this
use futures::sync::mpsc;
use std::time::{Duration, Instant};

pub struct KafkaScheduler<T>
where
    T: Sync,
{
    /// The name of this task
    name: String,
    /// The amount of time to wait before the first execution
    delay: Duration,
    /// The period with which to execute the task. If < 0 the task will execute only once.
    period: i32,
    /// An mpsc sender reference to report the completion of task to.
    tx: Option<futures::sync::mpsc::Sender<T>>,
}
// RAFKA Unimplemented:
// unit The unit for the preceding times, the callers of this function will have to use the
// Duration unit when calling the module

impl<T> Default for KafkaScheduler<T>
where
    T: Sync,
{
    fn default() -> Self {
        KafkaScheduler {
            name: String::from("Kafka-Scheduler-unnanmed"),
            delay: Duration::from_millis(0),
            period: -1i32,
            tx: None,
        }
    }
}

// Scheduled tasks will receive an mpsc tx to report back results.
// pub fn spawn_interval(
//    tx: mpsc::Sender<SomeTask>,
//) -> impl Future<Item = (), Error = ()> {
//}
