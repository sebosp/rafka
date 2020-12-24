//! Kafka Broker States
//! core/src/main/scala/kafka/server/BrokerStates.scala
//!
//! Broker states are the possible state that a kafka broker can be in.
//! A broker should be only in one state at a time.
//! The expected state transition with the following defined states is:
//                +-----------+
//                |Not Running|
//                +-----+-----+
//                      |
//                      v
//                +-----+-----+
//                |Starting   +--+
//                +-----+-----+  | +----+------------+
//                      |        +>+RecoveringFrom   |
//                      v          |UncleanShutdown  |
//               +-------+-------+ +-------+---------+
//               |RunningAsBroker|            |
//               +-------+-------+<-----------+
//                       |
//                       v
//                +-----+------------+
//                |PendingControlled |
//                |Shutdown          |
//                +-----+------------+
//                      |
//                      v
//               +-----+----------+
//               |BrokerShutting  |
//               |Down            |
//               +-----+----------+
//                     |
//                     v
//               +-----+-----+
//               |Not Running|
//               +-----------+
//
// Custom states is also allowed for cases where there are custom kafka states for different
// scenarios.

pub enum BrokerState {
    NotRunning,                    // 0
    Starting,                      // 1
    RecoveringFromUncleanShutdown, // 2
    RunningAsBroker,               // 3
    PendingControlledShutdown,     // 6
    BrokerShuttingDown,            // 7
    Undefined(u8),                 // Allowing undefined custom state
}

impl Default for BrokerState {
    fn default() -> Self {
        // Behavior grabbed from: @volatile var currentState: Byte = NotRunning.state
        BrokerState::NotRunning
    }
}

// TODO: Impl from_u8 and to_u8
