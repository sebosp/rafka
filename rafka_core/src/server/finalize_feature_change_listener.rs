//! Finalized Feature Change Listener
//! kafka/server/FinalizedFeatureChangeListener.scala
//! Listens to changes in the ZK feature node, via the ZK client. Whenever a change notification
//! is received from ZK, the feature cache in FinalizedFeatureCache is asynchronously updated
//! to the latest features read from ZK. The cache updates are serialized through a single
//! notification processor thread.
#[derive(Debug)]
pub struct FinalizedFeatureChangeListener;
