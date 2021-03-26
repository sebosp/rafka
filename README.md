# Purpose
Rafka is a learning process of learning both kafka and rust
at the same time. At no point is this expected to work and
it's just a deep dive.
Probably 99% of the stuff will be unimplemented.
However, it is expected that some tools for troubleshooting/etc
could arise from this.

## Significant differences

### Mutexes, atomic latches, etc:

Right now there are no mutexes and mostly this is trying to use mpsc to send
tasks around to a main coordinator (mpsc)

### Zookeeper
CreateRequest and CreateResponse are not available in the rust crate
(zookeeper-async, proto.rs private modules) while on the original java
code they are available, that gives the java code way lower level access
to zookeeper, maybe a similar approach can be done here by forking the
original repo and moving some stuff here

## Running

### Create the config file

```bash
$ cat rafka-config
zookeeper.connect = 127.0.0.1:2181/my-chroot
```

### Start zookeeper

```bash
$ docker run -d --name zkp -p 2181:2181 zookeeper:3.6.2
```

### Run rafka

```bash
$ cargo run -- -v info rafka-config
```


## Status
- Reading configuration file.
- Creation of persistent paths in zookeeper.

## Next
- Finalized Feature Change Listeners

## Current issues
-  If zookeeper is not available, the CPU usage goes crazy, using connection timeout/etc doesn't seem to help.


## TODO
Currently tokio is multithreaded. However the consumption of the messages is not or interaction between resources is not.
As ZNode /features is being implemented, the need for multiple threads becomes clear:
- A ZNode needs to watch state/content changes.
- When a ZNode changes, a subsequent call to zookeeper is needed to update the FinalizedFeatures.
- While data is being fetched from zookeeper, other messages should be served.

Thus, some threads will be dedicated to:
- ./rafka_core/src/majordomo/mod.rs Majordomo Coordinator. This is the main thread that coordinates services/messages/state
- ./rafka_core/src/zk/kafka_zk_client.rs KafkaZKClient. Needed because zookeeper watches/data-requests should not block the coordinator.
- ./rafka_core/src/server/finalize_feature_change_listener.rs FeatureCacheUpdater. Needed because while data is being requested from zookeeper,
   the coordinator shouldn't be blocked, likewise, more than one request operation on zookeeper may go at a given time.
   This tread may not be needed if the zookeeper req/res messages are uniquely identified.
   The original majordomo protocol does version messages with UUID and client, when requesting updates,
   the clients provide the UUID and the coordinator keeps track of the data returned for each req
   Uniquely identifying the messages and storing the data in the coordinator would
   point into following really the MDP protocol, returning status codes and UUIDs
   to requests and letting clients request responses in the future, this breaks a bit the ideas of oneshot channels.

### Breakdown
The zookeeper client ownership needs to be taken out of the coordinator and move it to KafkaZKClient.
The Majordomo coordinator to call new KafkaZKClient `setup_message_processor(&kafka_config, majordomo_tx.clone()) -> tokio::sync::mpsc::Sender<KafkaZKClientAsyncTask> `, and store the returned channel endpoint for the majordomo coordinator loop.
The Majordomo coordinator to call new FeatureCacheUpdater `setup_message_processor(&kafka_config, majordomo_tx.clone()) -> tokio::sync::mpsc::Sender<FeatureCacheUpdaterAsyncTask> `, and store the returned channel endpoint for the majordomo coordinator loop.
The KafkaZKClient to have a new functionality to add watchers.
A messages processor loop needs to be created in KafkaZKClient that to work on KafkaZKClientAsyncTask.
ZK Watchers will trigger messages to the coordinator as spawned tokio tasks (background somehow) (`majordomo_tx.send(ZKChangeTrigger(path: String))`
The Majordomo coordinator will read these messages and once processed by, say, FinalizedFeatures, may need to read again zookeeper data (i.e. what data has changed). `kafka_zk_client_tx.send(KafkaZKClientAsyncTask::GetDataAndVersion(path))`
The KafkaZKClient will process the fetch request and once data is available locally it will send the response to the Majordomo coordinator.
The majordomo Coordinator will sent the response to the FinalizedFeatures cache it owns.
