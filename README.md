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

## Threads and message passing

Several threads are created, they talk through tokio::sync::mpsc/oneshot channels.
- ./rafka_core/src/majordomo/mod.rs Majordomo Coordinator. This is the main thread that coordinates services/messages/state
- ./rafka_core/src/zk/kafka_zk_client.rs KafkaZKClient. zookeeper watches/data-requests should not block the coordinator.

## Running

### Create the config file

```bash
$ cat rafka-config zookeeper.connect = 127.0.0.1:2181/my-chroot
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

- The PathChildrenCache seems to be creating the /feature ZNode:
```
zookeeper_async::zookeeper: ZooKeeper::add_listener
zookeeper_async::zookeeper_ext: ensure_path /feature
zookeeper_async::zookeeper: ZooKeeper::create
zookeeper_async::zookeeper: request opcode=Create xid=14
```

Currently the PathChildrenCache is used to watch over the /feature, which has no children.
Watching over / as a workaround doesn't work as Initialized event type doesn't include which path changed so we cannot know which ZNodeHandler to call.
It seems a possible solution is to include an adaptation of code from src/recipes/cache.rs that doesn't create the path.
