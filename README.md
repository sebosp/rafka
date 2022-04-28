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

Several threads are created, they talk through tokio::sync::mpsc/oneshot channels, these threads
are currently not mapped to kafka's numthread variables.


### The pattern
A variable is shared mutable/locked through different threads in the original code.
An AsyncTask is created for its class. Optionally a Coordinator is created for it,
mostly in its own thread.

The job of a coordinator is to synchronize access to values from multiple requesters.
This is probably a big limitant and may in the end need to be moved to Mutex/Arcs/etc

### Coordinators
Message coordinators are used to handle communication between threads.

- MajordomoCoordinator: Main entry point, a thread is started for it. Receives channels to communicate with LogDirFailureChannel,
KafkaZkClientCoordinator, LogManagerCoordinator.

- ./rafka_core/src/zk/kafka_zk_client.rs KafkaZKClient. zookeeper watches/data-requests should not block the coordinator.

## Running

### Create the config file

```bash
$ bat basic-config
zookeeper.connect = 127.0.0.1:2181/chroot
zookeeper.connection.timeout.ms = 1000
log.dirs = bad-broker-metadata, non-existent-dir
```

### Start zookeeper

```bash
$ docker run -d --name zkp -p 2181:2181 zookeeper:3.6.2
```

### Run rafka

```bash
$ cargo run -- -v info basic-config
```

## Status
- Reading configuration file.
- Creation of persistent paths in zookeeper.
- Finalized Feature Change Listeners
- Dynamic Config can be recognized from zookeeper, but the reconfiguration is not done
yet, that should be done later, will focus on functionality and deal with this later.
- Dynamic Broker Configurations
- LogManager Start

## Next
- Log(Segment) start.

## Current issues
-  If zookeeper is not available, the CPU usage goes crazy, using connection timeout/etc doesn't seem to help.

## TODO
- The properties from KafkaConfig for example could be defined but unreferenced (i.e. when we read the config file, a specific property could not be in the `match{}`
  Find a way to derive maybe something that requires a setteable?
  This has been partially alleviated by having an enum for every ConfigProperties so that at least
  the clippy lints/cargo complaints about it.
- Create a KafkaConfigDefaults that contains the default values when they are computed, i.e. `pub const fn default_something((24 * 7).to_string())`

## In Progress
- PR created for zookeeper_async to fix watching over the chroot. Waiting for merge.
  The repo doesn't seem active.

## Current java/scala process flow.
- core/src/main/scala/kafka/Kafka.scala parses flags such as --version and creates KafkaServerStartable
  - This is moved to rafka/src/main.rs where clap is used to parse version/help
    TODO: Die early with `--version/--help`
- core/src/main/scala/kafka/server/KafkaServerStartable.scala creates KafkaServer with staticServerConfig and then calls the KafkaServer startup()
  - This is moved to rafka/src/main.rs kafka_server is moved to its own thread and communicates with majordomo thread to talk to zk/etc.
