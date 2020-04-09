# ReplicaMap: Key-value Kafka Database
[![Release](https://jitpack.io/v/com.vladykin/replicamap.svg)](https://jitpack.io/#com.vladykin/replicamap)
[![License](https://img.shields.io/badge/license-LGPLv3-blue.svg)](https://github.com/svladykin/replicamap/blob/master/LICENSE)
[![Build Status](https://travis-ci.org/svladykin/replicamap.svg?branch=master)](https://travis-ci.org/svladykin/replicamap)
![Coverage](https://img.shields.io/badge/coverage-92%25-green)

### All the buzzwords description: 

ReplicaMap is a replicated fault tolerant multi-master eventually consistent key-value embeddable in-memory database 
written in Java backed by the persistent Apache Kafka storage.

### Simpler description:

Think of it as a Java `ConcurrentMap<K,V>` that replicates all the updates over Apache Kafka.

## Main features:

+ ReplicaMap is multi-master. All the replicas are equal and writable: easy way to share mutable state between multiple processes.

+ All the atomic operations like `putIfAbsent` or `replace` work as expected.

+ Asynchronous operations `asyncPut`, `asyncPutIfAbsent`, `asyncReplace` and more are supported and return `CompletableFuture`.

+ Optimized Compute: instead of heavy retry loops just send one-shot serializable closures using `compute` or `merge` methods.
  This is also useful when you apply a small update to a huge value to avoid sending the whole value over the network. 

+ Sharding: configure list of allowed Kafka partitions for each instance.

+ Kafka `Log Compaction` provides persistent backup of the replicated map contents.

+ It is possible to wrap any thread-safe implementations of `Map<K,V>` with ReplicaMap. The most obvious choice is `ConcurrentHashMap<K,V>` 
  or `ConcurrentSkipListMap<K,V>`, but any custom off-heap or on-disk or other implementations can be used as well.

+ When `NavigableMap<K,V>` is needed, there is `ReplicaNavigableMap<K,V>`.

+ Multiple maps per single data topic are supported.

+ If you already have a Kafka cluster, there is no need to deploy any new infrastructure, just add the ReplicaMap 
  library to the application and you have a distributed database. 
  
+ If you already have a compacted Kafka topic, you can start using it with ReplicaMap.
  
+ Kafka client is the only dependency of ReplicaMap.

## Maven repository

The project artifacts are hosted on [jitpack.io](https://jitpack.io), you need to add the repository to your project:

```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>
```
And then add the dependency:

```xml
<dependency>
    <groupId>com.vladykin</groupId>
    <artifactId>replicamap</artifactId>
    <version>...</version>
</dependency>
```
The latest published version: [![Release](https://jitpack.io/v/com.vladykin/replicamap.svg)](https://jitpack.io/#com.vladykin/replicamap)

## Usage example

```java
// Setup the configuration like in Kafka (see all the options in KReplicaMapManagerConfig).
Map<String,Object> cfg = new HashMap<>();
cfg.put(KReplicaMapManagerConfig.BOOTSTRAP_SERVERS, BOOTSTRAP_SERVER);
cfg.put(KReplicaMapManagerConfig.DATA_TOPIC, "flowers");
cfg.put(KReplicaMapManagerConfig.OPS_TOPIC, "flowers_ops");
cfg.put(KReplicaMapManagerConfig.FLUSH_TOPIC, "flowers_flush");

// Setup the serialization for map keys and values, by default String serialization is used.
cfg.put(KReplicaMapManagerConfig.KEY_SERIALIZER_CLASS, LongSerializer.class);
cfg.put(KReplicaMapManagerConfig.KEY_DESERIALIZER_CLASS, LongDeserializer.class);

// Create and start the KReplicaMapManager instance.
ReplicaMapManager replicaMapManager = new KReplicaMapManager(cfg);

// Here we use DELETE_DELAY_MS that we have used for the topics configuration
// and adjusted it by 3 minutes to mitigate the possible clock skew.
replicaMapManager.start(DELETE_DELAY_MS - 180_000, TimeUnit.MILLISECONDS);

// Get the ReplicaMap instance and use it as if it was a typical ConcurrentMap,
// but all the updates will be visible to all the other clients even after restarts.
ReplicaMap<Long, String> flowers = replicaMapManager.getMap();

flowers.putIfAbsent(1L, "Rose");
flowers.putIfAbsent(2L, "Magnolia");
flowers.putIfAbsent(3L, "Cactus");

System.out.println(new TreeMap<>(flowers));
```

## Achtung Minen!

All the keys and values must correctly implement `hashCode` and `equals` because ReplicaMap
heavily relies on that. The same for the map identifiers returned by `MapsHolder.getMapId(K key)`.

All the equal keys (where `equals` returns `true`) must always get to the same Kafka partition.
For example, by default Kafka partitions the keys by hashing their bytes produced by the `Serializer`,
if you have some extra fields in the key and these fields do not participate in `equals` but participate 
in serialization, then the equal keys may be placed to different partitions. The sequence of updates 
for the key looses linearizability and the state may become inconsistent across the clients.

ReplicaMap is eventually consistent: beware of possible lags, it is hard to predict when all the 
replicas will receive a given update.

In a typical case you have more than one Kafka partition, thus it is possible to have your updates reordered
if they belong to the different partitions. For example, if you synchronously update key A and then key B and these
keys are not in the same partition, other clients may see the update for the key B earlier than for the key A. 
This is counter-intuitive behavior and it is inconsistent with the program order. If you need the program order to be 
preserved then make sure you either have only 1 partition per topic or have collocated the keys that need this property
to the same partition.

When using Optimized Compute you must never update values inplace, always create a modified copy of the value instead.
Otherwise incorrect state may flushed and data will be broken. For the same reason you must never modify
keys and values in a listener.

## Protocol

There are 3 Kafka topics participate in the protocol (see below about their configuration): 
 - `data` topic, a compacted topic where plain key-value records are stored (an existing topic can be used)
 - `ops` topic, an operations log topic, where every update attempt is sent (analog to WAL in conventional databases)
 - `flush` topic, where flush requests are sent

All these topics must be partitioned the same way.

Each modification attempt requires sending only a single message to the `ops` topic and reading 
it back. Thus, the latency of any modification attempt is the latency of this round trip.
Background `OpsWorker` threads poll incoming operations from the `ops` topic and apply them
to the underlying map. Since all the updates arrive in the same order for the same key (for
the same partition to be exact) we have eventually consistent state across all the replicas.

Optimized Compute feature relies on this per-partition update linearizability. Instead of 
a loop that retries calling `ConcurrentMap.reaplce(K,V,V)` and sending `ops` messages over 
and over again for each attempt, we can serialize the function passed to the `compute` method, 
the function will be sent to all the replicas and it will be executed on each replica only once.
This may be much more efficient for operations like incrementing heavily contended counters 
or applying small updates to huge values. 
It is only needed to implement `ComputeSerializer` and `ComputeDeserializer` interfaces and 
set them to a config. All the methods accepting `BiFunction` support Optimized Compute.

Once in a while a lucky client that has issued the update with the offset of multiple of 
`flush.period.ops` will send a flush request to the `flush` topic to initiate a data flush.
Obviously, the update issuer may die before sending a flush request, then the next guy 
will flush more updates in his turn. 

Background `FlushWorker` threads poll the `flush` topic and flush the updated key-value 
pairs to the `data` topic in a single Kafka transaction. Consumer group for `flush` consumers 
is configured to be the same for all the participants to have flush requests distributed
across the all clients. The transactional id of `data` producer is one per partition 
to fence the previous flushers in case of partition rebalancing. This is needed to have 
"exactly once" semantics for data flush (avoid mixing together racy flushes).

To avoid flush reordering (when a slow client requests a flush for the data that was already 
flushed time ago by another client and newer updates can be overwritten by the older ones) 
`FlushWorker` ignores the incoming out of order flush requests.

## Topics configuration

- All the topics (`data`, `ops` and `flush`) must be setup in a fault tolerant way:  
  `min.insync.replicas=2`  
  `unclean.leader.election.enable=false`
- The `data` topic must be compacted. With some other cleanup policy clients may end up in inconsistent state:  
  `cleanup.policy=compact`
- The `data` topic must not be compacted too early, otherwise races with data loading may occur. 
  Give enough time to load the full data set from Kafka brokers to the client:  
  `min.compaction.lag.ms=7200000`
- The `ops` and `flush` topics can be cleaned:  
  `cleanup.policy=delete`
- The `ops` and `flush` topics must have disabled time based retention, otherwise the logs 
  may become corrupted if the updates were not frequent enough or the application was down for some time:  
  `retention.ms=-1`
- For the `flush` topic retention of 1 GiB per partition must always be enough:  
  `retention.bytes=1073741824`
- For the `ops` topic safe retention size depends on many factors, the simplest way to calculate it for a partition:  
  `(maxKeySize + 2 * maxValueSize + recordOverhead) * (flushPeriodOps + maxFlushLagOps) * flushFailures`  
  Let's take `recordOverhead = 70B` (it may be lower, depending on how many records on average are batched by the producer),  
  `maxKeySize = 16B`, `maxValueSize = 1KiB`, `maxFlushLagOps = flushPeriodOps = 5000`, `flushFailures = 100`,  
  then we need to set retention size to `(16B + 2 * 1KiB + 70B) * (5000 + 5000) * 100 = 2GiB`:  
  `retention.bytes=2147483648`
- Another important factor is not covered in the formula above: while a new client is loading data from the `data` topic
  the found flush notification record in `ops` topic should not be evicted. Thus, we have to configure `ops` topic with 
  delayed file deletion as we did with `data` compaction lag:  
  `file.delete.delay.ms=7200000` 

### Example configuration:

```shell script
BOOTSTRAP_SERVER="localhost:9092"
DATA_TOPIC="flowers"
OPS_TOPIC="${DATA_TOPIC}_ops"
FLUSH_TOPIC="${DATA_TOPIC}_flush"
REPLICATION=3
PARTITIONS=12
MIN_INSYNC_REPLICAS=2
DELETE_DELAY_MS=7200000
OPS_RETENTION_BYTES=2147483648
FLUSH_RETENTION_BYTES=1073741824

bin/kafka-topics.sh --create --topic $DATA_TOPIC --bootstrap-server $BOOTSTRAP_SERVER \
                --replication-factor $REPLICATION --partitions $PARTITIONS \
                --config min.insync.replicas=$MIN_INSYNC_REPLICAS \
                --config unclean.leader.election.enable=false \
                --config cleanup.policy=compact \
                --config min.compaction.lag.ms=$DELETE_DELAY_MS

bin/kafka-topics.sh --create --topic $OPS_TOPIC --bootstrap-server $BOOTSTRAP_SERVER \
                --replication-factor $REPLICATION --partitions $PARTITIONS \
                --config min.insync.replicas=$MIN_INSYNC_REPLICAS \
                --config unclean.leader.election.enable=false \
                --config cleanup.policy=delete \
                --config retention.ms=-1 \
                --config retention.bytes=$OPS_RETENTION_BYTES \
                --config file.delete.delay.ms=$DELETE_DELAY_MS

bin/kafka-topics.sh --create --topic $FLUSH_TOPIC --bootstrap-server $BOOTSTRAP_SERVER \
                --replication-factor $REPLICATION --partitions $PARTITIONS \
                --config min.insync.replicas=$MIN_INSYNC_REPLICAS \
                --config unclean.leader.election.enable=false \
                --config cleanup.policy=delete \
                --config retention.ms=-1 \
                --config retention.bytes=$FLUSH_RETENTION_BYTES \
                --config file.delete.delay.ms=$DELETE_DELAY_MS

bin/kafka-topics.sh --list --bootstrap-server $BOOTSTRAP_SERVER | grep $DATA_TOPIC
```

### How to start with an existing key-value topic as the `data` topic:

0. Make sure other consumers of this data topic (if any) support `read_committed` isolation level.

1. Create empty `ops` and `flush` topics according to the instructions above.

2. Stop all the updates to your existing `data` topic.  
   __This step is very important, otherwise your data may be corrupted or lost 
   or ReplicaMap may have inconsistent state across the clients!!!__

3. Alter the `data` topic configuration to meet the requirements above:
    ```shell script
    ZOOKEEPER="localhost:2181"
    bin/kafka-configs.sh --zookeeper $ZOOKEEPER --entity-type topics --entity-name $DATA_TOPIC --alter --add-config \
       min.insync.replicas=$MIN_INSYNC_REPLICAS,unclean.leader.election.enable=false,cleanup.policy=compact,min.compaction.lag.ms=$DELETE_DELAY_MS
    ```

4. Run the following command to initialize `ops` topic with the last `data` topic offsets:  
    ```shell script
    java -cp slf4j-api-1.7.26.jar:kafka-clients-2.3.1.jar:replicamap-0.3.jar com.vladykin.replicamap.kafka.KReplicaMapTools \
          initExisting $BOOTSTRAP_SERVER $DATA_TOPIC $OPS_TOPIC
    ```
    it must print `OK: ...`

5. __PROFIT!!__ Start using ReplicaMap in your application, all the existing data will be loaded.
