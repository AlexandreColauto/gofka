# Kafka in Go Lang

This repo is a exercise to try to implement the queue service Kafka in go lang,
to show my Go lang skills and my knoledge of high available message system.

## Core Architecture & Concepts 🏛️
These are the absolute fundamentals of Kafka's design.

Topics, Partitions, and Offsets:

Topic: A named stream of records, like a table in a database (e.g., user_logins, order_updates).

Partition: A topic is split into one or more partitions. Each partition is an ordered, immutable sequence of records—essentially an append-only log. Partitions are the key to Kafka's scalability.

Offset: Each record within a partition is assigned a unique sequential ID called an offset. Consumers track their position in a partition using this offset. 

Producers, Consumers, and Brokers:

Producer: An application that writes (publishes) records to a Kafka topic.

Consumer: An application that reads (subscribes to) records from one or more topics.

Broker: A single Kafka server. A Kafka cluster is composed of one or more brokers working together.

## Producer Features 🚀
Features related to how data is sent to Kafka.

Partitioning Strategy: Producers can control which partition a message is sent to. The default is to use a hash of the message key (murmur2 hash) to ensure all messages with the same key go to the same partition. This guarantees ordering for records with that key. If the key is null, it uses a round-robin strategy.

Acknowledgements (acks): Producers can configure the level of acknowledgement they require from the broker before considering a message "sent." This controls the durability guarantee.

acks=0: Fire-and-forget. Lowest latency, but no guarantee of delivery.

acks=1: The leader broker confirms the write. The default. Good balance.

acks=all: The leader waits for all in-sync replicas to also confirm the write. Highest durability, but higher latency.

Idempotent Producer: A feature that ensures that resending a message (due to a network error, for example) will not result in a duplicate message being written to the log.

## Consumer Features 🎣
Features related to how data is read from Kafka.

Consumer Groups: This is the primary mechanism for scalable consumption.

Multiple consumers can form a group to read from a topic.

Kafka guarantees that each partition is consumed by only one consumer within the group.

If you have more consumers in a group than partitions, some consumers will be idle. If you have fewer consumers than partitions, some consumers will read from multiple partitions.

Consumer Rebalancing: When a consumer joins or leaves a group (or crashes), the cluster automatically triggers a "rebalance" to redistribute the partitions among the remaining consumers. This provides dynamic fault tolerance.

Offset Management: As you've seen, consumers need to "commit" the offsets they have successfully processed. This is how the system knows where to resume from. The key decision is when to commit, which defines the processing semantics:

At-least-once: Commit offsets after processing messages. If the app crashes after processing but before committing, it will re-process messages on restart.

At-most-once: Commit offsets before processing messages. If the app crashes after committing but before processing, it will miss messages on restart.

## Broker & Cluster Features 🏰
These features provide Kafka's signature fault tolerance and scalability.

Replication: Each partition can be replicated across multiple brokers. For each partition, one broker is elected as the leader and the others become followers.

All writes and reads for a partition go to the leader.

Followers passively copy the data from the leader.

Fault Tolerance: If a leader broker fails, the cluster controller elects one of the in-sync followers to become the new leader for that partition. This allows the system to continue operating without data loss.

Cluster Coordination (KRaft/ZooKeeper): The cluster needs a way to manage its state: which brokers are alive, who is the leader for each partition, topic configurations, etc. Historically this was done by Apache ZooKeeper, but newer versions use Kafka's own protocol called KRaft.

## Data & Topic Management 📜
How the data is stored and maintained over time.

Log Retention: Kafka doesn't keep records forever. You can configure topics to discard old records based on:

Time: e.g., delete records older than 7 days.

Size: e.g., delete old records when the partition size exceeds 10 GB.

Log Compaction: A different retention strategy. Instead of deleting all old records, compaction ensures that Kafka retains at least the last known value for each message key. This is useful for change-data-capture or maintaining state. Any previous messages with the same key are removed.


------------------------------------
# Kafka Go Implementation - Feature Checklist

## Broker Functionalities

### Topic & Partition Management
- [x] Create, delete, and configure topics
- [ ] Manage partition assignment and distribution
- [ ] Handle partition leadership election
- [ ] Support partition rebalancing across brokers
- [ ] Maintain partition replicas and replica synchronization
- [ ] Handle partition reassignment operations

### Message Storage & Persistence
- [ ] Write messages to log segments on disk
- [ ] Manage log segment files (creation, rotation, deletion)
- [ ] Handle message indexing (offset index, time index)
- [ ] Implement log compaction for keyed messages
- [ ] Support configurable retention policies (time-based, size-based)
- [ ] Handle log cleanup and garbage collection

### Replication & Fault Tolerance
- [ ] Maintain in-sync replica (ISR) sets
- [ ] Handle leader-follower replication
- [ ] Manage replica lag monitoring
- [ ] Implement unclean leader election options
- [ ] Handle broker failure detection and recovery
- [ ] Support rack-aware replica placement

### Client Protocol Handling
- [ ] Process produce requests (single/batch)
- [ ] Handle fetch requests from consumers
- [ ] Manage metadata requests and responses
- [ ] Support administrative operations (create topics, etc.)
- [ ] Handle SASL/SSL authentication and authorization
- [ ] Process heartbeat and group coordination requests

### Cluster Coordination
- [ ] Integrate with ZooKeeper for cluster metadata
- [ ] Handle broker registration and discovery
- [ ] Manage controller election and operations
- [ ] Support dynamic configuration changes
- [ ] Handle cluster membership changes

## Producer Functionalities

### Message Publishing
- [ ] Send messages to topics/partitions
- [ ] Support synchronous and asynchronous sending
- [ ] Handle message batching for efficiency
- [ ] Implement custom partitioning strategies
- [ ] Support message headers and metadata
- [ ] Handle message serialization

### Delivery Guarantees
- [ ] At-most-once delivery semantics
- [ ] At-least-once delivery semantics
- [ ] Exactly-once semantics (idempotent producer)
- [ ] Support configurable acknowledgment levels (acks=0,1,all)
- [ ] Handle retries and retry backoff
- [ ] Implement producer transaction support

### Performance & Reliability
- [ ] Connection pooling and management
- [ ] Request batching and compression (gzip, snappy, lz4, zstd)
- [ ] Buffer management and memory allocation
- [ ] Handle network failures and reconnection
- [ ] Support producer metrics and monitoring
- [ ] Implement backpressure handling

### Configuration & Tuning
- [ ] Configurable batch size and linger time
- [ ] Buffer memory management
- [ ] Request timeout configuration
- [ ] Retry and delivery timeout settings
- [ ] Compression type selection
- [ ] Partitioner configuration

## Consumer Functionalities

### Message Consumption
- [ ] Poll messages from subscribed topics
- [ ] Support topic subscription and partition assignment
- [ ] Handle message deserialization
- [ ] Process message headers and metadata
- [ ] Support seek operations to specific offsets
- [ ] Handle consumer pause/resume operations

### Group Management
- [ ] Join and participate in consumer groups
- [ ] Handle group rebalancing operations
- [ ] Support different partition assignment strategies (range, round-robin, sticky)
- [ ] Manage group membership and heartbeats
- [ ] Handle consumer group coordination
- [ ] Support static group membership

### Offset Management
- [ ] Commit offsets manually or automatically
- [ ] Support different commit strategies (sync/async)
- [ ] Handle offset storage (Kafka internal topics or external)
- [ ] Support offset reset policies (earliest, latest, none)
- [ ] Handle duplicate and missing offset scenarios
- [ ] Support transaction-aware offset commits

### Delivery & Processing
- [ ] Support at-least-once and exactly-once consumption
- [ ] Handle consumer lag monitoring
- [ ] Support message filtering and processing
- [ ] Handle consumer rebalance callbacks
- [ ] Support batch consumption patterns
- [ ] Implement flow control mechanisms

### Configuration & Reliability
- [ ] Connection management and failover
- [ ] Session timeout and heartbeat configuration
- [ ] Fetch size and buffer management
- [ ] Consumer metrics and monitoring
- [ ] Handle network partitions and recovery
- [ ] Support consumer interceptors

## Cross-Component Functionalities

### Security
- [ ] SSL/TLS encryption
- [ ] SASL authentication (PLAIN, SCRAM, GSSAPI, OAUTHBEARER)
- [ ] Access control lists (ACLs)
- [ ] Principal-based authorization
- [ ] Certificate-based authentication

### Monitoring & Observability
- [ ] JMX metrics exposure (or Go equivalent)
- [ ] Health check endpoints
- [ ] Request/response latency tracking
- [ ] Throughput and error rate metrics
- [ ] Resource utilization monitoring

### Administrative Operations
- [ ] Topic creation/deletion/configuration
- [ ] Partition management operations
- [ ] Consumer group management
- [ ] Offset management operations
- [ ] Broker configuration updates
- [ ] Cluster maintenance operations

## Implementation Progress

**Broker**: 0/25 tasks completed
**Producer**: 0/16 tasks completed  
**Consumer**: 0/21 tasks completed
**Cross-Component**: 0/16 tasks completed

**Total Progress**: 0/78 tasks completed (0%)

---

## Notes
- Check off tasks as you complete them
- Consider implementing core functionality first (basic produce/consume) before advanced features
- Some features may be interdependent - plan implementation order accordingly
- Consider which features are critical for MVP vs nice-to-have for full compatibility
