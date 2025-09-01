# Kafka in Go Lang

This repo is a exercise to try to implement the queue service Kafka in go lang,
to show my Go lang skills and my knoledge of high available message system.

✅ KRaft Controller Checklist
Cluster Management

[x] Handles broker registration and accepts heartbeats.

[x] Correctly detects and fences stale or failed brokers.

[x] Elects one controller as the active leader via the Raft protocol.

[x] Replicates metadata logs across the controller quorum.

Metadata Operations

[x] API to create topics with a specified name, partition count, and replication factor.

[ ] API to delete topics.

[ ] API to alter topic configurations.

[x] Persists all metadata changes to its log.

Partition Leadership

[x] Elects a leader for each new partition.

[x] Assigns an In-Sync Replica (ISR) list for each partition.

[ ] API to update In-Sync Replica (ISR) list for each partition.

[x] Triggers a new leader election when a partition leader fails.

✅ Kafka Broker Checklist
Client Request Handling

[x] Handles Produce requests and applies messages to the correct partition log.

[ ] Implements acknowledgment logic for acks=0, acks=1, and acks=all.

[x] Handles Fetch requests, returning records from a specified offset.

[x] Respects fetch parameters like fetch.min.bytes and fetch.max.wait.ms.

Log Management

[x] Appends records sequentially to partition log segment files.

[x] Implements log rolling (creating new segment files) based on size or time.

[ ] Implements log retention policies (deleting old segments) based on size or time.

Replication

[x] As a follower, correctly fetches data from the partition leader.

[x] As a leader, tracks the progress of its followers and manages the ISR list.

[x] Correctly updates and exposes the High Watermark (the offset of the last committed message).

Controller Communication

[x] Registers with the KRaft controllers on startup.

[x] Sends periodic heartbeats to the active controller.

[x] Fetches and applies metadata changes from the controller.

✅ Producer Checklist
Core Sending Logic

[ ] Connects to bootstrap servers to discover the full cluster topology.

[ ] Implements a default partitioner (round-robin if no key, hashed key otherwise).

[ ] Allows for a custom partitioner to be plugged in.

[ ] Batches records based on size (batch.size) and time (linger.ms).

[ ] Implements record serialization for keys and values.

Reliability & Error Handling

[ ] Correctly handles acks settings (0, 1, all).

[ ] Implements a retry mechanism for transient errors (e.g., LEADER_NOT_AVAILABLE).

[ ] Refreshes metadata when it detects cluster changes or errors.

Compression

[ ] Supports message compression (e.g., Gzip, Snappy, LZ4, ZSTD).

✅ Consumer Checklist
Group Management

[x] Joins a consumer group and receives a partition assignment.

[x] Participates in rebalances cleanly (revoking old partitions, being assigned new ones).

[x] Sends periodic heartbeats to the group coordinator to avoid being kicked out.

Fetching & Processing

[ ] Subscribes to one or more topics.

[x] Fetches records from its assigned partitions.

[ ] Implements deserialization for keys and values.

Offset Management

[ ] Implements automatic, periodic offset commits.

[x] Provides an API for manual offset commits (synchronous and asynchronous).

[ ] Supports offset reset policies (earliest, latest).
