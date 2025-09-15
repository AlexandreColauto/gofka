# Gofka 🚀
### A High-Performance Apache Kafka Implementation in Go

[![Build Status](https://img.shields.io/badge/build-passing-brightgreen)](https://github.com/alexandrecolauto/gofka)
[![Go Version](https://img.shields.io/badge/go-1.21+-blue)](https://golang.org/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![Coverage](https://img.shields.io/badge/coverage-85%25-brightgreen)](https://github.com/alexandrecolauto/gofka)

> **A complete distributed message streaming platform built from scratch in Go, demonstrating advanced systems design, consensus algorithms, and distributed computing principles.**

## 🎯 Why Gofka?

This project showcases enterprise-level Go development skills by implementing Apache Kafka's core functionality from the ground up. It demonstrates:

- **Distributed Systems Mastery**: Raft consensus, leader election, and fault tolerance
- **Performance Engineering**: High-throughput message processing with efficient log storage
- **Go Expertise**: Advanced concurrency patterns, gRPC services, and production-ready code
- **System Design**: Scalable architecture handling thousands of concurrent connections

Perfect for understanding how modern distributed streaming platforms work under the hood.

## ✨ Key Features

- 🏛️ **Raft-based Controllers** - Fault-tolerant cluster coordination with automatic leader election
- 📊 **Partitioned Topics** - Horizontal scaling with intelligent partition assignment
- 🔄 **Producer/Consumer Groups** - Full Kafka protocol compatibility
- 📈 **Real-time Visualizer** - Web dashboard for cluster monitoring and management
- 🗜️ **Compression Support** - Gzip compression for efficient network usage
- 🐳 **Docker Ready** - Complete containerized deployment

## 🚀 Quick Start

### Prerequisites
- Go 1.21+
- Docker & Docker Compose

### 5-Minute Demo

```bash
# Clone and start the cluster
git clone https://github.com/alexandrecolauto/gofka
cd gofka
make run-all

# Access the web visualizer
open http://localhost:8080

# From there you can create topics and start consumer/producers
```

### Basic Usage

```go
// Producer Example
cfg := config.NewProducerConfig()
cfg.BootstrapAddress = "localhost:42069"

p := producer.NewProducer(cfg)

p.UpdateTopic("bar-topic")

err := p.SendMessage("foo", "bar")
if err != nil {
    fmt.Println("error sending msg ", err)
}
```

```go
// Consumer Example
cfg := config.NewConsumerConfig()
cfg.BootstrapAddress = "localhost:42069"
cfg.GroupID = "foo-group"
cfg.Topics = []string{"foo-topic", "bar-topic"}

c := consumer.NewConsumer(cfg)

opt := &broker.ReadOptions{
    MaxMessages: 100,
    MaxBytes:    10 * 1024 * 1024, //10MB max
    MinBytes:    256 * 1024,       // 256kB min
}
msgs, err := c.Poll(5*time.Second, opt)
if err != nil {
    log.Println("Error pooling message: %w", err)
    return
}

fmt.Printf("Yehaaa got %d messages ", len(msgs))
```

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Gofka Cluster                           │
├─────────────────┬─────────────────┬─────────────────────────┤
│   Controllers   │     Brokers     │      Visualizer         │
│                 │                 │                         │
│ ┌─────────────┐ │ ┌─────────────┐ │ ┌─────────────────────┐ │
│ │Controller-1 │ │ │  Broker-1   │ │ │   Web Dashboard     │ │
│ │   (Leader)  │ │ │  (Leader)   │ │ │                     │ │
│ └─────────────┘ │ └─────────────┘ │ │ • Cluster Topology  │ │
│ ┌─────────────┐ │ ┌─────────────┐ │ │ • Topic Management  │ │
│ │Controller-2 │ │ │  Broker-2   │ │ │ • Real-time Metrics │ │
│ │ (Follower)  │ │ │ (Replica)   │ │ │ • Failure Simulation│ │
│ └─────────────┘ │ └─────────────┘ │ └─────────────────────┘ │
│                 │                 │                         │
│   Raft Consensus│  Partition Logs │    gRPC + WebSocket     │
└─────────────────┴─────────────────┴─────────────────────────┘
                          │
                          ▼
              ┌─────────────────────────┐
              │    Client Library       │
              │                         │
              │ ┌─────────┐ ┌─────────┐ │
              │ │Producer │ │Consumer │ │
              │ │         │ │ Groups  │ │
              │ └─────────┘ └─────────┘ │
              └─────────────────────────┘
```

### Core Components

#### 🏛️ **Controllers (Metadata Management)**
- **Raft Consensus**: Leader election and consistent metadata replication
- **Cluster Coordination**: Broker registration, topic creation, partition assignment
- **Fault Recovery**: Automatic failover and cluster rebalancing

#### 🏢 **Brokers (Message Storage)**
- **Append-Only Logs**: High-performance sequential writes like Apache Kafka
- **Partition Leadership**: Leader/follower replication for fault tolerance  
- **Log Segments**: Efficient storage with configurable segment rolling

#### 📤 **Producers**
- **Partition Strategy**: Key-based hashing and sticky partitioning
- **Batch Processing**: Efficient message batching for throughput
- **Compression**: Gzip compression to reduce network overhead

#### 📥 **Consumers**  
- **Consumer Groups**: Automatic partition assignment and rebalancing
- **Offset Management**: Reliable message delivery guarantees
- **Concurrent Processing**: Efficient goroutine-based message handling

## 🔧 Configuration

<details>
<summary>Click to expand configuration options</summary>

### Broker Configuration
```yaml
# broker.yaml
server: 
  node_id: "broker1"
  address: "gofka-broker1"
  port: 42069
  max_reconnection_retries: 10
  initial_backoff: 250ms
  #roles can be "broker" or "controller" or both.
  roles: 
    - "broker"


broker:
  # Controller bootstrap address
  controller_address: "gofka-controller1:42069"
  # The interval of heartbeat to controller
  # The value should be a duration string (e.g., "5s", "2m", "1h").
  heartbeat_interval: "250ms"
  # The interval of metadata request from controller
  # The value should be a duration string (e.g., "5s", "2m", "1h").
  metadata_interval: "250ms"
  # Max time behind before being removed from ISR
  # The value should be a duration string (e.g., "5s", "2m", "1h").
  max_lag_timeout: "15s"
  replica:
    # Interval to fetch form leader
    # The value should be a duration string (e.g., "5s", "2m", "1h").
    fetch_interval: "500ms"
  consumer_group:
    # Duration of joining phase, all consumers must connect whithin the time
    # The value should be a duration string (e.g., "5s", "2m", "1h").
    joining_duration: "1s"


visualizer:
  # A boolean flag to enable or disable the visualization client.
  # If set to 'true', the client will attempt to connect to the visualizer server.
  enabled: true
  
  # The address of the external visualizer service.
  address: "gofka-visualizer:42169"
```

### Controller Configuration  
```yaml
# controller.yaml
server: 
  node_id: "controller_1"
  address: "gofka-controller1"
  port: 42069
  max_reconnection_retries: 10
  initial_backoff: 250ms
  #roles can be "broker" or "controller" or both.
  roles: 
    - "controller"
  #controllers peers, only needed for controllers servers. Brokers automatically discover and gets redirected to leader.
  cluster:
    peers:
      controller-1: "gofka-controller1:42069"
      controller-2: "gofka-controller2:42069"
      controller-3: "gofka-controller3:42069"
      controller-4: "gofka-controller4:42069"
      controller-5: "gofka-controller5:42069"


kraft:
  # The timeout for Raft operations, such as leader election or append entries.
  # The value should be a duration string (e.g., "5s", "2m", "1h").
  timeout: "5s"
  
  # A grace period for the system to handle potential delays before declaring a failure.
  # The value is also a duration string.
  grace_period: "10s"
```
</details>


## 🎮 Interactive Demo

The **Visualizer Dashboard** provides a hands-on way to explore Gofka:

1. **Real-time Cluster View** - See brokers, controllers, and their states
2. **Topic Management** - Create topics and watch partition assignments
3. **Failure Simulation** - Kill nodes and observe automatic recovery  
4. **Message Flow** - Visualize producer/consumer activity



## 📋 Roadmap

- [ ] **Security** - SASL/SSL authentication and authorization
- [ ] **Streaming API** - Kafka Streams equivalent

## 🙋‍♂️ FAQ

<details>
<summary><strong>How does this compare to Apache Kafka?</strong></summary>

Gofka implements Kafka's core concepts but is built for learning and demonstration. It's not intended for production use but showcases the same distributed systems principles.
</details>

<details>
<summary><strong>Why build this instead of using Kafka?</strong></summary>

This project demonstrates deep understanding of distributed systems, Go programming, and message queue internals - valuable skills for senior engineering roles.
</details>

<details>
<summary><strong>Can I use this in production?</strong></summary>

This is an educational implementation. For production workloads, use Apache Kafka or managed services like Confluent Cloud.
</details>

## 📄 License

MIT License - see [LICENSE](LICENSE) file for details.

## 🚀 About This Project

Built by Organic Dev (Alexandre Colauto) to demonstrate expertise in:
- **Distributed Systems**: Consensus algorithms, fault tolerance, scalability
- **Go Programming**: Advanced concurrency, gRPC, performance optimization  
- **System Design**: Message queues, log-structured storage, microservices
- **DevOps**: Docker, monitoring, automated testing

---

<div align="center">
  
**⭐ Star this repo if it helped you understand distributed systems better!**

[📧 Contact](mailto:alex@dev.organic) | [💼 LinkedIn](https://www.linkedin.com/in/netoalexandre/) 

</div>



