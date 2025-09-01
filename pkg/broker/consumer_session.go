package broker

import (
	"time"

	"github.com/alexandrecolauto/gofka/proto/broker"
)

type ConsumerSession struct {
	last_heartbeat time.Time
	id             string
	topics         []string
	partitions     []*broker.PartitionInfo
}

type TopicPartition struct {
	Topic     string
	Partition int
}

func NewConsumerSession(id string, topics []string) *ConsumerSession {
	return &ConsumerSession{id: id, topics: topics}
}
