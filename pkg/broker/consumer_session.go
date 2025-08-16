package broker

import (
	"time"

	"github.com/alexandrecolauto/gofka/pkg/log"
)

type ConsumerSession struct {
	msg_ch         chan []*log.Message
	last_heartbeat time.Time
	id             string
	partitions     []TopicPartition
}

type TopicPartition struct {
	Topic     string
	Partition int
}

func NewConsumerSession(id string) *ConsumerSession {
	return &ConsumerSession{id: id}
}
