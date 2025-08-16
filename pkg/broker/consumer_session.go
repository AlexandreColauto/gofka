package broker

import "time"

type ConsumerSession struct {
	msg_ch         chan []*Message
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
