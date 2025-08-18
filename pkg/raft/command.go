package raft

import "encoding/json"

type CommandType string

const (
	CreateTopic           CommandType = "CREATE_TOPIC"
	CreatePartition       CommandType = "CREATE_PARTITION"
	ChangePartitionLeader CommandType = "CHANGE_PARTITION_LEADER"
	RegisterBrokerRecord  CommandType = "REGISTER_BROKER"
	ConfigRecord          CommandType = "CONFIG"
)

type Command struct {
	Type    CommandType
	Payload []byte
}

type CreateTopicCommand struct {
	Topic             string
	NPartition        int
	ReplicationFactor int
}

func NewCreateTopicCommand(topic string, partitions, replicationFactor int) (*Command, error) {
	payload, err := json.Marshal(CreateTopicCommand{
		Topic:             topic,
		NPartition:        partitions,
		ReplicationFactor: replicationFactor,
	})
	if err != nil {
		return nil, err
	}

	return &Command{
		Type:    CreateTopic,
		Payload: payload,
	}, nil
}
