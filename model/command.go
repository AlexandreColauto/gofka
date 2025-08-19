package model

import (
	"encoding/json"
	"time"
)

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

type RegisterBrokerCommand struct {
	ID       string
	Address  string
	Alive    bool
	LastSeen time.Time
}

type PartitionAssignment struct {
	TopicID     string
	PartitionID int32
	NewLeader   string
	NewReplicas []string
	NewISR      []string
	NewEpoch    int
}

type UpdateAssignmentsCommand struct {
	Assignments []PartitionAssignment
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
func NewRegisterBrokerCommand(ID, address string) (*Command, error) {
	payload, err := json.Marshal(RegisterBrokerCommand{
		ID:       ID,
		Address:  address,
		Alive:    true,
		LastSeen: time.Now(),
	})
	if err != nil {
		return nil, err
	}

	return &Command{
		Type:    RegisterBrokerRecord,
		Payload: payload,
	}, nil
}

func NewUpdateAssigmentCommand(assigments []PartitionAssignment) (*Command, error) {
	payload, err := json.Marshal(UpdateAssignmentsCommand{
		Assignments: assigments,
	})
	if err != nil {
		return nil, err
	}

	return &Command{
		Type:    ChangePartitionLeader,
		Payload: payload,
	}, nil
}
