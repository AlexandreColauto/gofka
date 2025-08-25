package model

import (
	"log"

	pb "github.com/alexandrecolauto/gofka/proto/broker"
)

type ClusterMetadata struct {
	Brokers map[string]*BrokerInfo
	Topics  map[string]*TopicInfo
}

type TopicInfo struct {
	Name       string
	Partitions map[int32]*PartitionInfo
}

type PartitionInfo struct {
	ID        int32
	TopicName string
	Leader    string
	Replicas  []string
	ISR       []string
	Epoch     int64
}

type client interface {
	ApplyCreateTopic(ctc *pb.Command_CreateTopic)
	ApplyRegisterBroker(ctc *pb.Command_RegisterBroker)
	ApplyUpdateBroker(ctc *pb.Command_UpdateBroker)
	ApplyUpdatePartitionLeader(ctc *pb.Command_ChangePartitionLeader)
}

func NewClusterMetadata() *ClusterMetadata {
	b := make(map[string]*BrokerInfo)
	t := make(map[string]*TopicInfo)
	return &ClusterMetadata{Brokers: b, Topics: t}
}

func (c *ClusterMetadata) DecodeLog(logEntry *pb.LogEntry, cli client) {
	cmd := logEntry.Command
	if cmd == nil {
		return
	}
	switch cmd.Type {
	case pb.Command_CREATE_TOPIC:
		if createTopic, ok := cmd.Payload.(*pb.Command_CreateTopic); ok {
			c.CreateTopic(*createTopic)
			cli.ApplyCreateTopic(createTopic)
		}
	case pb.Command_REGISTER_BROKER:
		if registerBroker, ok := cmd.Payload.(*pb.Command_RegisterBroker); ok {
			c.RegisterBroker(registerBroker)
			cli.ApplyRegisterBroker(registerBroker)
		}

	case pb.Command_UPDATE_BROKER:
		if updateBroker, ok := cmd.Payload.(*pb.Command_UpdateBroker); ok {
			c.UpdateBroker(updateBroker)
			cli.ApplyUpdateBroker(updateBroker)
		}
	case pb.Command_CHANGE_PARTITION_LEADER:
		if changePartition, ok := cmd.Payload.(*pb.Command_ChangePartitionLeader); ok {
			c.UpdatePartitionLeader(changePartition)
			cli.ApplyUpdatePartitionLeader(changePartition)
		}

	default:
		log.Printf("Unknown metadata command: %+v\n", cmd.Payload)
	}
}

func (c *ClusterMetadata) UpdatePartitionLeader(ctc *pb.Command_ChangePartitionLeader) {
	for _, asgn := range ctc.ChangePartitionLeader.Assignments {
		t, ok := c.Topics[asgn.TopicId]
		if !ok {
			continue
		}
		p, ok := t.Partitions[asgn.PartitionId]
		if !ok {
			continue
		}
		p.Leader = asgn.NewLeader
		p.ISR = asgn.NewIsr
		p.Replicas = asgn.NewReplicas
		p.Epoch = int64(asgn.NewEpoch)
	}

}

func (c *ClusterMetadata) UpdateBroker(ctc *pb.Command_UpdateBroker) {
	brk, ok := c.Brokers[ctc.UpdateBroker.Id]
	if !ok {
		return
	}
	brk.Address = ctc.UpdateBroker.Address
	brk.LastSeen = ctc.UpdateBroker.LastSeen.AsTime()
	brk.Alive = ctc.UpdateBroker.Alive
}

func (c *ClusterMetadata) RegisterBroker(ctc *pb.Command_RegisterBroker) {
	brk := &BrokerInfo{
		ID:       ctc.RegisterBroker.Id,
		Address:  ctc.RegisterBroker.Address,
		LastSeen: ctc.RegisterBroker.LastSeen.AsTime(),
		Alive:    ctc.RegisterBroker.Alive,
	}
	c.Brokers[ctc.RegisterBroker.Id] = brk
}

func (c *ClusterMetadata) CreateTopic(ctc pb.Command_CreateTopic) {
	parts := make(map[int32]*PartitionInfo)
	for i := range ctc.CreateTopic.NPartitions {
		replicas := make([]string, ctc.CreateTopic.ReplicationFactor)
		p := &PartitionInfo{
			TopicName: ctc.CreateTopic.Topic,
			ID:        int32(i),
			Replicas:  replicas,
		}
		parts[int32(i)] = p
	}
	topic := &TopicInfo{
		Name:       ctc.CreateTopic.Topic,
		Partitions: parts,
	}
	c.Topics[ctc.CreateTopic.Topic] = topic
}
