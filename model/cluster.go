package model

import (
	"errors"
	"fmt"
	"log"
	"time"

	pb "github.com/alexandrecolauto/gofka/proto/broker"
	pc "github.com/alexandrecolauto/gofka/proto/controller"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ClusterMetadata struct {
	Metadata *pb.ClusterMetadata
}

type BrokerInfo struct {
	ID       string
	Address  string
	Alive    bool
	LastSeen time.Time
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

type NotLeaderError struct {
	PartitionID int
	Topic       string
	Message     string
}

func (e *NotLeaderError) Error() string {
	return e.Message
}

func NewNotLeaderError(partition int, topic string) *NotLeaderError {
	return &NotLeaderError{
		PartitionID: partition,
		Topic:       topic,
		Message:     fmt.Sprintf("Not leader of %s-%d", topic, partition),
	}
}

func IsNotLeaderError(e error) bool {
	var err *NotLeaderError
	return errors.As(e, &err)
}

type client interface {
	ApplyCreateTopic(ctc *pc.Command_CreateTopic)
	ApplyRegisterBroker(ctc *pc.Command_RegisterBroker)
	ApplyUpdateBroker(ctc *pc.Command_UpdateBroker)
	ApplyUpdatePartitionLeader(ctc *pc.Command_ChangePartitionLeader)
}

func NewClusterMetadata() *ClusterMetadata {
	b := make(map[string]*pb.BrokerInfo)
	t := make(map[string]*pb.TopicInfo)
	mt := &pb.ClusterMetadata{Brokers: b, Topics: t}

	return &ClusterMetadata{
		Metadata: mt,
	}
}
func (c *ClusterMetadata) FetchMetadata(index int64) *pb.ClusterMetadata {
	if index == c.Metadata.LastIndex {
		return nil
	}
	return c.Metadata
}

func (c *ClusterMetadata) UpdateMetadata(metadata *pb.ClusterMetadata) {
	fmt.Println("New producer metadata: ", metadata)
	c.Metadata = metadata
}

func (c *ClusterMetadata) PartitionCount(topic string) (int, error) {
	t, ok := c.Metadata.Topics[topic]
	if !ok {
		return 0, fmt.Errorf("cannot find topic: %s ", topic)

	}
	return len(t.Partitions), nil
}

func (c *ClusterMetadata) Topic(topic string) (*pb.TopicInfo, bool) {
	t, ok := c.Metadata.Topics[topic]
	return t, ok
}
func (c *ClusterMetadata) Topics() map[string]*pb.TopicInfo {
	return c.Metadata.Topics
}

func (c *ClusterMetadata) Brokers() map[string]*pb.BrokerInfo {
	return c.Metadata.Brokers
}

func (c *ClusterMetadata) DecodeLog(logEntry *pc.LogEntry, cli client) {
	cmd := logEntry.Command
	if cmd == nil {
		return
	}
	c.Metadata.LastIndex = logEntry.Index
	switch cmd.Type {
	case pc.Command_CREATE_TOPIC:
		if createTopic, ok := cmd.Payload.(*pc.Command_CreateTopic); ok {
			c.CreateTopic(*createTopic)
			cli.ApplyCreateTopic(createTopic)
		}
	case pc.Command_REGISTER_BROKER:
		if registerBroker, ok := cmd.Payload.(*pc.Command_RegisterBroker); ok {
			c.RegisterBroker(registerBroker)
			cli.ApplyRegisterBroker(registerBroker)
		}

	case pc.Command_UPDATE_BROKER:
		if updateBroker, ok := cmd.Payload.(*pc.Command_UpdateBroker); ok {
			c.UpdateBroker(updateBroker)
			cli.ApplyUpdateBroker(updateBroker)
		}
	case pc.Command_CHANGE_PARTITION_LEADER:
		fmt.Println("Updating chan*ge partition leader in the metadata")
		if changePartition, ok := cmd.Payload.(*pc.Command_ChangePartitionLeader); ok {
			c.UpdatePartitionLeader(changePartition)
			cli.ApplyUpdatePartitionLeader(changePartition)
		}

	default:
		log.Printf("Unknown metadata command: %+v\n", cmd.Payload)
	}
}

func (c *ClusterMetadata) UpdatePartitionLeader(ctc *pc.Command_ChangePartitionLeader) {
	for _, asgn := range ctc.ChangePartitionLeader.Assignments {
		t, ok := c.Metadata.Topics[asgn.TopicId]
		if !ok {
			continue
		}
		p, ok := t.Partitions[asgn.PartitionId]
		if !ok {
			continue
		}
		p.Leader = asgn.NewLeader
		address := c.Metadata.Brokers[p.Leader].Address
		p.LeaderAddress = address
		p.Isr = asgn.NewIsr
		p.Replicas = asgn.NewReplicas
		p.Epoch = int64(asgn.NewEpoch)
	}

}

func (c *ClusterMetadata) UpdateBroker(ctc *pc.Command_UpdateBroker) {
	brk, ok := c.Metadata.Brokers[ctc.UpdateBroker.Id]
	if !ok {
		return
	}
	brk.Address = ctc.UpdateBroker.Address
	brk.LastSeen = timestamppb.New(ctc.UpdateBroker.LastSeen.AsTime())
	brk.Alive = ctc.UpdateBroker.Alive
}

func (c *ClusterMetadata) RegisterBroker(ctc *pc.Command_RegisterBroker) {
	brk := &pb.BrokerInfo{
		Id:       ctc.RegisterBroker.Id,
		Address:  ctc.RegisterBroker.Address,
		LastSeen: timestamppb.New(ctc.RegisterBroker.LastSeen.AsTime()),
		Alive:    ctc.RegisterBroker.Alive,
	}
	c.Metadata.Brokers[ctc.RegisterBroker.Id] = brk
}

func (c *ClusterMetadata) CreateTopic(ctc pc.Command_CreateTopic) {
	parts := make(map[int32]*pb.PartitionInfo)
	for i := range ctc.CreateTopic.NPartitions {
		replicas := make([]string, ctc.CreateTopic.ReplicationFactor)
		p := &pb.PartitionInfo{
			TopicName: ctc.CreateTopic.Topic,
			Id:        int32(i),
			Replicas:  replicas,
		}
		parts[int32(i)] = p
	}
	topic := &pb.TopicInfo{
		Name:       ctc.CreateTopic.Topic,
		Partitions: parts,
	}
	c.Metadata.Topics[ctc.CreateTopic.Topic] = topic
}

func (c *ClusterMetadata) PartitionLeader(topic string, partition int) (string, string, error) {
	t, ok := c.Metadata.Topics[topic]
	if !ok {
		return "", "", fmt.Errorf("cannot find topic %s", topic)
	}
	p, ok := t.Partitions[int32(partition)]
	if !ok {
		return "", "", fmt.Errorf("cannot find partition %d", partition)
	}
	if p.Leader == "" {
		return "", "", fmt.Errorf("cannot find partition leader %d", partition)
	}
	br, ok := c.Metadata.Brokers[p.Leader]
	if !ok {
		return "", "", fmt.Errorf("cannot find broker %s", p.Leader)
	}

	return p.Leader, br.Address, nil
}

func (c *ClusterMetadata) CommitOffset(topics []*pb.FromTopic) error {
	for _, topic := range topics {
		t, ok := c.Metadata.Topics[topic.Topic]
		if !ok {
			return fmt.Errorf("cannot find topic %s", topic.Topic)
		}
		p, ok := t.Partitions[topic.Partition]
		if !ok {
			return fmt.Errorf("cannot find partition %d on topic %s", topic.Partition, topic.Topic)
		}
		p.CommitedOffset = topic.Offset
	}
	return nil
}
