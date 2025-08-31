package consumer

import (
	"crypto/rand"
	"fmt"
	"time"

	pb "github.com/alexandrecolauto/gofka/proto/broker"
	"google.golang.org/grpc"
)

type Consumer struct {
	id          string
	group       ConsumerGroup
	assignments Assignments
	cluster     Cluster

	heartbeatTicker *time.Ticker
	stopHeartBeat   chan bool
}

type Assignments struct {
	session *pb.ConsumerSession
}

func NewConsumer(groupID string, brokerAddress string, topics []string) *Consumer {
	consumerID := generateConsumerID()
	b_conn := make(map[string]*grpc.ClientConn)
	b_cli := make(map[string]pb.ConsumerServiceClient)
	b_ass := make(map[string][]*pb.PartitionInfo)
	s_hb := make(chan bool)
	group := ConsumerGroup{id: groupID, coordinator: Coordinator{address: brokerAddress}}
	clu := Cluster{
		connections: b_conn,
		clients:     b_cli,
		assignments: b_ass,
	}
	c := Consumer{id: consumerID, group: group, stopHeartBeat: s_hb, cluster: clu}
	c.Dial()
	c.findGroupCoordinator()
	c.registerConsumer(consumerID, groupID, topics)
	c.startHeartbeat()

	return &c
}

func (c *Consumer) Poll(timeout time.Duration, opt *pb.ReadOptions) ([]*pb.Message, error) {
	response := []*pb.Message{}
	for brokerID, assignments := range c.cluster.assignments {
		topics := make([]*pb.FromTopic, 0)
		for _, assign := range assignments {
			req := &pb.FromTopic{
				Topic:     assign.TopicName,
				Partition: assign.Id,
				Offset:    0,
			}
			topics = append(topics, req)
		}

		msgs, err := c.fetchMessages(brokerID, topics, timeout, opt)
		if err != nil {
			return nil, err
		}
		response = append(response, msgs...)

	}
	return response, nil
}

func (c *Consumer) CommitOffsets() {
	for brokerID, assignments := range c.cluster.assignments {
		topics := make([]*pb.FromTopic, 0)
		for _, assign := range assignments {
			req := &pb.FromTopic{
				Topic:     assign.TopicName,
				Partition: assign.Id,
				Offset:    assign.Offset,
			}
			topics = append(topics, req)
		}

		c.commitOffset(brokerID, topics)

	}
}

func generateConsumerID() string {
	timestamp := time.Now().UnixNano()
	randomID := generateShortID()
	return fmt.Sprintf("consumer-%d-%s", timestamp, randomID)
}

func generateShortID() string {
	bytes := make([]byte, 4)
	rand.Read(bytes)
	return fmt.Sprintf("%x", bytes)
}
