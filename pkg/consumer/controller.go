package consumer

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	vC "github.com/alexandrecolauto/gofka/pkg/visualizer_client"
	pb "github.com/alexandrecolauto/gofka/proto/broker"
	"google.golang.org/grpc"
)

type Consumer struct {
	id          string
	group       ConsumerGroup
	assignments Assignments
	cluster     Cluster
	topics      []string

	heartbeatTicker *time.Ticker
	stopHeartBeat   chan bool
	visualizeClient *vC.VisualizerClient
}

type Assignments struct {
	session *pb.ConsumerSession
}

func NewConsumer(groupID string, brokerAddress string, topics []string, vc *vC.VisualizerClient) *Consumer {
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
	c := Consumer{id: consumerID, group: group, stopHeartBeat: s_hb, cluster: clu, visualizeClient: vc, topics: topics}
	c.Dial()
	c.findGroupCoordinator()
	err := c.registerConsumer(consumerID, groupID, topics)
	if err != nil {
		fmt.Println("WARNING: failed registering consumer. must do manual registering.")
	}
	c.startHeartbeat()

	if vc != nil {
		vc.Processor.RegisterClient(consumerID, &c)
	}
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
func (c *Consumer) GetClientId() string {
	return c.id
}

func (c *Consumer) AddTopic(topic string) error {
	c.topics = append(c.topics, topic)
	fmt.Printf("Added new topic to client %s. final topics: %v\n", c.id, c.topics)
	return c.reconnect()
}

func (c *Consumer) RemoveTopic(topic string) error {
	if slices.Contains(c.topics, topic) {
		newTopics := make([]string, 0, len(c.topics)-1)
		for _, t := range c.topics {
			if t != topic {
				newTopics = append(newTopics, t)
			}
		}
		c.topics = newTopics
		return c.reconnect()
	}
	return fmt.Errorf("consumer not consuming from topic %s", topic)
}

func (c *Consumer) Consume() error {
	opt := &pb.ReadOptions{
		MaxMessages: 100,
		MaxBytes:    1024 * 1024,
		MinBytes:    1024 * 1024,
	}
	fmt.Println("pooling msg")
	msgs, err := c.Poll(5*time.Second, opt)
	if err != nil {
		return err
	}
	if len(msgs) > 0 && c.visualizeClient != nil {
		highestPartitionOffset := make(map[string]map[int32]int64)
		for _, msg := range msgs {
			t := msg.Topic
			p := msg.Partition
			o := msg.Offset

			if _, ok := highestPartitionOffset[t][p]; !ok {
				partMap := make(map[int32]int64)
				highestPartitionOffset[t] = partMap
			}
			last := highestPartitionOffset[t][p]
			if o > last {
				highestPartitionOffset[t][p] = o
			}
		}
		action := "consumed-offset"
		target := c.id
		msg, err := json.Marshal(highestPartitionOffset)
		if err != nil {
			return err
		}
		c.visualizeClient.SendMessage(action, target, []byte(msg))
	}
	return nil
}

func (c *Consumer) reconnect() error {
	err := c.registerConsumer(c.id, c.group.id, c.topics)
	if err != nil {
		return err
	}
	return nil
}
