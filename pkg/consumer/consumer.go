package consumer

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"time"

	broker "github.com/alexandrecolauto/gofka/pkg/broker"
	pb "github.com/alexandrecolauto/gofka/proto/broker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Consumer struct {
	id              string
	group_id        string
	brokerAddress   string
	brokerId        string
	assignment      *pb.ConsumerSession
	heartbeatTicker *time.Ticker
	stopHeartBeat   chan bool
	offsets         map[broker.OffsetKey]int64

	brokersConnPoll    map[string]*grpc.ClientConn
	brokersCliPoll     map[string]pb.ConsumerServiceClient
	brokersAssignments map[string][]*pb.PartitionInfo

	brokerConn   *grpc.ClientConn
	brokerClient pb.ConsumerServiceClient
}

func NewConsumer(groupID string, brokerAddress string) *Consumer {
	consumerID := generateConsumerID()
	s_hb := make(chan bool)
	of := make(map[broker.OffsetKey]int64)
	c := Consumer{id: consumerID, group_id: groupID, stopHeartBeat: s_hb, offsets: of, brokerAddress: brokerAddress}
	c.Dial()
	//find group coordinator by group id
	c.findGroupCoordinator()
	//sends join group; coordinator wait for all joins or timeout
	c.RegisterConsumer(consumerID, groupID)
	c.startHeartbeat()
	//first connected become leader
	//respond with list of consumers + leader + available topic/paritions
	//leader decides partition assignments
	//leader send syncgroup with assignments, others sends empty sysnc group
	//coordinator respond with assignments

	return &c
}

func (c *Consumer) Dial() {
	conn, err := grpc.NewClient(c.brokerAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		panic(err)
	}

	c.brokerConn = conn
	c.brokerClient = pb.NewConsumerServiceClient(conn)
}

func (c *Consumer) connect(address string) (*grpc.ClientConn, pb.ConsumerServiceClient, error) {
	conn, err := grpc.NewClient(c.brokerAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, nil, err
	}

	cli := pb.NewConsumerServiceClient(conn)
	return conn, cli, nil
}

func (c *Consumer) RegisterConsumer(consumerId, groupId string) {
	fmt.Println("Registering new consumer")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req := &pb.RegisterConsumerRequest{
		Id:      consumerId,
		GroupId: groupId,
	}
	res, err := c.brokerClient.HandleRegisterConsumer(ctx, req)
	if err != nil {
		panic(err)
	}
	if !res.Success {
		panic(res.ErrorMessage)
	}
	if res.Leader == c.id {
		updated, err := c.assignPartitions(res)
		if err != nil {
			return
		}
		c.SyncGroup(updated)
	}
}

func (c *Consumer) SyncGroup(updated []*pb.ConsumerSession) {
	fmt.Println("Registering new consumer")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req := &pb.SyncGroupRequest{
		GroupId:   c.group_id,
		Consumers: updated,
	}
	res, err := c.brokerClient.HandleSyncGroup(ctx, req)
	if err != nil {
		panic(err)
	}
	if !res.Success {
		panic(res.ErrorMessage)
	}
	c.assignment = res.Assignment
	c.connectToBrokers()
}

func (c *Consumer) assignPartitions(res *pb.RegisterConsumerResponse) ([]*pb.ConsumerSession, error) {
	topics := res.AllTopics
	consumers := res.Consumers
	metadata, err := c.fetchMetadataFor(topics)
	if err != nil {
		return nil, err
	}
	updated_consumers := c.distribuite(consumers, metadata)
	return updated_consumers, nil
}

func (c *Consumer) fetchMetadataFor(topics []string) ([]*pb.TopicInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := &pb.FetchMetadataForTopicsRequest{
		Topics: topics,
	}

	res, err := c.brokerClient.HandleFetchMetadataForTopics(ctx, req)
	if err != nil {
		fmt.Println("error fetching msg:", err)
		return nil, err
	}
	if !res.Success {
		return nil, fmt.Errorf(res.ErrorMessage)
	}
	return res.TopicMetadata, nil
}

func (c *Consumer) distribuite(consumers []*pb.ConsumerSession, metadata []*pb.TopicInfo) []*pb.ConsumerSession {
	for _, topic := range metadata {
		cons := findConsumers(consumers, topic.Name)
		n_cons := len(cons)
		n_parts := len(topic.Partitions)
		for part_id := range n_parts {
			cons_id := part_id % n_cons
			c := cons[cons_id]
			p := topic.Partitions[int32(part_id)]
			c.Assignments = append(c.Assignments, p)
		}
	}
	return consumers
}

func findConsumers(consumers []*pb.ConsumerSession, topic string) []*pb.ConsumerSession {
	res := make([]*pb.ConsumerSession, 0)
	for _, c := range consumers {
		for _, t := range c.Topics {
			if t == topic {
				res = append(res, c)
			}
		}
	}
	return res
}

func (c *Consumer) findGroupCoordinator() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := &pb.GroupCoordinatorRequest{
		GroupId: c.group_id,
	}

	res, err := c.brokerClient.HandleGroupCoordinator(ctx, req)
	if err != nil {
		fmt.Println("error fetching msg:", err)
		return err
	}
	if !res.Success {
		return fmt.Errorf(res.ErrorMessage)
	}
	c.brokerAddress = res.CoordinatorAddress
	c.brokerId = res.CoordinatorId
	c.Dial()
	return nil
}

func (c *Consumer) connectToBrokers() error {
	for _, p := range c.assignment.Assignments {
		err := c.connectTo(p)
		if err != nil {
			return err
		}
	}
	return nil
}
func (c *Consumer) connectTo(assignment *pb.PartitionInfo) error {
	_, exists := c.brokersCliPoll[assignment.Leader]
	if exists {
		c.brokersAssignments[assignment.Leader] = append(c.brokersAssignments[assignment.Leader], assignment)
		return nil
	}

	address := assignment.LeaderAddress
	conn, cli, err := c.connect(address)
	if err != nil {
		return err
	}
	c.brokersConnPoll[assignment.Leader] = conn
	c.brokersCliPoll[assignment.Leader] = cli
	c.brokersAssignments[assignment.Leader] = append(c.brokersAssignments[assignment.Leader], assignment)
	return nil
}

func (c *Consumer) Poll(timeout time.Duration, opt *pb.ReadOptions) ([]*pb.Message, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req := &pb.FetchMessageRequest{
		Id:      c.id,
		GroupId: c.group_id,
		Opt:     opt,
	}

	res, err := c.brokerClient.HandleFetchMessage(ctx, req)
	if err != nil {
		fmt.Println("error fetching msg:", err)
		return nil, err
	}
	if !res.Success {
		return nil, fmt.Errorf(res.ErrorMessage)
	}
	if len(res.Messages) > 0 {
		log.Println("consumer - got messages: ", len(res.Messages))
		log.Printf("consumer - last message: %+v\n ", res.Messages[len(res.Messages)-1])
		c.updateOffsets(res.Messages)
		c.commitOffsets()
	}
	return res.Messages, nil
}

func (c *Consumer) updateOffsets(messages []*pb.Message) {
	partitionOffsets := make(map[broker.OffsetKey]int64)

	for _, msg := range messages {
		tp := broker.OffsetKey{Topic: msg.Topic, Partition: int(msg.Partition), GroupID: c.group_id}
		fmt.Printf("msg offset: %d, current: %d\n", msg.Offset, partitionOffsets[tp])
		if msg.Offset >= partitionOffsets[tp] {
			c.offsets[tp] = msg.Offset
		}
	}
}

func (c *Consumer) commitOffsets() {
	for tp, offset := range c.offsets {
		c.commitOffset(tp.Topic, int32(tp.Partition), offset+1)
	}
}

func (c *Consumer) commitOffset(topic string, partition int32, offset int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := &pb.CommitOffsetRequest{
		Id:        c.id,
		GroupId:   c.group_id,
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
	}

	res, err := c.brokerClient.HandleCommitOffset(ctx, req)
	if err != nil {
		return err
	}
	if !res.Success {
		return fmt.Errorf(res.ErrorMessage)
	}
	return nil
}
func (c *Consumer) commitOffsetsAsync() {
	go c.commitOffsets()
}

func (c *Consumer) Subscribe(topic string) error {
	// c.broker.Subscribe(topic, c.group_id)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := &pb.SubscribeRequest{
		GroupId: c.group_id,
		Topic:   topic,
	}

	res, err := c.brokerClient.HandleSubscribe(ctx, req)
	if err != nil {
		return err
	}
	if !res.Success {
		return fmt.Errorf(res.ErrorMessage)
	}
	return nil
}

func (c *Consumer) startHeartbeat() {
	c.heartbeatTicker = time.NewTicker(3 * time.Second)
	go func() {
		for {
			select {
			case <-c.heartbeatTicker.C:
				c.sendHeartbeat()
			case <-c.stopHeartBeat:
				c.heartbeatTicker.Stop()
				return
			}
		}
	}()
}

func (c *Consumer) sendHeartbeat() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := &pb.ConsumerHeartbeatRequest{
		Id:      c.id,
		GroupId: c.group_id,
	}

	res, err := c.brokerClient.HandleConsumerHeartbeat(ctx, req)
	if err != nil {
		fmt.Println("error sending heartbeat: ", err.Error())
		return
	}
	if !res.Success {
		fmt.Println("failed response: ", res.ErrorMessage)
		return
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
