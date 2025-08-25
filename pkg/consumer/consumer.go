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
	heartbeatTicker *time.Ticker
	stopHeartBeat   chan bool
	offsets         map[broker.OffsetKey]int64

	brokerConn   *grpc.ClientConn
	brokerClient pb.ConsumerServiceClient
}

func NewConsumer(groupID string, brokerAddress string) *Consumer {
	consumerID := generateConsumerID()
	s_hb := make(chan bool)
	of := make(map[broker.OffsetKey]int64)
	c := Consumer{id: consumerID, group_id: groupID, stopHeartBeat: s_hb, offsets: of, brokerAddress: brokerAddress}
	c.Dial()
	c.RegisterConsumer(consumerID, groupID)
	c.startHeartbeat()
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

func (c *Consumer) RegisterConsumer(consumerId, groupId string) {
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
}

func (c *Consumer) Poll(timeout time.Duration, opt *pb.ReadOptions) ([]*pb.Message, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req := &pb.FetchMessageRequest{
		Id:      c.id,
		GroupId: c.group_id,
		Opt:     opt,
	}

	res, err := c.brokerClient.HandleFetchMessage(ctx, req)
	if err != nil {
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
	// c.brokerClient.FetchMessages(c.id, c.group_id, opt.ToOpt())
	// select {
	// case messages := <-c.messagesch:
	// 	return messages
	// case <-time.After(timeout):
	// 	log.Println("consumer - timeout")
	// 	return []*pb.Message{}
	// }
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
		c.broker.CommitOffset(c.group_id, tp.Topic, tp.Partition, int(offset)+1)
	}
}

func (c *Consumer) commitOffsetsAsync() {
	go c.commitOffsets()
}

func (c *Consumer) Subscribe(topic string) {
	c.broker.Subscribe(topic, c.group_id)
}

func (c *Consumer) startHeartbeat() {
	c.heartbeatTicker = time.NewTicker(3 * time.Second)
	go func() {
		for {
			select {
			case <-c.heartbeatTicker.C:
				c.broker.ConsumerHandleHeartbeat(c.id, c.group_id)
			case <-c.stopHeartBeat:
				c.heartbeatTicker.Stop()
				return
			}
		}
	}()
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
