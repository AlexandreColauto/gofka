package consumer

import (
	"crypto/rand"
	"fmt"
	"log"
	"time"

	broker "github.com/alexandrecolauto/gofka/pkg/broker"
	pb "github.com/alexandrecolauto/gofka/proto/broker"
)

type Consumer struct {
	id              string
	group_id        string
	broker          *broker.Gofka
	messagesch      chan []*pb.Message
	heartbeatTicker *time.Ticker
	stopHeartBeat   chan bool
	offsets         map[broker.OffsetKey]int64
}

func NewConsumer(groupID string, gf *broker.Gofka) *Consumer {
	consumerID := generateConsumerID()
	m_ch := make(chan []*pb.Message, 100)
	s_hb := make(chan bool)
	of := make(map[broker.OffsetKey]int64)
	gf.RegisterConsumer(consumerID, groupID, m_ch)
	c := Consumer{id: consumerID, broker: gf, messagesch: m_ch, group_id: groupID, stopHeartBeat: s_hb, offsets: of}
	c.startHeartbeat()
	return &c
}

func (c *Consumer) Poll(timeout time.Duration, opt *broker.ReadOpts) []*pb.Message {
	c.broker.FetchMessages(c.id, c.group_id, opt.ToOpt())
	select {
	case messages := <-c.messagesch:
		if len(messages) > 0 {
			log.Println("consumer - got messages: ", len(messages))
			log.Printf("consumer - last message: %+v\n ", messages[len(messages)-1])
			c.updateOffsets(messages)
			c.commitOffsets()
		}
		return messages
	case <-time.After(timeout):
		log.Println("consumer - timeout")
		return []*pb.Message{}
	}
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
