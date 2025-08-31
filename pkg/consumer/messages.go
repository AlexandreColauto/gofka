package consumer

import (
	"context"
	"fmt"
	"time"

	pb "github.com/alexandrecolauto/gofka/proto/broker"
)

func (c *Consumer) fetchMessages(brokerID string, topics []*pb.FromTopic, timeout time.Duration, opt *pb.ReadOptions) ([]*pb.Message, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req := &pb.FetchMessageRequest{
		Id:     c.id,
		Topics: topics,
		Opt:    opt,
	}
	cli, ok := c.cluster.clients[brokerID]
	if !ok {
		return nil, fmt.Errorf("cannot find broker %s", brokerID)
	}

	res, err := cli.HandleFetchMessage(ctx, req)
	if err != nil {
		fmt.Println("error fetching msg:", err)
		return nil, err
	}
	if !res.Success {
		return nil, fmt.Errorf(res.ErrorMessage)
	}
	if len(res.Messages) > 0 {
		c.updateOffsets(res.Messages)
		// c.commitOffsets()
	}
	return res.Messages, nil
}

func (c *Consumer) updateOffsets(messages []*pb.Message) {
	for _, msg := range messages {
		for _, assignments := range c.cluster.assignments {
			for _, ass := range assignments {
				if ass.TopicName == msg.Topic && ass.Id == msg.Partition {
					if msg.Offset > ass.Offset {
						ass.Offset = msg.Offset
					}
				}
			}
		}
	}
}

func (c *Consumer) commitOffset(brokerID string, topics []*pb.FromTopic) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req := &pb.CommitOffsetRequest{
		Id:      c.id,
		GroupId: c.group.id,
		Topics:  topics,
	}
	cli, ok := c.cluster.clients[brokerID]
	if !ok {
		return fmt.Errorf("cannot find broker %s", brokerID)
	}

	res, err := cli.HandleCommitOffset(ctx, req)
	if err != nil {
		fmt.Println("error fetching msg:", err)
		return err
	}
	if !res.Success {
		return fmt.Errorf(res.ErrorMessage)
	}
	c.applyCommitOffests(topics)
	return nil
}

func (c *Consumer) applyCommitOffests(topics []*pb.FromTopic) {
	for _, assignments := range c.cluster.assignments {
		for _, assign := range assignments {
			for _, topic := range topics {
				if topic.Topic == assign.TopicName && topic.Partition == assign.Id {
					assign.CommitedOffset = topic.Offset
				}
			}
		}
	}
}
