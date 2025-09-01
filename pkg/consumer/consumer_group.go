package consumer

import (
	"context"
	"fmt"
	"time"

	pb "github.com/alexandrecolauto/gofka/proto/broker"
	"google.golang.org/grpc"
)

type ConsumerGroup struct {
	id          string
	coordinator Coordinator
}
type Coordinator struct {
	id         string
	address    string
	connection *grpc.ClientConn
	client     pb.ConsumerServiceClient
}

func (c *Consumer) findGroupCoordinator() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := &pb.GroupCoordinatorRequest{
		GroupId: c.group.id,
	}

	res, err := c.group.coordinator.client.HandleGroupCoordinator(ctx, req)
	if err != nil {
		fmt.Println("error fetching msg:", err)
		return err
	}
	if !res.Success {
		return fmt.Errorf(res.ErrorMessage)
	}
	c.group.coordinator.address = res.CoordinatorAddress
	c.group.coordinator.id = res.CoordinatorId
	c.Dial()
	return nil
}

func (c *Consumer) registerConsumer(consumerId, groupId string, topics []string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req := &pb.RegisterConsumerRequest{
		Id:      consumerId,
		GroupId: groupId,
		Topics:  topics,
	}
	res, err := c.group.coordinator.client.HandleRegisterConsumer(ctx, req)
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
		c.syncGroup(updated)
	} else {
		c.syncGroup(nil)
	}
}

func (c *Consumer) assignPartitions(res *pb.RegisterConsumerResponse) ([]*pb.ConsumerSession, error) {
	topics := res.AllTopics
	consumers := res.Consumers
	metadata, err := c.fetchMetadataFor(topics)
	if err != nil {
		return nil, err
	}
	updated_consumers := c.distribuite(consumers, metadata)
	for _, cs := range updated_consumers {
		fmt.Printf("Final consumers: %+v\n", cs)
	}
	return updated_consumers, nil
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

func (c *Consumer) syncGroup(updated []*pb.ConsumerSession) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req := &pb.SyncGroupRequest{
		Id:        c.id,
		GroupId:   c.group.id,
		Consumers: updated,
	}
	res, err := c.group.coordinator.client.HandleSyncGroup(ctx, req)
	if err != nil {
		panic(err)
	}
	if !res.Success {
		panic(res.ErrorMessage)
	}
	c.assignments.session = res.Assignment
	c.connectToBrokers()
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
