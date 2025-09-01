package consumer

import (
	"context"
	"fmt"
	"time"

	pb "github.com/alexandrecolauto/gofka/proto/broker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Cluster struct {
	connections map[string]*grpc.ClientConn
	clients     map[string]pb.ConsumerServiceClient
	assignments map[string][]*pb.PartitionInfo
}

func (c *Consumer) Dial() {
	conn, err := grpc.NewClient(c.group.coordinator.address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		panic(err)
	}

	c.group.coordinator.connection = conn
	c.group.coordinator.client = pb.NewConsumerServiceClient(conn)
}

func (c *Consumer) fetchMetadataFor(topics []string) ([]*pb.TopicInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := &pb.FetchMetadataForTopicsRequest{
		Topics: topics,
	}

	res, err := c.group.coordinator.client.HandleFetchMetadataForTopics(ctx, req)
	if err != nil {
		fmt.Println("error fetching msg:", err)
		return nil, err
	}

	if !res.Success {
		return nil, fmt.Errorf(res.ErrorMessage)
	}

	return res.TopicMetadata, nil
}

func (c *Consumer) connectToBrokers() error {
	for _, p := range c.assignments.session.Assignments {
		err := c.connectTo(p)
		if err != nil {
			return err
		}
	}
	return nil
}
func (c *Consumer) connectTo(assignment *pb.PartitionInfo) error {
	_, exists := c.cluster.clients[assignment.Leader]
	if exists {
		c.cluster.assignments[assignment.Leader] = append(c.cluster.assignments[assignment.Leader], assignment)
		return nil
	}

	address := assignment.LeaderAddress
	conn, cli, err := c.connect(address)
	if err != nil {
		return err
	}
	c.cluster.connections[assignment.Leader] = conn
	c.cluster.clients[assignment.Leader] = cli
	c.cluster.assignments[assignment.Leader] = append(c.cluster.assignments[assignment.Leader], assignment)
	return nil
}

func (c *Consumer) connect(address string) (*grpc.ClientConn, pb.ConsumerServiceClient, error) {
	conn, err := grpc.NewClient(address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, nil, err
	}

	cli := pb.NewConsumerServiceClient(conn)
	return conn, cli, nil
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
		GroupId: c.group.id,
	}

	for _, cli := range c.cluster.clients {
		res, err := cli.HandleConsumerHeartbeat(ctx, req)
		if err != nil {
			fmt.Println("error sending heartbeat: ", err.Error())
			continue
		}
		if !res.Success {
			fmt.Println("failed response: ", res.ErrorMessage)
			continue
		}

	}

	res, err := c.group.coordinator.client.HandleConsumerHeartbeat(ctx, req)
	if err != nil {
		fmt.Println("error sending heartbeat: ", err.Error())
		return
	}
	if !res.Success {
		fmt.Println("failed response: ", res.ErrorMessage)
		return
	}
}
