package kraft

import (
	"context"
	"fmt"

	pb "github.com/alexandrecolauto/gofka/proto/controller"
	pr "github.com/alexandrecolauto/gofka/proto/raft"
)

func (c *KraftServer) HandleVoteRequest(ctx context.Context, req *pr.VoteRequest) (*pr.VoteResponse, error) {
	resp := c.controller.raftModule.ProcessVoteRequest(req)
	return resp, nil
}

func (c *KraftServer) HandleAppendEntries(ctx context.Context, req *pr.AppendEntriesRequest) (*pr.AppendEntriesResponse, error) {
	resp := c.controller.raftModule.ProcessAppendRequest(req)
	return resp, nil
}

func (c *KraftServer) HandleCreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	if req.Topic == "" {
		return nil, fmt.Errorf("topic cannot be blank")
	}

	if req.NPartitions <= 0 {
		req.NPartitions = 1
	}

	topics := c.controller.clusterMetadata.Topics()
	_, exists := topics[req.Topic]
	if exists {
		return nil, fmt.Errorf("topic already exists %s", req.Topic)
	}
	pyld := &pb.CreateTopicCommand{
		Topic:             req.Topic,
		NPartitions:       req.NPartitions,
		ReplicationFactor: req.ReplicationFactor,
	}
	cmd := &pb.Command{
		Type: pb.Command_CREATE_TOPIC,
		Payload: &pb.Command_CreateTopic{
			CreateTopic: pyld,
		},
	}
	if len(c.controller.clusterMetadata.Brokers()) < int(req.ReplicationFactor) {
		return nil, fmt.Errorf("invalid replication factor, cannot be greater than available nodes")
	}
	err := c.controller.SubmitCommandGRPC(cmd)
	if err != nil {
		return nil, err
	}
	resp := &pb.CreateTopicResponse{
		Success: true,
	}
	return resp, nil
}

func (c *KraftServer) HandleBrokerHeartbeat(ctx context.Context, req *pb.BrokerHeartbeatRequest) (*pb.BrokerHeartbeatResponse, error) {
	err := c.controller.brokerHeartbeat(req.BrokerId)
	if err != nil {
		return nil, err
	}
	resp := &pb.BrokerHeartbeatResponse{
		Success: true,
	}
	return resp, nil
}
func (c *KraftServer) HandleFetchMetadata(ctx context.Context, req *pb.BrokerMetadataRequest) (*pb.BrokerMetadataResponse, error) {
	logs, err := c.controller.brokerMetadata(req.Index)
	if err != nil {
		return nil, err
	}
	if len(logs) > 0 {
		last := logs[len(logs)-1]
		response := &pb.BrokerMetadataResponse{
			Success:       true,
			MetadataIndex: last.Index,
			Logs:          logs,
		}
		return response, nil
	}
	response := &pb.BrokerMetadataResponse{
		Success:       true,
		MetadataIndex: req.Index,
		Logs:          logs,
	}
	return response, nil
}

func (c *KraftServer) HandleRegisterBroker(ctx context.Context, req *pb.BrokerRegisterRequest) (*pb.BrokerRegisterResponse, error) {
	err := c.controller.registerBroker(req)
	if err != nil {
		return nil, err
	}
	response := &pb.BrokerRegisterResponse{
		Success: true,
	}
	return response, nil
}
