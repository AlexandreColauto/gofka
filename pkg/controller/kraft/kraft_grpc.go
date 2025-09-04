package kraft

import (
	"context"
	"fmt"
	"slices"

	pc "github.com/alexandrecolauto/gofka/proto/controller"
	pr "github.com/alexandrecolauto/gofka/proto/raft"
)

func (c *KraftServer) HandleVoteRequest(ctx context.Context, req *pr.VoteRequest) (*pr.VoteResponse, error) {
	if c.fenced {
		return nil, fmt.Errorf("fenced")
	}
	resp := c.controller.raftModule.ProcessVoteRequest(req)
	return resp, nil
}

func (c *KraftServer) HandleAppendEntries(ctx context.Context, req *pr.AppendEntriesRequest) (*pr.AppendEntriesResponse, error) {
	if c.fenced {
		return nil, fmt.Errorf("fenced")
	}
	resp := c.controller.raftModule.ProcessAppendRequest(req)
	return resp, nil
}

func (c *KraftServer) HandleCreateTopic(ctx context.Context, req *pc.CreateTopicRequest) (*pc.CreateTopicResponse, error) {
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
	pyld := &pc.CreateTopicCommand{
		Topic:             req.Topic,
		NPartitions:       req.NPartitions,
		ReplicationFactor: req.ReplicationFactor,
	}
	cmd := &pc.Command{
		Type: pc.Command_CREATE_TOPIC,
		Payload: &pc.Command_CreateTopic{
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
	resp := &pc.CreateTopicResponse{
		Success: true,
	}
	return resp, nil
}

func (c *KraftServer) HandleBrokerHeartbeat(ctx context.Context, req *pc.BrokerHeartbeatRequest) (*pc.BrokerHeartbeatResponse, error) {
	err := c.controller.brokerHeartbeat(req.BrokerId)
	if err != nil {
		return nil, err
	}
	resp := &pc.BrokerHeartbeatResponse{
		Success: true,
	}
	return resp, nil
}

func (c *KraftServer) HandleFetchMetadata(ctx context.Context, req *pc.BrokerMetadataRequest) (*pc.BrokerMetadataResponse, error) {
	logs, err := c.controller.brokerMetadata(req.Index)
	if err != nil {
		return nil, err
	}
	if len(logs) > 0 {
		last := logs[len(logs)-1]
		response := &pc.BrokerMetadataResponse{
			Success:       true,
			MetadataIndex: last.Index,
			Logs:          logs,
		}
		return response, nil
	}
	response := &pc.BrokerMetadataResponse{
		Success:       true,
		MetadataIndex: req.Index,
		Logs:          logs,
	}
	return response, nil
}

func (c *KraftServer) HandleRegisterBroker(ctx context.Context, req *pc.BrokerRegisterRequest) (*pc.BrokerRegisterResponse, error) {
	err := c.controller.registerBroker(req)
	if err != nil {
		return nil, err
	}
	response := &pc.BrokerRegisterResponse{
		Success: true,
	}
	return response, nil
}

func (c *KraftServer) HandleAlterPartition(ctx context.Context, req *pc.AlterPartitionRequest) (*pc.AlterPartitionResponse, error) {
	changes := c.filterChanges(req.Changes)
	if len(changes) == 0 {
		resp := &pc.AlterPartitionResponse{
			Success: true,
		}
		return resp, nil
	}
	// fmt.Println("Applyng changes: ", changes)
	pyld := &pc.AlterPartitionCommand{
		Changes: changes,
	}
	cmd := &pc.Command{
		Type: pc.Command_ALTER_PARTITION,
		Payload: &pc.Command_AlterPartition{
			AlterPartition: pyld,
		},
	}
	err := c.controller.SubmitCommandGRPC(cmd)
	if err != nil {
		return nil, err
	}
	resp := &pc.AlterPartitionResponse{
		Success: true,
	}
	return resp, nil
}

func (c *KraftServer) filterChanges(changes []*pc.AlterPartition) []*pc.AlterPartition {
	changed := []*pc.AlterPartition{}
	for _, change := range changes {
		topic := change.Topic
		part := change.Partition
		newIsr := change.NewIsr
		if c.hasChanges(topic, int32(part), newIsr) {
			changed = append(changed, change)
		}
	}
	return changed
}

func (c *KraftServer) hasChanges(topic string, partition int32, newIsr []string) bool {
	t, ok := c.controller.clusterMetadata.Topic(topic)
	if !ok {
		return false
	}
	p, ok := t.Partitions[partition]
	if !ok {
		return false
	}
	old := make([]string, len(p.Isr))
	n := make([]string, len(newIsr))
	copy(old, p.Isr)
	copy(n, newIsr)
	slices.Sort(old)
	slices.Sort(n)
	changes := !slices.Equal(old, n)
	if changes {
		fmt.Printf("found changing isr old: %v, new: %v\n", old, n)
	}
	return changes
}
