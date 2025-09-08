package broker

import (
	"context"
	"fmt"

	pb "github.com/alexandrecolauto/gofka/common/proto/broker"
)

func (s *BrokerServer) HandleSendBatch(ctx context.Context, req *pb.SendBatchRequest) (*pb.SendBatchResponse, error) {
	ack := req.Ack
	switch ack {
	case pb.ACKLevel_ACK_0:
		go s.broker.SendMessageBatch(req.Topic, int(req.Partition), req.Messages)
		res := &pb.SendBatchResponse{
			Success: true,
		}
		return res, nil
	case pb.ACKLevel_ACK_1:
		fmt.Println("sending batch")
		err := s.broker.SendMessageBatch(req.Topic, int(req.Partition), req.Messages)
		if err != nil {
			return nil, err
		}
		res := &pb.SendBatchResponse{
			Success: true,
		}
		return res, nil
	case pb.ACKLevel_ACK_ALL:
		return s.sendAndWaitForAll(ctx, req)
	default:
		fmt.Println("invalid ack level", req.Ack)
		res := &pb.SendBatchResponse{
			Success:  false,
			ErrorMsg: "invalid ack level",
		}
		return res, nil
	}
}

func (s *BrokerServer) HandleCreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	err := s.createTopicController(req.Topic, int(req.Partition), int(req.Replication))
	if err != nil {
		res := &pb.CreateTopicResponse{
			Success:  false,
			ErrorMsg: err.Error(),
		}
		return res, err
	}
	res := &pb.CreateTopicResponse{
		Success: true,
	}
	return res, nil
}

func (s *BrokerServer) HandleFetchMetadataForTopics(ctx context.Context, req *pb.FetchMetadataForTopicsRequest) (*pb.FetchMetadataForTopicsResponse, error) {
	meta, err := s.broker.TopicsMetadata(req.Topics)
	if err != nil {
		return nil, err
	}
	res := &pb.FetchMetadataForTopicsResponse{
		Success:       true,
		TopicMetadata: meta,
	}
	return res, nil
}

func (s *BrokerServer) HandleFetchMessage(ctx context.Context, req *pb.FetchMessageRequest) (*pb.FetchMessageResponse, error) {
	msgs, err := s.broker.FetchMessages(req.Id, req.Topics, req.Opt)
	if err != nil {
		return nil, err
	}
	res := &pb.FetchMessageResponse{
		Success:  true,
		Messages: msgs,
	}
	return res, nil
}

func (s *BrokerServer) HandleConsumerHeartbeat(ctx context.Context, req *pb.ConsumerHeartbeatRequest) (*pb.ConsumerHeartbeatResponse, error) {
	s.broker.ConsumerHandleHeartbeat(req.Id, req.GroupId)
	res := &pb.ConsumerHeartbeatResponse{
		Success: true,
	}
	return res, nil
}

func (s *BrokerServer) HandleGroupCoordinator(ctx context.Context, req *pb.GroupCoordinatorRequest) (*pb.GroupCoordinatorResponse, error) {
	address, id, err := s.broker.GroupCoordinator(req.GroupId)
	if err != nil {
		return nil, err
	}
	res := &pb.GroupCoordinatorResponse{
		Success:            true,
		CoordinatorId:      id,
		CoordinatorAddress: address,
	}
	return res, nil
}

func (s *BrokerServer) HandleRegisterConsumer(ctx context.Context, req *pb.RegisterConsumerRequest) (*pb.RegisterConsumerResponse, error) {
	res := s.broker.RegisterConsumer(req.Id, req.GroupId, req.Topics)
	if res.Leader == req.Id {
		return res, nil
	} else {
		response := &pb.RegisterConsumerResponse{
			Success:      res.Success,
			Leader:       res.Leader,
			ErrorMessage: res.ErrorMessage,
		}
		return response, nil
	}
}

func (s *BrokerServer) HandleSyncGroup(ctx context.Context, req *pb.SyncGroupRequest) (*pb.SyncGroupResponse, error) {
	session, err := s.broker.SyncGroup(req.Id, req.GroupId, req.Consumers)
	if err != nil {
		return nil, err
	}
	res := &pb.SyncGroupResponse{
		Success:    true,
		Assignment: session,
	}
	return res, nil
}

func (s *BrokerServer) HandleCommitOffset(ctx context.Context, req *pb.CommitOffsetRequest) (*pb.CommitOffsetResponse, error) {
	err := s.broker.CommitOffset(req.Topics)
	if err != nil {
		return nil, err
	}
	res := &pb.CommitOffsetResponse{
		Success: true,
	}
	return res, nil
}

func (s *BrokerServer) FetchMetadata(ctx context.Context, req *pb.FetchMetadataRequest) (*pb.FetchMetadataResponse, error) {
	mt := s.broker.clusterMetadata.metadata.FetchMetadata(req.LastIndex)
	res := &pb.FetchMetadataResponse{
		Success: true,
	}
	if mt == nil {
		res.Success = false
		res.ErrorMsg = "already up to date"
		return res, nil

	}
	res.Metadata = mt
	return res, nil
}
