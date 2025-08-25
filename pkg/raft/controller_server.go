package raft

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	pb "github.com/alexandrecolauto/gofka/proto/broker"
	pr "github.com/alexandrecolauto/gofka/proto/raft"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ControllerServer struct {
	pb.UnimplementedBrokerServiceServer
	pr.UnimplementedRaftServiceServer

	Controller       *RaftController
	Peers            map[string]string
	PeersClients     map[string]pr.RaftServiceClient
	PeersConnections map[string]*grpc.ClientConn
}

func NewControllerServer(nodeID, address string, peers map[string]string) *ControllerServer {
	c := NewController(nodeID, address, peers)
	pc := make(map[string]pr.RaftServiceClient)
	pcn := make(map[string]*grpc.ClientConn)
	cs := &ControllerServer{
		Controller:       c,
		Peers:            peers,
		PeersClients:     pc,
		PeersConnections: pcn,
	}
	c.Raft.setSendVoteRequest(cs.sendVoteRequest)
	c.Raft.setSendAppendEntriesRequest(cs.sendAppendEntriesRequest)
	return cs
}

func (cs *ControllerServer) ConnectGRPC() error {
	for peerID, address := range cs.Peers {
		err := cs.initGRPCConnection(peerID, address)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *ControllerServer) initGRPCConnection(peerID, address string) error {
	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		fmt.Println("error connecting to grpc controller ", err)
		return err
	}
	c.PeersConnections[peerID] = conn
	c.PeersClients[peerID] = pr.NewRaftServiceClient(conn)
	log.Println("adding client to: ", peerID)
	return nil
}

func (c *ControllerServer) Start(port string) error {
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer()
	pb.RegisterBrokerServiceServer(grpcServer, c)
	pr.RegisterRaftServiceServer(grpcServer, c)
	log.Printf("Controller grpc starting on port: %s\n", port)
	return grpcServer.Serve(listener)
}

func (c *ControllerServer) HandleVoteRequest(ctx context.Context, req *pr.VoteRequest) (*pr.VoteResponse, error) {
	resp := c.Controller.Raft.processVoteRequest(req)
	return resp, nil
}

func (c *ControllerServer) HandleAppendEntries(ctx context.Context, req *pr.AppendEntriesRequest) (*pr.AppendEntriesResponse, error) {
	resp := c.Controller.Raft.processAppendRequest(req)
	return resp, nil
}

func (c *ControllerServer) sendVoteRequest(peerID string, req *pr.VoteRequest) (*pr.VoteResponse, error) {
	log.Println(c.Controller.NodeId, " asking for vote to ", peerID)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cli, ok := c.PeersClients[peerID]
	if !ok {
		log.Println("Cannot find peer")
		return nil, fmt.Errorf("Cannot find peer: %s", peerID)
	}
	res, err := cli.HandleVoteRequest(ctx, req)
	if err != nil {
		log.Println("err: ", err)
		return nil, err
	}
	log.Println("Res: ", res)
	return res, nil
}

func (c *ControllerServer) sendAppendEntriesRequest(peerID string, req *pr.AppendEntriesRequest) (*pr.AppendEntriesResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cli, ok := c.PeersClients[peerID]
	if !ok {
		return nil, fmt.Errorf("Cannot find peer: %s", peerID)
	}
	res, err := cli.HandleAppendEntries(ctx, req)
	if err != nil {
		return nil, err
	}
	return res, nil
}
func (c *ControllerServer) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
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
	err := c.Controller.submitCommandGRPC(cmd)
	if err != nil {
		return nil, err
	}
	resp := &pb.CreateTopicResponse{
		Success: true,
	}
	return resp, nil
}

func (c *ControllerServer) BrokerHeartbeat(ctx context.Context, req *pb.BrokerHeartbeatRequest) (*pb.BrokerHeartbeatResponse, error) {
	err := c.Controller.brokerHeartbeat(req.BrokerId)
	if err != nil {
		return nil, err
	}
	resp := &pb.BrokerHeartbeatResponse{
		Success: true,
	}
	return resp, nil
}
func (c *ControllerServer) FetchMetadata(ctx context.Context, req *pb.BrokerMetadataRequest) (*pb.BrokerMetadataResponse, error) {
	logs, err := c.Controller.brokerMetadata(req.Index)
	if err != nil {
		return nil, err
	}
	last := logs[len(logs)-1]
	response := &pb.BrokerMetadataResponse{
		Success:       true,
		MetadataIndex: last.Index,
		Logs:          logs,
	}
	return response, nil
}

func (c *ControllerServer) RegisterBroker(ctx context.Context, req *pb.BrokerRegisterRequest) (*pb.BrokerRegisterResponse, error) {
	log.Println("Registering new broker gRPC")
	err := c.Controller.RegisterBroker(req)
	if err != nil {
		return nil, err
	}
	response := &pb.BrokerRegisterResponse{
		Success: true,
	}
	return response, nil
}
