package kraft

import (
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	pb "github.com/alexandrecolauto/gofka/proto/controller"
	pr "github.com/alexandrecolauto/gofka/proto/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type KraftServer struct {
	pb.UnimplementedControllerServiceServer
	pr.UnimplementedRaftServiceServer

	controller *KraftController

	Peers            map[string]string
	PeersClients     map[string]pr.RaftServiceClient
	PeersConnections map[string]*grpc.ClientConn

	maxRetries     int
	initialBackoff time.Duration
}

func NewControllerServer(nodeID, address string, peers map[string]string) (*KraftServer, error) {
	s := &KraftServer{
		maxRetries:     10,
		initialBackoff: 250 * time.Millisecond,
	}
	k, err := NewManager(nodeID, address, peers, s.sendAppendEntriesRequest, s.sendVoteRequest)
	if err != nil {
		return nil, err
	}
	pc := make(map[string]pr.RaftServiceClient)
	pcn := make(map[string]*grpc.ClientConn)

	s.controller = k
	s.Peers = peers
	s.PeersClients = pc
	s.PeersConnections = pcn
	segs := strings.Split(address, ":")
	if len(segs) < 2 {
		return nil, fmt.Errorf("invalid address, cannot find port %s, expected localhost:3000", address)
	}
	port := segs[1]
	go s.Start(port)
	s.ConnectGRPC()
	return s, nil
}

func (c *KraftServer) Start(port string) error {
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer()
	pb.RegisterControllerServiceServer(grpcServer, c)
	pr.RegisterRaftServiceServer(grpcServer, c)
	log.Printf("Controller grpc starting on port: %s\n", port)
	return grpcServer.Serve(listener)
}

func (cs *KraftServer) ConnectGRPC() error {
	for peerID, address := range cs.Peers {
		currentBackoff := cs.initialBackoff
		for range cs.maxRetries {
			err := cs.initGRPCConnection(peerID, address)
			if err == nil {
				break
			}
			time.Sleep(currentBackoff)
			currentBackoff *= 2
		}
	}
	cs.controller.raftModule.Start()
	return nil
}

func (c *KraftServer) initGRPCConnection(peerID, address string) error {
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
	return nil
}

func (c *KraftServer) sendVoteRequest(peerID string, req *pr.VoteRequest) (*pr.VoteResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cli, ok := c.PeersClients[peerID]
	if !ok {
		return nil, fmt.Errorf("Cannot find peer: %s", peerID)
	}
	res, err := cli.HandleVoteRequest(ctx, req)
	if err != nil {
		log.Println("err: ", err)
		return nil, err
	}
	return res, nil
}

func (c *KraftServer) sendAppendEntriesRequest(peerID string, req *pr.AppendEntriesRequest) (*pr.AppendEntriesResponse, error) {
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
