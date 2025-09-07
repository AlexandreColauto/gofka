package kraft

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	vC "github.com/alexandrecolauto/gofka/common/pkg/visualizer_client"
	pb "github.com/alexandrecolauto/gofka/common/proto/controller"
	pr "github.com/alexandrecolauto/gofka/common/proto/raft"
	"github.com/alexandrecolauto/gofka/server/pkg/config"
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

	maxRetries       int
	initialBackoff   time.Duration
	visualizerClient *vC.VisualizerClient
	fenced           bool

	server Server

	shutdownOnce sync.Once
	shutdownCh   chan any
	wg           sync.WaitGroup
	isShutdown   bool
}

type Server struct {
	grpc     *grpc.Server
	listener net.Listener
}

func NewControllerServer(config *config.Config) (*KraftServer, error) {
	shutdownCh := make(chan any)
	s := &KraftServer{
		maxRetries:     config.Server.MaxRetries,
		initialBackoff: config.Server.InitialBackoff,
		shutdownCh:     shutdownCh,
	}
	if config.Visualizer.Enabled {
		s.createVisualizerClient(config.Visualizer.Address)
	}

	k, err := NewManager(config, shutdownCh, s.sendAppendEntriesRequest, s.sendVoteRequest, s.visualizerClient)
	if err != nil {
		return nil, err
	}
	pc := make(map[string]pr.RaftServiceClient)
	pcn := make(map[string]*grpc.ClientConn)

	if s.visualizerClient != nil {
		s.visualizerClient.Processor.RegisterClient(config.Server.NodeID, s)
	}

	s.controller = k
	s.Peers = config.Server.Cluster.Peers
	s.PeersClients = pc
	s.PeersConnections = pcn
	return s, nil
}

func (c *KraftServer) Register(grpcServer *grpc.Server) error {
	pb.RegisterControllerServiceServer(grpcServer, c)
	pr.RegisterRaftServiceServer(grpcServer, c)

	c.server.grpc = grpcServer

	return nil
}

func (cs *KraftServer) ConnectGRPC() error {
	for peerID, address := range cs.Peers {
		if peerID == cs.controller.ID() {
			continue
		}
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
	if cs.visualizerClient != nil {
		action := "alive"
		target := cs.controller.ID()
		msg := fmt.Sprintf("controller %s just become alive", cs.controller.ID())
		cs.visualizerClient.SendMessage(action, target, []byte(msg))
	}
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
	if c.fenced {
		return nil, fmt.Errorf("fenced")
	}
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
	if c.fenced {
		return nil, fmt.Errorf("fenced")
	}
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

func (c *KraftServer) createVisualizerClient(address string) {
	nodeType := "controller"
	viCli := vC.NewVisualizerClient(nodeType, address)
	c.visualizerClient = viCli
}

func (c *KraftServer) GetClientId() string {
	return c.controller.ID()
}

func (c *KraftServer) Fence() error {
	fmt.Println("Fencing node: ", c.controller.ID())
	c.fenced = true
	if c.visualizerClient != nil {
		action := "fenced"
		target := c.controller.ID()
		msg := fmt.Sprintf("controller %s just become alive", c.controller.ID())
		c.visualizerClient.SendMessage(action, target, []byte(msg))
	}

	time.AfterFunc(5*time.Second, func() {
		c.fenced = false
		if c.visualizerClient != nil {
			action := "fenced-removed"
			target := c.controller.ID()
			msg := fmt.Sprintf("controller %s just become alive", c.controller.ID())
			c.visualizerClient.SendMessage(action, target, []byte(msg))
		}
	})
	return nil
}

func (c *KraftServer) Shutdown() error {
	fmt.Println("Shut down Kraft Server")
	var shutErr error
	c.shutdownOnce.Do(func() {
		if c.isShutdown {
			return
		}
		c.isShutdown = true
		grpcServer := c.server.grpc
		listener := c.server.listener

		close(c.shutdownCh)

		done := make(chan any)

		go func() {
			c.wg.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			shutErr = fmt.Errorf("timeout: some goroutines didin't finish within 5 seconds")
		}

		if grpcServer != nil {
			stopped := make(chan any)
			go func() {
				grpcServer.GracefulStop()
				close(stopped)
			}()

			select {
			case <-stopped:
				log.Println("gRPC gracefully stopped")
			case <-time.After(5 * time.Second):
				grpcServer.Stop()
				shutErr = fmt.Errorf("timeout: forced gRPC stop")
			}
		}

		if listener != nil {
			if err := listener.Close(); err != nil {
				if shutErr == nil {
					shutErr = err
				}
			}
		}

		c.controller.Shutdown()
		for _, peerConn := range c.PeersConnections {
			peerConn.Close()
		}

	})
	return shutErr
}
