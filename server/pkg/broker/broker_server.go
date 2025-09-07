package broker

import (
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/alexandrecolauto/gofka/common/model"
	vC "github.com/alexandrecolauto/gofka/common/pkg/visualizer_client"
	pb "github.com/alexandrecolauto/gofka/common/proto/broker"
	pc "github.com/alexandrecolauto/gofka/common/proto/controller"
	"github.com/alexandrecolauto/gofka/server/pkg/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type BrokerServer struct {
	pb.UnimplementedIntraBrokerServiceServer
	pb.UnimplementedProducerServiceServer
	pb.UnimplementedConsumerServiceServer

	controllerAddress string
	brokerAddress     string
	brokerID          string

	broker *GofkaBroker

	cluster Cluster

	server  Server
	tickers ServerTimers

	controller      ControllerClient
	visualizeClient *vC.VisualizerClient
	fenced          bool

	maxRetries     int
	initialBackoff time.Duration
	mu             sync.RWMutex

	shutdownOnce sync.Once
	shutdownCh   chan any
	wg           sync.WaitGroup
	isShutdown   bool
}

type ServerTimers struct {
	heartbeat *time.Ticker
	metadata  *time.Ticker
}

type Cluster struct {
	clients     map[string]pb.IntraBrokerServiceClient
	connections map[string]*grpc.ClientConn
}

type ControllerClient struct {
	client     pc.ControllerServiceClient
	connection *grpc.ClientConn
}

type Server struct {
	grpc     *grpc.Server
	listener net.Listener
}

func NewBrokerServer(config *config.Config) (*BrokerServer, error) {
	shutdownCh := make(chan any)
	brks := make(map[string]pb.IntraBrokerServiceClient)
	brks_conn := make(map[string]*grpc.ClientConn)
	cl := Cluster{
		clients:     brks,
		connections: brks_conn,
	}
	ti := ServerTimers{
		heartbeat: time.NewTicker(config.Broker.HeartbeatInterval),
		metadata:  time.NewTicker(config.Broker.MetadataInterval),
	}

	bs := &BrokerServer{
		controllerAddress: config.Broker.ControllerAddress,
		brokerAddress:     fmt.Sprintf("%s:%d", config.Server.Address, config.Server.Port),
		brokerID:          config.Server.NodeID,
		cluster:           cl,
		tickers:           ti,
		maxRetries:        config.Server.MaxRetries * 2,
		initialBackoff:    config.Server.InitialBackoff,
		shutdownCh:        shutdownCh,
	}
	if config.Visualizer.Enabled {
		bs.createVisualizerClient(config.Visualizer.Address)
	}
	broker := NewBroker(config, bs, bs.visualizeClient, shutdownCh)

	bs.broker = broker

	bs.wg.Add(2)
	go bs.startHeartbeat()
	// go bs.Start(fmt.Sprintf("%d", config.Broker.Port))
	go bs.monitorLaggingReplicas()

	if bs.visualizeClient != nil {
		bs.visualizeClient.Processor.RegisterClient(config.Broker.BrokerID, bs)
	}
	return bs, nil
}

func (c *BrokerServer) Register(grpcServer *grpc.Server) error {
	pb.RegisterIntraBrokerServiceServer(grpcServer, c)
	pb.RegisterProducerServiceServer(grpcServer, c)
	pb.RegisterConsumerServiceServer(grpcServer, c)

	c.server.grpc = grpcServer

	return nil
}

func (c *BrokerServer) Connect() error {
	return c.initGRPCConnection()
}

func (cs *BrokerServer) initGRPCConnection() error {
	currentBackoff := cs.initialBackoff
	for range cs.maxRetries {
		err := cs._initGRPCConnection()
		if err == nil {
			break
		}
		time.Sleep(currentBackoff)
		currentBackoff *= 2
	}
	currentBackoff = cs.initialBackoff
	for range cs.maxRetries {
		err := cs.registerBroker()
		if err == nil {
			break
		}
		time.Sleep(currentBackoff)
		currentBackoff *= 2
		fmt.Println(currentBackoff, err)
	}
	cs.wg.Add(1)
	go cs.startMetadataFetcher()
	if cs.visualizeClient != nil {
		action := "alive"
		target := cs.brokerID
		msg := fmt.Sprintf("controller %s just become alive", cs.brokerID)
		cs.visualizeClient.SendMessage(action, target, []byte(msg))
	}
	fmt.Println("Broker connected")
	return nil
}

func (c *BrokerServer) FetchRecords(ctx context.Context, req *pb.FetchRecordsRequest) (*pb.FetchRecordsResponse, error) {
	opt := &pb.ReadOptions{
		MaxBytes: req.MaxBytes,
	}
	res, err := c.broker.FetchMessagesReplica(req.Topic, int(req.Partition), req.Offset, opt)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (c *BrokerServer) UpdateFollowerState(ctx context.Context, req *pb.UpdateFollowerStateRequest) (*pb.UpdateFollowerStateResponse, error) {
	err := c.broker.UpdateFollowerState(req.Topic, req.FollowerId, int(req.Partition), req.FetchOffset, req.LongEndOffset)
	if err != nil {
		return nil, err
	}
	res := &pb.UpdateFollowerStateResponse{
		Success: true,
	}
	return res, nil
}

func (c *BrokerServer) Stop() {
	c.tickers.heartbeat.Stop()
	c.tickers.metadata.Stop()
	if c.controller.client != nil {
		c.controller.connection.Close()
		c.controller.client = nil
	}
	for _, conn := range c.cluster.connections {
		conn.Close()
	}
}
func (b *BrokerServer) startHeartbeat() {
	defer b.wg.Done()
	b.tickers.heartbeat.Reset(250 * time.Millisecond)
	for {
		select {
		case <-b.tickers.heartbeat.C:
			err := b.sendHeartbeatToController()
			if err != nil {
				//avoid  flooding the output
				// fmt.Println("err sending heartbeat: ", err)
			}
		case <-b.shutdownCh:
			return
		}

	}
}

func (b *BrokerServer) startMetadataFetcher() {
	defer b.wg.Done()
	b.tickers.metadata.Reset(250 * time.Millisecond)
	for {
		select {
		case <-b.tickers.metadata.C:
			err := b.poolMetadatFromController()
			if err != nil {
				// log.Println("Error metadata: %w", err.Error())
			}
		case <-b.shutdownCh:
			return
		}
	}
}

func (b *BrokerServer) _initGRPCConnection() error {
	fmt.Println("connecting to :", b.controllerAddress)
	conn, err := grpc.NewClient(
		b.controllerAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		fmt.Println("error connecting to grpc controller ", err)
		return err
	}
	b.controller.connection = conn
	b.controller.client = pc.NewControllerServiceClient(conn)
	return nil
}

func (s *BrokerServer) FetchRecordsRequest(req *pb.FetchRecordsRequest) (*pb.FetchRecordsResponse, error) {
	if s.fenced {
		return nil, fmt.Errorf("fenced")
	}
	s.mu.RLock()
	cli, ok := s.cluster.clients[req.BrokerId]
	s.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("Cannot find broker %s", req.BrokerId)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := cli.FetchRecords(ctx, req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (s *BrokerServer) UpdateBroker(req model.BrokerInfo) {
	conn, err := grpc.NewClient(
		req.Address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		fmt.Println("error connecting to grpc controller ", err)
		return
	}
	s.mu.Lock()
	s.cluster.connections[req.ID] = conn
	s.cluster.clients[req.ID] = pb.NewIntraBrokerServiceClient(conn)
	s.mu.Unlock()

}

func (s *BrokerServer) UpdateFollowerStateRequest(req *pb.UpdateFollowerStateRequest) (*pb.UpdateFollowerStateResponse, error) {
	if s.fenced {
		return nil, fmt.Errorf("fenced")
	}
	s.mu.RLock()
	cli, ok := s.cluster.clients[req.BrokerId]
	s.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("Cannot find broker %s", req.BrokerId)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := cli.UpdateFollowerState(ctx, req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (s *BrokerServer) registerBroker() error {
	if s.controller.client == nil {
		if err := s.initGRPCConnection(); err != nil {
			return err
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := &pc.BrokerRegisterRequest{
		Id:      s.brokerID,
		Address: s.brokerAddress,
	}

	response, err := s.controller.client.HandleRegisterBroker(ctx, req)
	if err != nil {
		return s.checkErrAndRedirect(err, s.registerBroker)
	}
	if response != nil && !response.Success {
		return fmt.Errorf(response.ErrorMessage)
	}
	return nil
}

func (s *BrokerServer) checkErrAndRedirect(err error, fun func() error) error {
	if st, ok := status.FromError(err); ok {
		if st.Code() == codes.FailedPrecondition {
			parts := strings.Split(st.Message(), "|")
			if len(parts) == 3 && parts[0] == "not leader" {
				leaderID := parts[1]
				leaderAddr := parts[2]

				log.Printf("Redirecting to leader %s at %s", leaderID, leaderAddr)
				s.updateLeaderAddress(leaderAddr)
				return fun()
			}
		}
	}
	return err
}

func (s *BrokerServer) updateLeaderAddress(address string) {
	s.mu.Lock()
	s.controllerAddress = address
	s.mu.Unlock()
	s.controller.connection.Close()
	s.initGRPCConnection()
}

func (s *BrokerServer) createTopicController(topic string, n_partitions, replication_factor int) error {
	if s.controller.client == nil {
		if err := s.initGRPCConnection(); err != nil {
			return err
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req := &pc.CreateTopicRequest{
		Topic:             topic,
		NPartitions:       int32(n_partitions),
		ReplicationFactor: int32(replication_factor),
	}

	res, err := s.controller.client.HandleCreateTopic(ctx, req)
	if err != nil {
		fn := func() error { return s.createTopicController(topic, n_partitions, replication_factor) }
		return s.checkErrAndRedirect(err, fn)
	}
	if !res.Success {
		return fmt.Errorf("Failed to create topic in controller")
	}

	return nil
}

func (s *BrokerServer) sendHeartbeatToController() error {
	if s.fenced {
		return fmt.Errorf("fenced")
	}
	if s.controller.client == nil {
		if err := s.initGRPCConnection(); err != nil {
			return err
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	request := &pc.BrokerHeartbeatRequest{
		BrokerId: s.brokerID,
	}

	res, err := s.controller.client.HandleBrokerHeartbeat(ctx, request)
	if err != nil {
		return s.checkErrAndRedirect(err, s.sendHeartbeatToController)
	}

	if !res.Success {
		return fmt.Errorf("Failed to fetch metadata: %s", res.ErrorMessage)
	}

	return nil
}

func (s *BrokerServer) poolMetadatFromController() error {
	if s.fenced {
		return fmt.Errorf("fenced")
	}
	if s.controller.client == nil {
		if err := s.initGRPCConnection(); err != nil {
			return err
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	request := &pc.BrokerMetadataRequest{
		BrokerId: s.brokerID,
		Index:    s.broker.clusterMetadata.index + 1,
	}
	response, err := s.controller.client.HandleFetchMetadata(ctx, request)
	if err != nil {
		return s.checkErrAndRedirect(err, s.poolMetadatFromController)
	}

	if !response.Success {
		return fmt.Errorf("Failed to fetch metadata: %s", response.ErrorMessage)
	}
	s.broker.ProcessControllerLogs(response.Logs)
	return nil
}

func (s *BrokerServer) sendAndWaitForAll(ctx context.Context, req *pb.SendBatchRequest) (*pb.SendBatchResponse, error) {
	err := s.broker.SendMessageBatchAndWaitForReplicas(req.Topic, int(req.Partition), req.Messages)
	if err != nil {
		return nil, err
	}
	res := &pb.SendBatchResponse{
		Success: true,
	}
	return res, nil
}

func (s *BrokerServer) monitorLaggingReplicas() {
	ticker := time.NewTicker(2 * time.Second)
	defer s.wg.Done()
	defer ticker.Stop()
	for {
		select {
		case <-s.shutdownCh:
			return
		case <-ticker.C:
			p_to_alter := s.broker.findLaggingReplicas()
			s.removeLaggingFormISR(p_to_alter)
		}
	}
}

func (s *BrokerServer) removeLaggingFormISR(alter []*pc.AlterPartition) {
	if len(alter) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := &pc.AlterPartitionRequest{
		Changes: alter,
	}
	res, err := s.controller.client.HandleAlterPartition(ctx, req)
	if err != nil {
		fmt.Println("error removing from isr ", err)
	}

	if res != nil && !res.Success {
		fmt.Println("error removing from isr ", res.ErrorMessage)
	}
}

func (s *BrokerServer) GetClientId() string {
	return s.brokerID
}

func (s *BrokerServer) Fence() error {
	fmt.Println("Fencing node: ", s.GetClientId())
	s.fenced = true
	if s.visualizeClient != nil {
		action := "fenced"
		target := s.GetClientId()
		msg := fmt.Sprintf("controller %s just become alive", target)
		s.visualizeClient.SendMessage(action, target, []byte(msg))
	}

	time.AfterFunc(10*time.Second, func() {
		s.fenced = false
		if s.visualizeClient != nil {
			action := "fenced-removed"
			target := s.GetClientId()
			msg := fmt.Sprintf("controller %s just become alive", target)
			s.visualizeClient.SendMessage(action, target, []byte(msg))
		}
	})
	return nil
}
func (c *BrokerServer) createVisualizerClient(address string) {
	nodeType := "broker"
	viCli := vC.NewVisualizerClient(nodeType, address)
	c.visualizeClient = viCli
}

func (c *BrokerServer) Shutdown() error {
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

		c.broker.Shutdown()
		c.stopTimers()

		if c.controller.connection != nil {
			c.controller.connection.Close()
		}
		for _, conn := range c.cluster.connections {
			conn.Close()
		}

	})
	return shutErr
}
func (c *BrokerServer) stopTimers() {
	if c.tickers.metadata != nil {
		c.tickers.metadata.Stop()
	}
	if c.tickers.heartbeat != nil {
		c.tickers.heartbeat.Stop()
	}
}
