package broker

import (
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/alexandrecolauto/gofka/common/model"
	vC "github.com/alexandrecolauto/gofka/common/pkg/visualizer_client"
	pb "github.com/alexandrecolauto/gofka/common/proto/broker"
	pc "github.com/alexandrecolauto/gofka/common/proto/controller"
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

	tickers ServerTimers

	controller      ControllerClient
	visualizeClient *vC.VisualizerClient
	fenced          bool
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

func NewBrokerServer(controllerAddress, brokerAddress, brokerID string, vc *vC.VisualizerClient) (*BrokerServer, error) {
	brks := make(map[string]pb.IntraBrokerServiceClient)
	brks_conn := make(map[string]*grpc.ClientConn)
	cl := Cluster{
		clients:     brks,
		connections: brks_conn,
	}
	ti := ServerTimers{
		heartbeat: time.NewTicker(250 * time.Millisecond),
		metadata:  time.NewTicker(1 * time.Second),
	}

	bs := &BrokerServer{controllerAddress: controllerAddress, brokerAddress: brokerAddress, brokerID: brokerID, cluster: cl, tickers: ti, visualizeClient: vc}
	broker := NewBroker(brokerID, bs, vc)
	err := bs.registerBroker()
	if err != nil {
		return nil, err
	}

	bs.broker = broker
	err = bs.initGRPCConnection()
	if err != nil {
		return nil, err
	}

	go bs.startHeartbeat()
	go bs.startMetadataFetcher()
	port := strings.Split(brokerAddress, ":")[1]
	go bs.Start(port)
	go bs.monitorLaggingReplicas()

	if vc != nil {
		vc.Processor.RegisterClient(brokerID, bs)
	}
	return bs, nil
}

func (c *BrokerServer) Start(port string) error {
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		c.Stop()
		return err
	}
	grpcServer := grpc.NewServer()
	pb.RegisterIntraBrokerServiceServer(grpcServer, c)
	pb.RegisterProducerServiceServer(grpcServer, c)
	pb.RegisterConsumerServiceServer(grpcServer, c)

	if c.visualizeClient != nil {
		action := "alive"
		target := c.brokerID
		msg := fmt.Sprintf("controller %s just become alive", c.brokerID)
		c.visualizeClient.SendMessage(action, target, []byte(msg))
	}
	return grpcServer.Serve(listener)
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
	b.tickers.heartbeat.Reset(250 * time.Millisecond)
	for range b.tickers.heartbeat.C {
		err := b.sendHeartbeatToController()
		if err != nil {
			//avoid  flooding the output
			// fmt.Println("err sending heartbeat: ", err)
		}

	}
}

func (b *BrokerServer) startMetadataFetcher() {
	b.tickers.metadata.Reset(250 * time.Millisecond)
	for range b.tickers.metadata.C {
		err := b.poolMetadatFromController()
		if err != nil {
			// log.Println("Error metadata: %w", err.Error())
		}
	}
}

func (b *BrokerServer) initGRPCConnection() error {
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
	cli, ok := s.cluster.clients[req.BrokerId]
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
	s.cluster.connections[req.ID] = conn
	s.cluster.clients[req.ID] = pb.NewIntraBrokerServiceClient(conn)

}

func (s *BrokerServer) UpdateFollowerStateRequest(req *pb.UpdateFollowerStateRequest) (*pb.UpdateFollowerStateResponse, error) {
	if s.fenced {
		return nil, fmt.Errorf("fenced")
	}
	cli, ok := s.cluster.clients[req.BrokerId]
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
	s.controllerAddress = address
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
	defer ticker.Stop()
	for range ticker.C {
		p_to_alter := s.broker.findLaggingReplicas()
		s.removeLaggingFormISR(p_to_alter)
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
