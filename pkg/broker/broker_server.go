package broker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/alexandrecolauto/gofka/model"
	pb "github.com/alexandrecolauto/gofka/proto/broker"
	pc "github.com/alexandrecolauto/gofka/proto/controller"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type BrokerServer struct {
	pb.UnimplementedIntraBrokerServiceServer
	pb.UnimplementedProducerServiceServer

	Broker *Gofka

	hb_ticker *time.Ticker
	md_ticker *time.Ticker

	controllerAddress string
	brokerAddress     string
	brokerID          string

	brokers     map[string]pb.IntraBrokerServiceClient
	brokersConn map[string]*grpc.ClientConn

	grpcControllerClient pc.ControllerServiceClient
	grpcControllerConn   *grpc.ClientConn
}

func NewBrokerServer(controllerAddress, brokerAddress, brokerID string) (*BrokerServer, error) {
	brks := make(map[string]pb.IntraBrokerServiceClient)
	brks_conn := make(map[string]*grpc.ClientConn)
	bs := &BrokerServer{controllerAddress: controllerAddress, brokerAddress: brokerAddress, brokerID: brokerID, brokers: brks, brokersConn: brks_conn}
	b := NewGofka(brokerID, bs)
	bs.Broker = b
	err := bs.registerBroker()
	if err != nil {
		return nil, err
	}
	bs.hb_ticker = time.NewTicker(250 * time.Millisecond)
	bs.md_ticker = time.NewTicker(1 * time.Second)
	go bs.startHeartbeat()
	go bs.startMetadataFetcher()
	bs.initGRPCConnection()
	port := strings.Split(brokerAddress, ":")[1]
	go bs.Start(port)
	return bs, nil
}

func (c *BrokerServer) Start(port string) error {
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer()
	pb.RegisterIntraBrokerServiceServer(grpcServer, c)
	pb.RegisterProducerServiceServer(grpcServer, c)
	log.Printf("IntraBroker grpc starting on port: %s\n", port)
	return grpcServer.Serve(listener)
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
	b.grpcControllerConn = conn
	b.grpcControllerClient = pc.NewControllerServiceClient(conn)
	return nil
}

func (b *BrokerServer) startMetadataFetcher() {
	b.md_ticker.Reset(1 * time.Second)
	for {
		select {
		case <-b.md_ticker.C:
			err := b.fetchMetadata()
			if err != nil {
				log.Println("Error metadata: %w", err.Error())
			}
		}
	}
}

func (b *BrokerServer) startHeartbeat() {
	b.hb_ticker.Reset(250 * time.Millisecond)
	for {
		select {
		case <-b.hb_ticker.C:
			b.sendHeartbeat()
		}
	}
}

func (s *BrokerServer) sendHeartbeat() error {
	if s.grpcControllerClient == nil {
		if err := s.initGRPCConnection(); err != nil {
			return err
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	request := &pc.BrokerHeartbeatRequest{
		BrokerId: s.brokerID,
	}

	res, err := s.grpcControllerClient.BrokerHeartbeat(ctx, request)
	if err != nil {
		return s.checkErrAndRedirect(err, s.sendHeartbeat)
	}

	if !res.Success {
		return fmt.Errorf("Failed to fetch metadata: %s", res.ErrorMessage)
	}

	return nil
}

func (s *BrokerServer) fetchMetadata() error {
	if s.grpcControllerClient == nil {
		if err := s.initGRPCConnection(); err != nil {
			return err
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	request := &pc.BrokerMetadataRequest{
		BrokerId: s.brokerID,
		Index:    s.Broker.MetadataIndex + 1,
	}
	response, err := s.grpcControllerClient.FetchMetadata(ctx, request)
	if err != nil {
		return s.checkErrAndRedirect(err, s.fetchMetadata)
	}

	if !response.Success {
		return fmt.Errorf("Failed to fetch metadata: %s", response.ErrorMessage)
	}
	s.Broker.ProcessControllerLogs(response.Logs)
	return nil
}

func (s *BrokerServer) registerBroker() error {
	if s.grpcControllerClient == nil {
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

	response, err := s.grpcControllerClient.RegisterBroker(ctx, req)
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
			// Parse the error message for leader info
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
	s.grpcControllerConn.Close()
	s.initGRPCConnection()
}

func (s *BrokerServer) FetchRecordsRequest(req *pb.FetchRecordsRequest) (*pb.FetchRecordsResponse, error) {
	cli, ok := s.brokers[req.BrokerId]
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
	s.brokersConn[req.ID] = conn
	s.brokers[req.ID] = pb.NewIntraBrokerServiceClient(conn)
	log.Printf("new broker added: %+v\n", req)

}

func (s *BrokerServer) UpdateFollowerStateRequest(req *pb.UpdateFollowerStateRequest) (*pb.UpdateFollowerStateResponse, error) {
	cli, ok := s.brokers[req.BrokerId]
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

func (s *BrokerServer) ClientCreateTopic(topic string, n_partitions, replication_factor int) error {
	if s.grpcControllerClient == nil {
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

	res, err := s.grpcControllerClient.CreateTopic(ctx, req)
	if err != nil {
		fn := func() error { return s.ClientCreateTopic(topic, n_partitions, replication_factor) }
		return s.checkErrAndRedirect(err, fn)
	}
	if !res.Success {
		return fmt.Errorf("Failed to create topic in controller")
	}

	return nil
}

func (s *BrokerServer) HandleSendMessage(ctx context.Context, req *pb.SendMessageRequest) (*pb.SendMessageResponse, error) {
	err := s.Broker.SendMessage(req.Topic, req.Key, req.Value)
	if err != nil {
		if model.IsNotLeaderError(err) {
			var notLeader *model.NotLeaderError
			errors.As(err, &notLeader)
			leader, addr, err := s.Broker.Metadata.PartitionLeader(notLeader.Topic, notLeader.PartitionID)
			if err != nil {
				return nil, err
			}
			errorMsg := fmt.Sprintf("not leader|%s|%s", leader, addr)
			return nil, status.Error(codes.FailedPrecondition, errorMsg)
		}
		return nil, err
	}
	res := &pb.SendMessageResponse{
		Success: true,
	}
	return res, nil
}

func (s *BrokerServer) HandleProduce(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("ok"))
}

func (s *BrokerServer) HandleMetadata(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("ok"))
}

func (s *BrokerServer) HandleListOffsets(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("ok"))
}

func (s *BrokerServer) HandleJoinGroup(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("ok"))
}

func (s *BrokerServer) HandleSyncGroup(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("ok"))
}

func (s *BrokerServer) HandleLeaveGroup(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("ok"))
}

func (s *BrokerServer) HandleHeartbeat(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("ok"))
}

func (s *BrokerServer) HandleOffsetCommit(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("ok"))
}

func (s *BrokerServer) HandleOffsetFetch(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("ok"))
}

func (s *BrokerServer) HandleCreateTopics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("ok"))
}

func (s *BrokerServer) HandleDeleteTopics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("ok"))
}

func (s *BrokerServer) StopHeartbeat() {
	log.Println("Stoping heartbeat")
	s.hb_ticker.Stop()
	s.md_ticker.Stop()
}
func (s *BrokerServer) ResumeHeartbeat() {
	log.Println("Reseting heartbeat")
	s.hb_ticker.Reset(250 * time.Millisecond)
	s.md_ticker.Reset(1 * time.Second)
}
