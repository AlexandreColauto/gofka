package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	pb "github.com/alexandrecolauto/gofka/proto/broker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type BrokerServer struct {
	Broker    *Gofka
	hb_ticker *time.Ticker
	md_ticker *time.Ticker

	controllerAddress string
	brokerAddress     string
	brokerID          string

	grpcClient pb.BrokerServiceClient
	grpcConn   *grpc.ClientConn
}

func NewBrokerServer(controllerAddress, brokerAddress, brokerID string) (*BrokerServer, error) {
	b := NewGofka(brokerID)
	bs := &BrokerServer{Broker: b, controllerAddress: controllerAddress, brokerAddress: brokerAddress, brokerID: brokerID}
	err := bs.registerBroker()
	if err != nil {
		return nil, err
	}
	bs.hb_ticker = time.NewTicker(1 * time.Second)
	bs.md_ticker = time.NewTicker(1 * time.Second)
	go bs.startHeartbeat()
	go bs.startMetadataFetcher()
	bs.initGRPCConnection()
	return bs, nil
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
	b.grpcConn = conn
	b.grpcClient = pb.NewBrokerServiceClient(conn)
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
	b.hb_ticker.Reset(1 * time.Second)
	for {
		select {
		case <-b.hb_ticker.C:
			b.sendHeartbeat()
		}
	}
}

func (s *BrokerServer) sendHeartbeat() error {
	panic("change to grpc")
	//to-do: change to gRPC based
	// body := model.BrokerHeartbeatRequest{
	// 	ID: s.brokerID,
	// }
	//
	// url := fmt.Sprintf("http://%s/broker/heartbeat", s.controllerAddress)
	// str, err := json.Marshal(&body)
	// if err != nil {
	// 	return err
	// }
	//
	// resp, err := http.Post(url, "application/json", bytes.NewReader(str))
	//
	// if err != nil {
	// 	return err
	// }
	//
	// defer resp.Body.Close()
	// if resp.StatusCode != http.StatusOK {
	// 	return fmt.Errorf("Failed to register on controller")
	// }
	return nil
}

func (s *BrokerServer) fetchMetadata() error {
	if s.grpcClient == nil {
		if err := s.initGRPCConnection(); err != nil {
			return err
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	request := &pb.BrokerMetadataRequest{
		BrokerId: s.brokerID,
		Index:    s.Broker.MetadataIndex,
	}
	response, err := s.grpcClient.FetchMetadata(ctx, request)
	if err != nil {
		return err
	}

	if !response.Success {
		return fmt.Errorf("Failed to fetch metadata: %s", response.ErrorMessage)
	}
	s.Broker.ProcessControllerLogs(response.Logs)
	return nil
}

func (s *BrokerServer) registerBroker() error {
	if s.grpcClient == nil {
		if err := s.initGRPCConnection(); err != nil {
			return err
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := &pb.BrokerRegisterRequest{
		Id:      s.brokerID,
		Address: s.brokerAddress,
	}
	response, err := s.grpcClient.RegisterBroker(ctx, req)
	if err != nil {
		if st, ok := status.FromError(err); ok {
			if st.Code() == codes.FailedPrecondition {
				// Parse the error message for leader info
				parts := strings.Split(st.Message(), "|")
				if len(parts) == 3 && parts[0] == "not leader" {
					leaderID := parts[1]
					leaderAddr := parts[2]

					log.Printf("Redirecting to leader %s at %s", leaderID, leaderAddr)
					s.updateLeaderAddress(leaderAddr)
					return s.registerBroker()
				}
			}
		}
		return err
	}
	if response != nil && !response.Success {
		return fmt.Errorf(response.ErrorMessage)
	}
	return nil
}

func (s *BrokerServer) updateLeaderAddress(address string) {
	s.controllerAddress = address
	s.grpcConn.Close()
	s.initGRPCConnection()
}

func (s *BrokerServer) HandleFetchReplica(w http.ResponseWriter, r *http.Request) {
	if !r.URL.Query().Has("topic") || !r.URL.Query().Has("partition") || !r.URL.Query().Has("offset") || !r.URL.Query().Has("maxBytes") {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	topic := r.URL.Query().Get("topic")
	partitionID := r.URL.Query().Get("partition")
	offset := r.URL.Query().Get("offset")
	maxBytes := r.URL.Query().Get("maxBytes")

	mB_int, err := strconv.ParseInt(maxBytes, 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	p_id, err := strconv.ParseInt(partitionID, 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	off, err := strconv.ParseInt(offset, 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	opt := NewReadOpts(100, int32(mB_int), 1024)

	msgs, err := s.Broker.FetchMessagesReplica(topic, int(p_id), off, opt.ToOpt())
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(msgs)
}

func (s *BrokerServer) HandleUpdateFollower(w http.ResponseWriter, r *http.Request) {
	if !r.URL.Query().Has("topic") || !r.URL.Query().Has("partition") || !r.URL.Query().Has("fetchoffset") || !r.URL.Query().Has("longendoffset") || !r.URL.Query().Has("followerid") {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	topic := r.URL.Query().Get("topic")
	partitionID := r.URL.Query().Get("partition")
	f_offset := r.URL.Query().Get("fetchoffset")
	leo_str := r.URL.Query().Get("longendoffset")
	f_id := r.URL.Query().Get("followerid")

	f_off, err := strconv.ParseInt(f_offset, 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	p_id, err := strconv.ParseInt(partitionID, 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	leo, err := strconv.ParseInt(leo_str, 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	err = s.Broker.UpdateFollowerState(topic, f_id, int(p_id), f_off, leo)

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
}

func (s *BrokerServer) ClientCreateTopic(topic string, n_partitions, replication_factor int) error {
	//change to grpc based
	panic("change to grpc")
	// body := model.CreateTopicRequest{
	// 	Topic:             topic,
	// 	NPartition:        n_partitions,
	// 	ReplicationFactor: replication_factor,
	// }
	// url := fmt.Sprintf("http://%s/admin/create-topic", s.controllerAddress)
	// str, err := json.Marshal(&body)
	// if err != nil {
	// 	return err
	// }
	//
	// resp, err := http.Post(url, "application/json", bytes.NewReader(str))
	// if err != nil {
	// 	return err
	// }
	//
	// defer resp.Body.Close()
	// if resp.StatusCode != http.StatusOK {
	// 	fmt.Println("Error request: ", resp.StatusCode)
	// 	return fmt.Errorf("Failed to create topic on controller")
	// }
	return nil
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
	s.hb_ticker.Reset(1 * time.Second)
	s.md_ticker.Reset(1 * time.Second)
}
