package broker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"

	"github.com/alexandrecolauto/gofka/model"
	"github.com/gorilla/mux"
)

type BrokerServer struct {
	Broker *Gofka

	controllerAddress string
	brokerAddress     string
	brokerID          string
}

func NewBrokerServer(controllerAddress, brokerAddress, brokerID string) (*BrokerServer, error) {
	b := NewGofka(brokerID)
	bs := &BrokerServer{Broker: b, controllerAddress: controllerAddress, brokerAddress: brokerAddress, brokerID: brokerID}
	go bs.SetupServer()
	err := bs.registerBroker()
	if err != nil {
		return nil, err
	}
	return bs, nil
}

func (b *BrokerServer) SetupServer() {
	log.Printf("Starting broker %s", b.brokerID)
	router := mux.NewRouter()

	router.HandleFunc("/controller/broker-register", b.HandleBrokerRegister)
	router.HandleFunc("/controller/topic-create", b.HandleTopicCreate)
	router.HandleFunc("/controller/leader-update", b.HandleLeaderUpdate)
	router.HandleFunc("/fetch-replica", b.HandleFetchReplica)
	router.HandleFunc("/update-follower", b.HandleUpdateFollower)

	server := &http.Server{
		Addr:    b.brokerAddress,
		Handler: router,
	}

	log.Printf("Broker %s is listening on %s", b.brokerID, b.brokerAddress)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("Error: %v", err)
	}
}

func (s *BrokerServer) registerBroker() error {
	fmt.Println("registering")
	body := model.RegisterBrokerRequest{
		ID:      s.brokerID,
		Address: s.brokerAddress,
	}
	url := fmt.Sprintf("http://%s/admin/register", s.controllerAddress)
	str, err := json.Marshal(&body)
	if err != nil {
		return err
	}

	resp, err := http.Post(url, "application/json", bytes.NewReader(str))

	if err != nil {
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Failed to register on controller")
	}
	return nil
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

func (s *BrokerServer) HandleBrokerRegister(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	str, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	var brokers []model.BrokerInfo
	err = json.Unmarshal(str, &brokers)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	s.Broker.UpdateBrokers(brokers)
}

func (s *BrokerServer) HandleTopicCreate(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	str, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	var topic model.CreateTopicCommand
	err = json.Unmarshal(str, &topic)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	s.Broker.CreateTopic(topic.Topic, topic.NPartition)
	fmt.Printf("Topic %s created\n", topic.Topic)
}

func (s *BrokerServer) HandleLeaderUpdate(w http.ResponseWriter, r *http.Request) {
	fmt.Println("NEW MESSAGE IN SERER")
	defer r.Body.Close()
	str, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	var assignments []model.PartitionAssignment
	err = json.Unmarshal(str, &assignments)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	for _, ass := range assignments {
		s.updateFollower(ass)
	}
}

func (s *BrokerServer) updateFollower(assigment model.PartitionAssignment) {
	s.Broker.RplManager.HandleLeaderChange(assigment.TopicID, int(assigment.PartitionID), assigment.NewLeader, int64(assigment.NewEpoch))
}
func (s *BrokerServer) CreateTopic(topic string, n_partitions, replication_factor int) error {
	body := model.CreateTopicRequest{
		Topic:             topic,
		NPartition:        n_partitions,
		ReplicationFactor: replication_factor,
	}
	url := fmt.Sprintf("http://%s/admin/create-topic", s.controllerAddress)
	str, err := json.Marshal(&body)
	if err != nil {
		return err
	}

	resp, err := http.Post(url, "application/json", bytes.NewReader(str))
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Println("Error request: ", resp.StatusCode)
		return fmt.Errorf("Failed to create topic on controller")
	}
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
