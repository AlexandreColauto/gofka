package broker

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

type BrokerServer struct {
	Broker *Gofka

	controllerAddress string
	brokerAddress     string
}

func NewBrokerServer(controllerAddress, brokerAddress string) (*BrokerServer, error) {
	b := NewGofka()
	bs := &BrokerServer{Broker: b, controllerAddress: controllerAddress, brokerAddress: brokerAddress}
	err := bs.registerBroker()
	if err != nil {
		return nil, err
	}
	return bs, nil
}

func (s *BrokerServer) registerBroker() error {
	fmt.Println("registering")
	url := fmt.Sprintf("http://%s/raft/register/%s", s.controllerAddress, s.brokerAddress)
	resp, err := http.Get(url)
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
	if !r.URL.Query().Has("topic") || !r.URL.Query().Has("partition") || !r.URL.Query().Has("fetchoffset") || !r.URL.Query().Has("longendoffset") {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	topic := r.URL.Query().Get("topic")
	partitionID := r.URL.Query().Get("partition")
	f_offset := r.URL.Query().Get("fetchoffset")
	leo_str := r.URL.Query().Get("longendoffset")

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

	err = s.Broker.UpdateFollowerState(topic, "follower_id", int(p_id), f_off, leo)

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
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
