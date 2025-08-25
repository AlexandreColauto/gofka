package raft

import (
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/alexandrecolauto/gofka/model"
	pb "github.com/alexandrecolauto/gofka/proto/broker"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type RaftController struct {
	Raft *RaftModule

	Metadata *model.ClusterMetadata

	timeout time.Duration

	NodeId   string
	Peers    map[string]string
	HttpAddr string
}

type TopicMetadata struct {
	Name       string
	Partitions map[int]*PartitionMetadata
}

type PartitionMetadata struct {
	ID       int
	Leader   string
	Replicas []string
	ISR      []string
	Epoch    int64
}

func NewController(nodeID, address string, peers map[string]string) *RaftController {
	applyCh := make(chan *pb.LogEntry)
	mt := model.NewClusterMetadata()

	r := NewRaftModule(nodeID, peers, applyCh)
	c := &RaftController{
		Raft:     r,
		Metadata: mt,

		timeout: 2 * time.Second,

		NodeId:   nodeID,
		Peers:    peers,
		HttpAddr: address,
	}
	go c.applyCommands(applyCh)
	go c.monitorDeadSessions()
	return c
}

// func (c *RaftController) SetupServer() {
// 	log.Printf("Starting node %s", c.NodeId)
// 	router := mux.NewRouter()
//
// 	// router.HandleFunc("/raft/vote", c.HandleVoteRequest)
// 	// router.HandleFunc("/raft/append", c.HandleAppendEntries)
//
// 	// router.HandleFunc("/admin/register", c.HandleRegisterBroker)
// 	// router.HandleFunc("/admin/create-topic", c.HandleCreateTopic)
// 	//
// 	// router.HandleFunc("/broker/heartbeat", c.HandleBrokerHeartbeat)
// 	// router.HandleFunc("/broker/metadata", c.HandleBrokerMetadata)
//
// 	server := &http.Server{
// 		Addr:    c.HttpAddr,
// 		Handler: router,
// 	}
//
// 	log.Printf("Node %s is listening on %s", c.NodeId, c.HttpAddr)
// 	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
// 		log.Printf("Error: %v", err)
// 	}
// }

func (c *RaftController) applyCommands(ch chan *pb.LogEntry) {
	for entry := range ch {
		if entry.Command == nil {
			continue
		}
		c.Metadata.DecodeLog(entry, c)
	}
}

func (c *RaftController) ApplyCreateTopic(ctc *pb.Command_CreateTopic) {
	if c.Raft.state == Leader {
		c.electPartitionsLeaders()
	}
}

func (c *RaftController) ApplyRegisterBroker(ctc *pb.Command_RegisterBroker) {
	if c.Raft.state == Leader {
		c.electPartitionsLeaders()
	}
}

func (c *RaftController) ApplyUpdateBroker(ctc *pb.Command_UpdateBroker) {
	brk := &model.BrokerInfo{
		ID:       ctc.UpdateBroker.Id,
		Address:  ctc.UpdateBroker.Address,
		Alive:    ctc.UpdateBroker.Alive,
		LastSeen: ctc.UpdateBroker.LastSeen.AsTime(),
	}
	if !brk.Alive {
		c.removeFromISR(brk)
		if c.Raft.state == Leader {
			c.brokerFailOver(ctc.UpdateBroker.Id)
		}
	}
}

func (c *RaftController) ApplyUpdatePartitionLeader(ctc *pb.Command_ChangePartitionLeader) {
	for _, assignment := range ctc.ChangePartitionLeader.Assignments {
		t, ok := c.Metadata.Topics[assignment.TopicId]
		if !ok {
			continue
		}
		p, ok := t.Partitions[assignment.PartitionId]
		if !ok {
			continue
		}
		p.Epoch = int64(assignment.NewEpoch)
		p.Leader = assignment.NewLeader
		p.Replicas = assignment.NewReplicas
		p.ISR = assignment.NewIsr
	}
}

func (c *RaftController) electPartitionsLeaders() {
	brokers := make([]*model.BrokerInfo, 0)
	for _, brkr := range c.Metadata.Brokers {
		if brkr.Alive {
			brokers = append(brokers, brkr)
		}
	}

	if len(brokers) == 0 {
		return // No alive brokers
	}

	sort.Slice(brokers, func(i, j int) bool {
		return brokers[i].ID < brokers[j].ID
	})

	brokerIndex := 0

	assigments := make([]*pb.PartitionAssignment, 0)

	for _, topic := range c.Metadata.Topics {
		for _, partition := range topic.Partitions {
			if partition.Leader == "" {
				leaderID := brokers[brokerIndex].ID
				replicas := make([]string, len(partition.Replicas))
				replicas[0] = leaderID
				for i := 1; i < len(replicas); i++ {
					index := (brokerIndex + i) % len(brokers)
					replicas[i] = brokers[index].ID
				}
				ass := &pb.PartitionAssignment{
					TopicId:     topic.Name,
					PartitionId: int32(partition.ID),
					NewLeader:   leaderID,
					NewReplicas: replicas,
					NewIsr:      replicas,
					NewEpoch:    int32(partition.Epoch + 1),
				}
				assigments = append(assigments, ass)
				brokerIndex = (brokerIndex + 1) % len(brokers)
			}
		}
	}

	if len(assigments) > 0 {
		payload := &pb.ChangePartitionLeaderCommand{
			Assignments: assigments,
		}
		cmd := &pb.Command{
			Type: pb.Command_CHANGE_PARTITION_LEADER,
			Payload: &pb.Command_ChangePartitionLeader{
				ChangePartitionLeader: payload,
			},
		}
		c.Raft.SubmitCommand(cmd)
	}
}

func (c *RaftController) brokerFailOver(leaderID string) {
	if len(c.Metadata.Brokers) == 0 {
		return // No alive brokers
	}
	assigments := make([]*pb.PartitionAssignment, 0)
	for _, topic := range c.Metadata.Topics {
		for _, partition := range topic.Partitions {
			if !c.isBrokerAlive(partition.Leader) && partition.Leader == leaderID {
				newLeader := ""
				for _, replicaID := range partition.ISR {
					if c.isBrokerAlive(replicaID) && replicaID != partition.Leader {
						newLeader = replicaID
						break
					}
				}

				if newLeader != "" {
					newISR := make([]string, 0, len(partition.ISR)-1)
					for _, isrBrokerID := range partition.ISR {
						if isrBrokerID != leaderID {
							newISR = append(newISR, isrBrokerID)
						}
					}
					ass := &pb.PartitionAssignment{
						TopicId:     topic.Name,
						PartitionId: int32(partition.ID),
						NewLeader:   newLeader,
						NewReplicas: partition.Replicas,
						NewIsr:      newISR,
						NewEpoch:    int32(partition.Epoch) + 1,
					}

					assigments = append(assigments, ass)

				} else {
					// CRITICAL: No available leader in ISR. Partition is offline.
					fmt.Println("NO ISR AVAILABLE FOR LEADERSHIP")
				}
			}
		}
	}

	if len(assigments) > 0 {
		fmt.Println("Registering topic assigment")
		payload := &pb.ChangePartitionLeaderCommand{
			Assignments: assigments,
		}
		cmd := &pb.Command{
			Type: pb.Command_CHANGE_PARTITION_LEADER,
			Payload: &pb.Command_ChangePartitionLeader{
				ChangePartitionLeader: payload,
			},
		}
		c.Raft.SubmitCommand(cmd)
	}
}

func (c *RaftController) isBrokerAlive(brokerID string) bool {
	return c.Metadata.Brokers[brokerID].Alive
}

func (c *RaftController) RegisterBroker(r *pb.BrokerRegisterRequest) error {
	pyld := &pb.RegisterBrokerCommand{
		Id:       r.Id,
		Address:  r.Address,
		LastSeen: timestamppb.New(time.Now()),
		Alive:    true,
	}
	cmd := &pb.Command{
		Type: pb.Command_UPDATE_BROKER,
		Payload: &pb.Command_RegisterBroker{
			RegisterBroker: pyld,
		},
	}
	return c.submitCommandGRPC(cmd)
}

// func (c *RaftController) HandleRegisterBroker(w http.ResponseWriter, r *http.Request) {
// 	var req model.RegisterBrokerRequest
// 	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
// 		http.Error(w, err.Error(), http.StatusBadRequest)
// 		return
// 	}
// 	cmd, err := model.NewRegisterBrokerCommand(req.ID, req.Address)
// 	if err != nil {
// 		http.Error(w, err.Error(), http.StatusBadRequest)
// 		return
// 	}
// 	c.submitCommand(cmd, w, r)
// }
//
// func (c *RaftController) HandleBrokerHeartbeat(w http.ResponseWriter, r *http.Request) {
// 	var req model.BrokerHeartbeatRequest
// 	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
// 		http.Error(w, err.Error(), http.StatusBadRequest)
// 		return
// 	}
// 	redirected := c.redirectIfNotLeader(w, r)
// 	if redirected {
// 		return
// 	}
// 	err := c.brokerHeartbeat(req.ID)
// 	if err != nil {
// 		http.Error(w, err.Error(), http.StatusBadRequest)
// 		return
// 	}
// 	w.WriteHeader(http.StatusOK)
// }
//
// func (c *RaftController) HandleBrokerMetadata(w http.ResponseWriter, r *http.Request) {
// 	var req model.BrokerMetadataRequest
// 	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
// 		log.Println("error metadata request", err)
// 		http.Error(w, err.Error(), http.StatusBadRequest)
// 		return
// 	}
// 	redirected := c.redirectIfNotLeader(w, r)
// 	if redirected {
// 		return
// 	}
// 	logs, err := c.brokerMetadata(req.Index)
// 	if err != nil {
// 		log.Println("error metadata response", err)
// 		http.Error(w, err.Error(), http.StatusBadRequest)
// 		return
// 	}
// 	w.WriteHeader(http.StatusOK)
// 	json.NewEncoder(w).Encode(logs)
// }

// func (c *RaftController) HandleCreateTopic(w http.ResponseWriter, r *http.Request) {
// 	var req model.CreateTopicRequest
// 	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
// 		http.Error(w, err.Error(), http.StatusBadRequest)
// 		return
// 	}
// 	cmd, err := model.NewCreateTopicCommand(req.Topic, req.NPartition, req.ReplicationFactor)
// 	if err != nil {
// 		http.Error(w, err.Error(), http.StatusBadRequest)
// 		return
// 	}
// 	if len(c.Metadata.Brokers) < req.ReplicationFactor {
// 		fmt.Println("Replication factor wrong", req.ReplicationFactor, len(c.Metadata.Brokers))
// 		http.Error(w, "Replication factor is larger than the number of available brokers", http.StatusBadRequest)
// 		return
// 	}
// 	c.submitCommand(cmd, w, r)
// }

func (s *RaftController) submitCommandGRPC(cmd *pb.Command) error {
	err := s.Raft.SubmitCommand(cmd)
	if err != nil {
		return s.isLeader()
	}
	return nil
}

func (s *RaftController) isLeader() error {
	leader := s.Raft.leaderID
	if leader == "" {
		return status.Error(codes.Unavailable, "no leader available")
	}

	addr, ok := s.Peers[leader]
	if !ok {
		return status.Error(codes.Unavailable, "leader address not found")
	}

	// Create status with metadata containing leader address
	errorMsg := fmt.Sprintf("not leader|%s|%s", leader, addr)
	return status.Error(codes.FailedPrecondition, errorMsg)
}

func (c *RaftController) brokerHeartbeat(brokerID string) error {
	if notLeader := c.isLeader(); notLeader != nil {
		return notLeader
	}
	b, ok := c.Metadata.Brokers[brokerID]
	if !ok {
		log.Println("cannot find broker", brokerID)
		return fmt.Errorf("cannot find broker with id: %s", brokerID)
	}
	oldAlive := b.Alive
	b.Alive = true
	b.LastSeen = time.Now()
	if !oldAlive {
		log.Println("reviving broker:", b)
		c.commandChangeBroker(b)
	}
	return nil
}
func (c *RaftController) brokerMetadata(index int64) ([]*pb.LogEntry, error) {
	return c.Raft.LogFromIndex(index)
}

func (c *RaftController) monitorDeadSessions() {
	ticker := time.NewTicker(500 * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			c.cleanDeadSessions()

		}
	}
}

func (c *RaftController) cleanDeadSessions() {
	if !c.Raft.IsLeader() {
		return
	}
	for _, broker := range c.Metadata.Brokers {
		if time.Now().Sub(broker.LastSeen) > c.timeout && broker.Alive {
			brokerChange := &model.BrokerInfo{
				ID:       broker.ID,
				Address:  broker.Address,
				Alive:    false,
				LastSeen: broker.LastSeen,
			}
			log.Println("Found dead broker:", brokerChange)
			c.commandChangeBroker(brokerChange)
		}
	}
}

func (c *RaftController) commandChangeBroker(brokerInfo *model.BrokerInfo) {
	pyld := &pb.UpdateBrokerCommand{
		Id:       brokerInfo.ID,
		Address:  brokerInfo.Address,
		LastSeen: timestamppb.New(brokerInfo.LastSeen),
		Alive:    brokerInfo.Alive,
	}
	cmd := &pb.Command{
		Type: pb.Command_UPDATE_BROKER,
		Payload: &pb.Command_UpdateBroker{
			UpdateBroker: pyld,
		},
	}
	c.Raft.SubmitCommand(cmd)
}

func (c *RaftController) removeFromISR(brokerInfo *model.BrokerInfo) {
	for _, t := range c.Metadata.Topics {
		for _, p := range t.Partitions {
			newISR := make([]string, 0)
			for _, isr := range p.ISR {
				if isr != brokerInfo.ID {
					newISR = append(newISR, isr)
				}
			}
			p.ISR = newISR
		}
	}
}
