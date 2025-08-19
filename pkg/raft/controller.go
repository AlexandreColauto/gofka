package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"

	"github.com/alexandrecolauto/gofka/model"
	"github.com/gorilla/mux"
)

type RaftController struct {
	Raft *RaftModule

	Topics  map[string]*TopicMetadata
	Brokers map[string]*model.BrokerInfo

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
	applyCh := make(chan model.LogEntry)
	bm := make(map[string]*model.BrokerInfo)
	tm := make(map[string]*TopicMetadata)

	r := NewRaftModule(nodeID, peers, applyCh)
	c := &RaftController{
		Raft:    r,
		Brokers: bm,
		Topics:  tm,

		NodeId:   nodeID,
		Peers:    peers,
		HttpAddr: address,
	}
	r.setSendAppendEntriesRequest(c.sendAppendEntriesRequest)
	r.setSendVoteRequest(c.sendVoteRequest)
	go c.applyCommands(applyCh)
	go c.SetupServer()
	return c
}

func (c *RaftController) SetupServer() {
	log.Printf("Starting node %s", c.NodeId)
	router := mux.NewRouter()

	router.HandleFunc("/raft/vote", c.HandleVoteRequest)
	router.HandleFunc("/raft/append", c.HandleAppendEntries)

	router.HandleFunc("/admin/register", c.HandleRegisterBroker)
	router.HandleFunc("/admin/create-topic", c.HandleCreateTopic)

	server := &http.Server{
		Addr:    c.HttpAddr,
		Handler: router,
	}

	log.Printf("Node %s is listening on %s", c.NodeId, c.HttpAddr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("Error: %v", err)
	}
}

func (c *RaftController) applyCommands(ch chan model.LogEntry) {
	for entry := range ch {
		if entry.Command == nil {
			continue
		}
		cmd := entry.Command
		switch cmd.Type {
		case model.CreateTopic:
			var ctc model.CreateTopicCommand
			if err := json.Unmarshal(cmd.Payload, &ctc); err != nil {
				log.Println("failed unmarshaling create topic command")
				continue
			}
			c.createTopic(ctc)
		case model.RegisterBrokerRecord:
			var ctc model.RegisterBrokerCommand
			if err := json.Unmarshal(cmd.Payload, &ctc); err != nil {
				log.Println("failed unmarshaling create topic command")
				continue
			}
			c.registerBroker(ctc)

		case model.ChangePartitionLeader:
			var ctc model.UpdateAssignmentsCommand
			if err := json.Unmarshal(cmd.Payload, &ctc); err != nil {
				log.Println("failed unmarshaling create topic command")
				continue
			}
			c.updatePartitionLeader(ctc)

		// case CreatePartition:
		// 	var cpc CreatePartitionCommand
		// 	if err := json.Unmarshal(cmd.Payload, &cpc); err != nil {
		// 		log.Println("failed unmarshaling create partition command")
		// 		continue
		// 	}
		// 	c.handleCreatePartition(cpc) // <-- This is where you select partition leader
		//
		// case ChangePartitionLeader:
		// 	var cplc ChangePartitionLeaderCommand
		// 	if err := json.Unmarshal(cmd.Payload, &cplc); err != nil {
		// 		log.Println("failed unmarshaling change leader command")
		// 		continue
		// 	}
		// 	c.handlePartitionLeaderChange(cplc)

		default:
			log.Println("Unknown metadata command: ", string(cmd.Payload))
		}
	}
}

func (c *RaftController) createTopic(ctc model.CreateTopicCommand) {
	//now create topic
	partitions := make(map[int]*PartitionMetadata, 0)
	for i := range ctc.NPartition {
		p := &PartitionMetadata{
			ID:       i,
			Replicas: make([]string, ctc.ReplicationFactor),
			ISR:      make([]string, ctc.ReplicationFactor+1),
		}
		partitions[i] = p
	}

	t := &TopicMetadata{
		Name:       ctc.Topic,
		Partitions: partitions,
	}
	c.Topics[ctc.Topic] = t

	if c.Raft.state == Leader {
		c.notifyTopicCreated(ctc)
		c.electPartitionsLeaders()
	}
}

func (c *RaftController) registerBroker(ctc model.RegisterBrokerCommand) {
	brk := &model.BrokerInfo{
		ID:       ctc.ID,
		Address:  ctc.Address,
		Alive:    ctc.Alive,
		LastSeen: ctc.LastSeen,
	}
	c.Brokers[ctc.ID] = brk
	if c.Raft.state == Leader {
		c.notifyBrokerUpdate()
		c.electPartitionsLeaders()
	}
}

func (c *RaftController) updatePartitionLeader(ctc model.UpdateAssignmentsCommand) {
	for _, assignment := range ctc.Assignments {
		t, ok := c.Topics[assignment.TopicID]
		if !ok {
			continue
		}
		p, ok := t.Partitions[int(assignment.PartitionID)]
		if !ok {
			continue
		}
		p.Epoch = int64(assignment.NewEpoch)
		p.Leader = assignment.NewLeader
		p.Replicas = assignment.NewReplicas
		p.ISR = assignment.NewISR
	}
	// who should I notify about the leader change, should I try to notify the dead leader (all replicas), or just teh alivve?
	if c.Raft.IsLeader() {
		c.notifyChanges(ctc)
	}

}
func (c *RaftController) notifyBrokerUpdate() error {
	for _, b := range c.Brokers {
		peers := []*model.BrokerInfo{}
		for _, other := range c.Brokers {
			if b.ID != other.ID {
				peers = append(peers, other)
			}
		}
		body, err := json.Marshal(peers)
		if err != nil {
			return err
		}
		_, err = http.Post(fmt.Sprintf("http://%s/controller/broker-register", b.Address), "application/json", bytes.NewReader(body))
		if err != nil {
			return err
		}
	}
	return nil
}
func (c *RaftController) notifyTopicCreated(cmd model.CreateTopicCommand) error {
	body, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	for _, b := range c.Brokers {
		_, err = http.Post(fmt.Sprintf("http://%s/controller/topic-create", b.Address), "application/json", bytes.NewReader(body))
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *RaftController) notifyChanges(command model.UpdateAssignmentsCommand) error {
	toNotify := make(map[string][]model.PartitionAssignment)
	for _, assigment := range command.Assignments {
		for _, replica := range assigment.NewReplicas {
			if c.isBrokerAlive(replica) {
				addr := c.Brokers[replica].Address
				//post new leader, epoch, replicas and isr to the broker
				toNotify[addr] = append(toNotify[addr], assigment)
			}
		}
	}
	for address, assignments := range toNotify {
		body, err := json.Marshal(assignments)
		if err != nil {
			return err
		}
		res, err := http.Post(fmt.Sprintf("http://%s/controller/leader-update", address), "application/json", bytes.NewReader(body))
		if err != nil {
			return err
		}
		fmt.Println(res.StatusCode)

	}
	return nil
}
func (c *RaftController) electPartitionsLeaders() {
	brokers := make([]*model.BrokerInfo, 0)
	for _, brkr := range c.Brokers {
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

	assigments := make([]model.PartitionAssignment, 0)

	for _, topic := range c.Topics {
		for _, partition := range topic.Partitions {
			if partition.Leader == "" {
				leaderID := brokers[brokerIndex].ID
				replicas := make([]string, len(partition.Replicas))
				replicas[0] = leaderID
				for i := 1; i < len(replicas); i++ {
					index := (brokerIndex + i) % len(brokers)
					replicas[i] = brokers[index].ID
				}
				ass := model.PartitionAssignment{
					TopicID:     topic.Name,
					PartitionID: int32(partition.ID),
					NewLeader:   leaderID,
					NewReplicas: replicas,
					NewISR:      replicas,
					NewEpoch:    int(partition.Epoch) + 1,
				}
				assigments = append(assigments, ass)
				brokerIndex = (brokerIndex + 1) % len(brokers)
			}
		}
	}

	if len(assigments) > 0 {
		fmt.Println("Registering topic assigment")
		cmd, err := model.NewUpdateAssigmentCommand(assigments)
		if err != nil {
			return
		}
		c.Raft.SubmitCommand(cmd)
	}
}

func (c *RaftController) brokerFailOver(leaderID string) {
	if len(c.Brokers) == 0 {
		return // No alive brokers
	}
	assigments := make([]model.PartitionAssignment, 0)
	for _, topic := range c.Topics {
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
					ass := model.PartitionAssignment{
						TopicID:     topic.Name,
						PartitionID: int32(partition.ID),
						NewLeader:   newLeader,
						NewReplicas: partition.Replicas,
						NewISR:      newISR,
						NewEpoch:    int(partition.Epoch) + 1,
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
		cmd, err := model.NewUpdateAssigmentCommand(assigments)
		if err != nil {
			return
		}
		c.Raft.SubmitCommand(cmd)
	}

}

func (c *RaftController) isBrokerAlive(brokerID string) bool {
	return c.Brokers[brokerID].Alive
}

func (c *RaftController) HandleRegisterBroker(w http.ResponseWriter, r *http.Request) {
	var req model.RegisterBrokerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	cmd, err := model.NewRegisterBrokerCommand(req.ID, req.Address)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	c.submitCommand(cmd, w, r)
}

func (c *RaftController) HandleCreateTopic(w http.ResponseWriter, r *http.Request) {
	var req model.CreateTopicRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	cmd, err := model.NewCreateTopicCommand(req.Topic, req.NPartition, req.ReplicationFactor)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(c.Brokers) < req.ReplicationFactor {
		fmt.Println("Replication factor wrong", req.ReplicationFactor, len(c.Brokers))
		http.Error(w, "Replication factor is larger than the number of available brokers", http.StatusBadRequest)
		return
	}
	c.submitCommand(cmd, w, r)
}

func (c *RaftController) submitCommand(cmd *model.Command, w http.ResponseWriter, r *http.Request) {
	err := c.Raft.SubmitCommand(cmd)
	if err != nil {
		leader := c.Raft.leaderID
		if leader == "" {
			http.Error(w, "no leader available", http.StatusServiceUnavailable)
			return
		}
		addr, ok := c.Peers[c.Raft.leaderID]
		if !ok {
			http.Error(w, "no leader available", http.StatusServiceUnavailable)
			return
		}
		redirectURL := fmt.Sprintf("http://%s%s", addr, r.URL.Path)
		w.Header().Set("location", redirectURL)
		w.WriteHeader(http.StatusTemporaryRedirect)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
}
