package raft

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/alexandrecolauto/gofka/pkg/broker"
	"github.com/gorilla/mux"
)

type RaftController struct {
	Raft       *RaftModule
	Broker     *broker.Gofka
	HttpServer *broker.BrokerServer

	NodeId   string
	Peers    map[string]string
	HttpAddr string
}

func NewController(nodeID, address string, peers map[string]string) *RaftController {
	applyCh := make(chan LogEntry)

	b := broker.NewGofka()

	r := NewRaftModule(nodeID, peers, applyCh)
	c := &RaftController{
		Raft:   r,
		Broker: b,

		NodeId:   nodeID,
		Peers:    peers,
		HttpAddr: address,
	}
	r.setSendAppendEntriesRequest(c.sendAppendEntriesRequest)
	r.setSendVoteRequest(c.sendVoteRequest)
	go c.applyCommands(applyCh)
	return c
}

func (c *RaftController) applyCommands(ch chan LogEntry) {
	for entry := range ch {
		if entry.Command == nil {
			continue
		}
		cmd := entry.Command
		switch cmd.Type {
		case CreateTopic:
			var ctc CreateTopicCommand
			if err := json.Unmarshal(cmd.Payload, &ctc); err != nil {
				log.Println("failed unmarshaling create topic command")
				continue
			}
			c.handleCreateTopic(ctc)

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
			log.Println("Unknown metadata command: ", cmd)
		}
	}
}

func (c *RaftController) handleCreateTopic(ctc CreateTopicCommand) {
	//now create topic
	// distribuite among all available brokers.
	//Topic: orders, Replication Factor: 3

	// Partition 0: [broker-1*, broker-2, broker-3]  # broker-1 is leader
	// Partition 1: [broker-2*, broker-3, broker-1]  # broker-2 is leader
	// Partition 2: [broker-3*, broker-1, broker-2]  # broker-3 is leader
	// Partition 3: [broker-1*, broker-2, broker-3]  # broker-1 is leader
	// Partition 4: [broker-2*, broker-3, broker-1]  # broker-2 is leader
	//
	// * = partition leader

}
func (c *RaftController) HandleRegisterBroker(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	address := vars["address"]
	fmt.Println("Address:", address)
}

// func (c *RaftController) HandleProduceMessage(w http.ResponseWriter, r *http.Request) {
// 	fmt.Printf("Producing msg with %s\n", c.Raft.id)
// 	var req ProduceCommand
// 	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
// 		http.Error(w, err.Error(), http.StatusBadRequest)
// 		return
// 	}
// 	com, err := NewProduceCommand(req.Topic, req.Partition, req.Key, req.Message)
// 	if err != nil {
// 		http.Error(w, err.Error(), http.StatusBadRequest)
// 		return
// 	}
//
// 	err = c.Raft.SubmitCommand(com)
// 	if err != nil {
// 		leader := c.Raft.leaderID
// 		if leader == "" {
// 			http.Error(w, "no leader available", http.StatusServiceUnavailable)
// 			return
// 		}
// 		addr, ok := c.Peers[c.Raft.leaderID]
// 		if !ok {
// 			http.Error(w, "no leader available", http.StatusServiceUnavailable)
// 			return
// 		}
// 		redirectURL := fmt.Sprintf("http://%s%s", addr, r.URL.Path)
// 		fmt.Println("Redirecting to", redirectURL)
// 		w.Header().Set("location", redirectURL)
// 		w.WriteHeader(http.StatusTemporaryRedirect)
// 		return
// 	}
// 	w.Header().Set("Content-Type", "application/json")
// 	w.WriteHeader(http.StatusOK)
// }
