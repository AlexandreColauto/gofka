package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/alexandrecolauto/gofka/pkg/broker"
	"github.com/alexandrecolauto/gofka/pkg/raft"
	"github.com/gorilla/mux"
)

func main() {
	setup()
	time.Sleep(1 * time.Second)
	_, err := broker.NewBrokerServer("localhost:42069", "broker-address")
	if err != nil {
		panic(err)
	}
	//create new broker, now brokers must contain the Controller address.
	//broker send register to controller.
	//create new topic
	//check assignments

	time.Sleep(20 * time.Second)
}
func createTopic() {
	url := "http://localhost:42069/produce"
	payload := raft.CreateTopicCommand{
		Topic:             "foo-topic",
		NPartition:        1,
		ReplicationFactor: 3,
	}
	body, err := json.Marshal(&payload)
	if err != nil {
		panic(err)
	}
	res, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		panic(err)
	}
	defer res.Body.Close()
	b, err := io.ReadAll(res.Body)
	fmt.Println("code", res.StatusCode)
	fmt.Println("b", b)
}

func setup() {
	nodeAddresses := map[string]string{
		"node1": "localhost:42069",
		"node2": "localhost:42070",
		"node3": "localhost:42071",
		"node4": "localhost:42072",
		"node5": "localhost:42073",
	}

	controllers := make([]*raft.RaftController, 0, 5)
	for nodeID, address := range nodeAddresses {
		peers := make(map[string]string)
		for nID, addr := range nodeAddresses {
			if nID != nodeID {
				peers[nID] = addr
			}
		}

		ctrl := raft.NewController(nodeID, address, peers)
		controllers = append(controllers, ctrl)

		go func(nodeID, addr string, ctr *raft.RaftController) {
			log.Printf("Starting node %s", nodeID)
			router := mux.NewRouter()

			router.HandleFunc("/raft/vote", ctr.HandleVoteRequest)
			router.HandleFunc("/raft/append", ctr.HandleAppendEntries)
			router.HandleFunc("/raft/register/{address}", ctr.HandleRegisterBroker)

			server := &http.Server{
				Addr:    addr,
				Handler: router,
			}

			log.Printf("Node %s is listening on %s", nodeID, addr)
			if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Printf("Error: %v", err)
			}

		}(nodeID, address, ctrl)
	}
}
