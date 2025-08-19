package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/alexandrecolauto/gofka/model"
	"github.com/alexandrecolauto/gofka/pkg/broker"
	"github.com/alexandrecolauto/gofka/pkg/raft"
)

func main() {
	setup()
	time.Sleep(2 * time.Second)
	bs_0, err := broker.NewBrokerServer("localhost:42069", "localhost:42169", "broker-0")
	if err != nil {
		panic(err)
	}
	bs_1, err := broker.NewBrokerServer("localhost:42069", "localhost:42170", "broker-1")
	if err != nil {
		panic(err)
	}

	fmt.Println(bs_1)
	time.Sleep(2 * time.Second)
	bs_0.ClientCreateTopic("topic-0", 5, 2)

	time.Sleep(20 * time.Second)
}

func createTopic() {
	url := "http://localhost:42069/produce"
	payload := model.CreateTopicCommand{
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
	}
}
