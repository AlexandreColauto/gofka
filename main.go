package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/alexandrecolauto/gofka/pkg/broker"
	"github.com/alexandrecolauto/gofka/pkg/raft"
)

func main() {
	setup()
	time.Sleep(2 * time.Second)
	bs_0, err := broker.NewBrokerServer("localhost:42000", "localhost:42169", "broker-0")
	if err != nil {
		panic(err)
	}
	bs_1, err := broker.NewBrokerServer("localhost:42000", "localhost:42170", "broker-1")
	if err != nil {
		panic(err)
	}

	fmt.Println(bs_0)
	fmt.Println(bs_1)
	time.Sleep(2 * time.Second)
	// bs_0.ClientCreateTopic("topic-0", 5, 2)
	// time.Sleep(2 * time.Second)
	// bs_0.StopHeartbeat()
	// time.Sleep(6 * time.Second)
	// bs_0.ResumeHeartbeat()

	time.Sleep(20 * time.Second)
}

func setup() {
	nodeAddresses := map[string]string{
		"node1": "localhost:42069",
		"node2": "localhost:42070",
		"node3": "localhost:42071",
		"node4": "localhost:42072",
		"node5": "localhost:42073",
	}

	controllers := make([]*raft.ControllerServer, 0, 5)
	// port := 42000
	for nodeID, address := range nodeAddresses {
		peers := make(map[string]string)
		for nID, addr := range nodeAddresses {
			if nID != nodeID {
				peers[nID] = addr
			}
		}

		port := strings.Split(address, ":")[1]
		fmt.Println("Port:", port)
		p, _ := strconv.ParseInt(port, 10, 64)

		ctrl := raft.NewControllerServer(nodeID, address, peers)
		go run(ctrl, int(p))
		controllers = append(controllers, ctrl)
	}
	for _, ctr := range controllers {
		err := ctr.ConnectGRPC()
		if err != nil {
			panic(err)
		}
	}
}
func run(ctrl *raft.ControllerServer, port int) {
	err := ctrl.Start(strconv.Itoa(port))
	if err != nil {
		panic(err)
	}
}
