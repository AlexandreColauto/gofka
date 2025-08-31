package main

import (
	"fmt"
	// "log"
	"strconv"
	"strings"
	"time"

	"github.com/alexandrecolauto/gofka/pkg/broker"
	"github.com/alexandrecolauto/gofka/pkg/consumer"
	"github.com/alexandrecolauto/gofka/pkg/controller/kraft"
	"github.com/alexandrecolauto/gofka/pkg/producer"
	pb "github.com/alexandrecolauto/gofka/proto/broker"
)

func main() {

	setupRaftController()
	time.Sleep(2 * time.Second)
	setupBrokers()
	time.Sleep(5 * time.Second)
	// log.Println("SENDING MSG ----------------")
	// produceMessage()
	// time.Sleep(5 * time.Second)
	// log.Println("CREATING TOPIIC ----------------")
	// createTopic()
	// log.Println("STARTING CONSUMERS ----------------")
	// consumeMessage()
	time.Sleep(20 * time.Second)
}
func consumeMessage() {
	groupID := "foo-group"
	brokerAddress := "localhost:42169"
	topics := []string{"topic-1"}
	go func() { consumer.NewConsumer(groupID, brokerAddress, topics) }()
	c := consumer.NewConsumer(groupID, brokerAddress, topics)
	if c != nil {

	}
	// // err := c.Subscribe(topic)
	// if err != nil {
	// 	panic(err)
	// }
	opt := &pb.ReadOptions{
		MaxMessages: 100,
		MaxBytes:    1024 * 1024,
		MinBytes:    1024 * 1024,
	}
	msgs, err := c.Poll(5*time.Second, opt)
	if err != nil {
		panic(err)
	}
	for _, msg := range msgs {
		fmt.Println("Yehaaa got the message: ", msg)
	}
}

func createTopic() {
	p := producer.NewProducer("topic-0", "localhost:42169")
	p.ConnectToBroker()
	time.Sleep(1 * time.Second)
	err := p.CreateTopic("topic-1", 3, 2)
	if err != nil {
		fmt.Println("error creating topic msg ", err)
	}
}
func produceMessage() {
	p := producer.NewProducer("topic-1", "localhost:42169")
	p.ConnectToBroker()
	time.Sleep(1 * time.Second)
	err := p.SendMessage("foo", "bar")
	if err != nil {
		fmt.Println("error sending msg ", err)
	}
	err = p.SendMessage("foo", "bar2")
	if err != nil {
		fmt.Println("error sending second msg ", err)
	}
}

func setupBrokers() {
	brokerAddresses := map[string]string{
		"broker1": "localhost:42169",
		"broker2": "localhost:42170",
		"broker3": "localhost:42171",
	}
	for nodeID, address := range brokerAddresses {
		_, err := broker.NewBrokerServer("localhost:42069", address, nodeID)
		if err != nil {
			panic(err)
		}
	}
}
func setupRaftController() {
	nodeAddresses := map[string]string{
		"node1": "localhost:42069",
		"node2": "localhost:42070",
		"node3": "localhost:42071",
		"node4": "localhost:42072",
		"node5": "localhost:42073",
	}

	controllers := make([]*kraft.KraftServer, 0, 5)
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

		ctrl, err := kraft.NewControllerServer(nodeID, address, peers)
		if err != nil {
			panic(err)
		}
		go run(ctrl, int(p))
		controllers = append(controllers, ctrl)
	}
	time.Sleep(1 * time.Second)
	for _, ctr := range controllers {
		err := ctr.ConnectGRPC()
		if err != nil {
			panic(err)
		}
	}
}

func run(ctrl *kraft.KraftServer, port int) {
	err := ctrl.Start(strconv.Itoa(port))
	if err != nil {
		panic(err)
	}
}
