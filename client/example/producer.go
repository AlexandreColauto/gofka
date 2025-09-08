package example

import (
	"fmt"

	"github.com/alexandrecolauto/gofka/client/config"
	"github.com/alexandrecolauto/gofka/client/pkg/producer"
)

func Produce() {
	cfg := config.NewProducerConfig()
	cfg.BootstrapAddress = "localhost:42069"
	//can set the topic on the config
	cfg.Topic = "foo-topic"
	p := producer.NewProducer(cfg)
	//or via function call
	p.UpdateTopic("bar-topic")
	err := p.SendMessage("foo", "bar")
	if err != nil {
		fmt.Println("error sending msg ", err)
	}
}

func CreateTopic() {
	cfg := config.NewProducerConfig()
	cfg.BootstrapAddress = "localhost:42069"

	//create topic with AutoCreateTopics = true
	cfg.AutoCreateTopics = true
	p := producer.NewProducer(cfg)

	//the topic will be created when sending new message
	p.UpdateTopic("bar-topic")
	err := p.SendMessage("foo", "bar")

	//or for more control explicitly create a new topic
	topic := "foo-topic"
	partitions := 3
	replication_factor := 2
	err = p.CreateTopic(topic, partitions, replication_factor)
	if err != nil {
		fmt.Println("error creating topic msg ", err)
	}
}

func main() {
	// log.Println("CREATING TOPIIC ----------------")
	// createTopic()
	// time.Sleep(5 * time.Second)
	log.Println("SENDING MSG ----------------")
	produceMessage()
	log.Println("STARTING CONSUMERS ----------------")
	consumeMessage()
	// time.Sleep(20 * time.Second)
	waitCh := make(chan any)
	<-waitCh
}

func consumeMessage() {
	groupID := "foo-group"
	brokerAddress := "localhost:42169"
	topics := []string{"foo-topic"}
	nodeType := "consumer"
	go func() {
		viCli := visualizerclient.NewVisualizerClient(nodeType, "localhost:42042")
		consumer.NewConsumer(groupID, brokerAddress, topics, viCli)
	}()
	viCli := visualizerclient.NewVisualizerClient(nodeType, "localhost:42042")
	c := consumer.NewConsumer(groupID, brokerAddress, topics, viCli)
	if c != nil {

	}
	// opt := &pb.ReadOptions{
	// 	MaxMessages: 100,
	// 	MaxBytes:    1024 * 1024,
	// 	MinBytes:    1024 * 1024,
	// }
	// fmt.Println("pooling msg")
	// msgs, err := c.Poll(5*time.Second, opt)
	// if err != nil {
	// 	panic(err)
	// }
	// for _, msg := range msgs {
	// 	fmt.Println("Yehaaa got the message: ", msg)
	// }
}

func createTopic() {
	topic := "foo-topic"
	ack := pb.ACKLevel_ACK_1
	nodeType := "producer"
	viCli := visualizerclient.NewVisualizerClient(nodeType, "localhost:42042")
	p := producer.NewProducer("topic-0", "localhost:42169", ack, viCli)
	p.ConnectToBroker()
	time.Sleep(1 * time.Second)
	err := p.CreateTopic(topic, 3, 2)
	if err != nil {
		fmt.Println("error creating topic msg ", err)
	}
}
func produceMessage() {
	topic := "foo-topic"
	ack := pb.ACKLevel_ACK_ALL
	for range 5 {
		nodeType := "producer"
		viCli := visualizerclient.NewVisualizerClient(nodeType, "localhost:42042")
		p := producer.NewProducer(topic, "localhost:42169", ack, viCli)
		p.ConnectToBroker()
	}
	time.Sleep(1 * time.Second)
	// err := p.SendMessage("foo", "bar")
	// if err != nil {
	// 	fmt.Println("error sending msg ", err)
	// }
	// err = p.SendMessage("foo", "bar2")
	// if err != nil {
	// 	fmt.Println("error sending second msg ", err)
	// }
}

func setupBrokers() {
	brokerAddresses := map[string]string{
		"broker1": "localhost:42169",
		"broker2": "localhost:42170",
		"broker3": "localhost:42171",
	}
	for nodeID, address := range brokerAddresses {
		nodeType := "broker"
		viCli := visualizerclient.NewVisualizerClient(nodeType, "localhost:42042")
		_, err := broker.NewBrokerServer("localhost:42069", address, nodeID, viCli)
		if err != nil {
			panic(err)
		}
	}
	fmt.Println("Brokers connected")
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
		nodeType := "controller"
		viCli := visualizerclient.NewVisualizerClient(nodeType, "localhost:42042")

		ctrl, err := kraft.NewControllerServer(nodeID, address, peers, viCli)
		if err != nil {
			panic(err)
		}
		controllers = append(controllers, ctrl)
	}
}
