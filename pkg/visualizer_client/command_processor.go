package visualizerclient

import (
	"encoding/json"
	"fmt"

	pv "github.com/alexandrecolauto/gofka/proto/visualizer"
)

type CommandProcessor struct {
	clients map[string]Client

	sendError func(target, errorMsg string)
}

type CreateTopicCommand struct {
	Topic       string `json:"topic"`
	NParts      int    `json:"n_parts"`
	Replication int    `json:"replication"`
}

type Client interface {
	GetClientId() string
}

type ProducerClient interface {
	Client
	CreateTopic(topic string, n_partitions int, replication int) error
	UpdateTopic(topic string)
	Produce() error
	StopProducing()
}

type ConsumerClient interface {
	Client
	RemoveTopic(topic string) error
	Consume() error
	StopConsume()
	AddTopic(topic string) error
}
type BrokerClient interface {
	Client
	Fence() error
}

func NewCommandProcessor() *CommandProcessor {
	c := make(map[string]Client)
	return &CommandProcessor{clients: c}
}

func (c *CommandProcessor) RegisterClient(id string, cli Client) {
	c.clients[id] = cli
}

func (c *CommandProcessor) GetProducerClient(id string) (ProducerClient, bool) {
	cli, ok := c.clients[id]
	if !ok {
		return nil, ok
	}
	cl, ok := cli.(ProducerClient)
	if !ok {
		return nil, ok
	}
	return cl, true
}
func (c *CommandProcessor) GetConsumerClient(id string) (ConsumerClient, bool) {
	cli, ok := c.clients[id]
	if !ok {
		fmt.Println("no client found:", c.clients)
		return nil, ok
	}
	cl, ok := cli.(ConsumerClient)
	if !ok {
		fmt.Println("cannot convert to ConsumerClient", cl)
		return nil, ok
	}
	return cl, true
}

func (c *CommandProcessor) GetBrokerClient(id string) (BrokerClient, bool) {
	cli, ok := c.clients[id]
	if !ok {
		fmt.Println("no client found:", c.clients)
		return nil, ok
	}
	cl, ok := cli.(BrokerClient)
	if !ok {
		fmt.Println("cannot convert to BrokerClient", cl)
		return nil, ok
	}
	return cl, true
}

func (c *CommandProcessor) Process(commands []*pv.Command) {
	for _, command := range commands {
		data := command.Data
		action := command.Action
		target := command.Target
		switch action {
		case "create-topic":
			c.processCreateTopic(data, target)
		case "update-topic":
			c.updateTopic(data, target)
		case "fence":
			c.processFence(target)
		case "send-message":
			c.sendMessage(data, target)
		case "stop-send-message":
			c.stopSendMessage(target)
		case "add-topic":
			c.addTopic(data, target)
		case "remove-topic":
			c.removeTopic(data, target)
		case "consume-message":
			c.consumeMessage(target)
		case "stop-consume-message":
			c.stopConsumeMessage(target)
		default:
			fmt.Println("cannot process: ", action)
		}
	}
}

func (c *CommandProcessor) processCreateTopic(data []byte, targetID string) {
	var create CreateTopicCommand
	err := json.Unmarshal(data, &create)
	if err != nil {
		panic(err)
	}
	fmt.Println("CRAETING TOPIC WITH: ", create, targetID)
	cli, ok := c.GetProducerClient(targetID)
	if !ok {
		errMsg := fmt.Sprintf("cannot find producer: %s", targetID)
		c.sendError(targetID, errMsg)
		return
	}
	err = cli.CreateTopic(create.Topic, create.NParts, create.Replication)
	if err != nil {
		c.sendError(targetID, err.Error())
	}
}

func (c *CommandProcessor) updateTopic(data []byte, targetID string) {
	topic := string(data)
	cli, ok := c.GetProducerClient(targetID)
	if !ok {
		errMsg := fmt.Sprintf("cannot find producer: %s", targetID)
		c.sendError(targetID, errMsg)
		return
	}
	cli.UpdateTopic(topic)
	fmt.Println("topic topic updated: ", topic)
}

func (c *CommandProcessor) addTopic(data []byte, targetID string) {
	topic := string(data)
	cli, ok := c.GetConsumerClient(targetID)
	if !ok {
		errMsg := fmt.Sprintf("cannot find consumer: %s", targetID)
		c.sendError(targetID, errMsg)
		return
	}
	err := cli.AddTopic(topic)
	if err != nil {
		c.sendError(targetID, err.Error())
		return
	}

	fmt.Println("topic added : ", topic)
}

func (c *CommandProcessor) removeTopic(data []byte, targetID string) {
	topic := string(data)
	cli, ok := c.GetConsumerClient(targetID)
	if !ok {
		errMsg := fmt.Sprintf("cannot find consumer: %s", targetID)
		c.sendError(targetID, errMsg)
		return
	}
	err := cli.RemoveTopic(topic)
	if err != nil {
		c.sendError(targetID, err.Error())
		return
	}
	fmt.Println("topic removed : ", topic)
}

func (c *CommandProcessor) processFence(targetID string) {
	fmt.Println("Fencing: ", targetID)
	cli, ok := c.GetBrokerClient(targetID)
	if !ok {
		errMsg := fmt.Sprintf("cannot find producer: %s", targetID)
		c.sendError(targetID, errMsg)
		return
	}
	fmt.Println("found client", cli)
	err := cli.Fence()
	if err != nil {
		c.sendError(targetID, err.Error())
	}
}

func (c *CommandProcessor) sendMessage(data []byte, targetID string) {
	topic := string(data)
	cli, ok := c.GetProducerClient(targetID)
	if !ok {
		errMsg := fmt.Sprintf("cannot find producer: %s", targetID)
		if ok == true {
			c.sendError(targetID, errMsg)
		}
		return
	}

	err := cli.Produce()
	if err != nil {
		fmt.Println("err sending msg from command: ", err)
		return
	}

	fmt.Println("msg sent: ", topic)
}
func (c *CommandProcessor) stopSendMessage(targetID string) {
	cli, ok := c.GetProducerClient(targetID)
	if !ok {
		errMsg := fmt.Sprintf("cannot find producer: %s", targetID)
		if ok == true {
			c.sendError(targetID, errMsg)
		}
		return
	}

	cli.StopProducing()

}
func (c *CommandProcessor) consumeMessage(targetID string) {
	cli, ok := c.GetConsumerClient(targetID)
	if !ok {
		errMsg := fmt.Sprintf("cannot find consumer: %s", targetID)
		c.sendError(targetID, errMsg)
		return
	}
	err := cli.Consume()
	if err != nil {
		c.sendError(targetID, err.Error())
		return
	}
	fmt.Println("consumed from topic : ")
}
func (c *CommandProcessor) stopConsumeMessage(targetID string) {
	fmt.Println("Arrived stop consuming msg")
	cli, ok := c.GetConsumerClient(targetID)
	if !ok {
		errMsg := fmt.Sprintf("cannot find consumer: %s", targetID)
		c.sendError(targetID, errMsg)
		return
	}
	cli.StopConsume()
	fmt.Println("stop consuming from topic : ")
}
