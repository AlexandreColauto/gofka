package producer

import (
	"context"
	cr "crypto/rand"
	"fmt"
	"sync"
	"time"

	"github.com/alexandrecolauto/gofka/client/config"
	"github.com/alexandrecolauto/gofka/common/model"
	vC "github.com/alexandrecolauto/gofka/common/pkg/visualizer_client"
	pb "github.com/alexandrecolauto/gofka/common/proto/broker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
)

type Producer struct {
	id        string
	bootstrap Bootstrap
	cluster   Cluster
	messages  *Messages
	acks      pb.ACKLevel

	waitCh  chan any
	waiting bool

	produceCh       chan any
	producing       bool
	visualizeClient *vC.VisualizerClient

	shutdownOnce sync.Once
	shutdownCh   chan any
	wg           sync.WaitGroup
	isShutdown   bool

	mu sync.RWMutex
}

type Messages struct {
	topic     string
	partition *StickyPartition
	batches   map[int32]*MessageBatch
}
type StickyPartition struct {
	partition int
	expires   time.Time
}

func NewProducer(config *config.Config) *Producer {
	shutdownCh := make(chan any)
	producerID := generateProducerID()
	mt := model.NewClusterMetadata(shutdownCh)
	b := make(map[int32]*MessageBatch)
	cc := make(map[string]pb.ProducerServiceClient)
	msgs := &Messages{
		topic:   config.Producer.Topic,
		batches: b,
	}

	boot := Bootstrap{
		address: config.Producer.BootstrapAddress,
	}
	acks := getAcks(config)
	clu := Cluster{
		metadata: mt,
		clients:  cc,
	}
	p := &Producer{bootstrap: boot, cluster: clu, messages: msgs, acks: acks, id: producerID}

	if config.Producer.Visualizer.Enabled {
		p.createVisualizerClient(config.Producer.Visualizer.Address)
	}

	if p.visualizeClient != nil {
		p.visualizeClient.Processor.RegisterClient(producerID, p)
	}

	p.wg.Add(1)
	go p.startMetadataFetcher()
	return p
}

func (p *Producer) UpdateTopic(topic string) {
	p.messages.topic = topic
	p.messages.partition = nil
	fmt.Println("topic updated: ", p.messages.topic)
}

func (p *Producer) GetClientId() string {
	return p.id
}

func (p *Producer) ConnectToBroker() {
	conn, err := grpc.NewClient(p.bootstrap.address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		panic(err)
	}

	cli := pb.NewProducerServiceClient(conn)
	p.bootstrap.connection = conn
	p.bootstrap.client = cli

	if p.visualizeClient != nil {
		action := "alive"
		target := p.id
		msg := fmt.Sprintf("controller %s just become alive", target)
		p.visualizeClient.SendMessage(action, target, []byte(msg))
	}
}

func (p *Producer) SendMessage(key, value string) error {
	if p.messages.topic == "" {
		return fmt.Errorf("empty topic, set one first")
	}
	partition, err := p.getPartition(key)
	if err != nil {
		return err
	}
	// fmt.Println("counter: ", p.messages.roundRobinCounter)
	return p.addMessageToBatch(partition, key, value)
}

func (p *Producer) CreateTopic(topic string, n_partitions int, replication int) error {
	return p.createTopicAtBroker(topic, n_partitions, replication)
}

func (p *Producer) addMessageToBatch(partition int, key, value string) error {
	b := p.getCurrentBatchFor(partition)
	msg := &pb.Message{
		Partition: int32(partition),
		Topic:     p.messages.topic,
		Key:       key,
		Value:     value,
	}
	b.Messages = append(b.Messages, msg)
	if len(b.Messages) >= b.MaxMsg {
		fmt.Println("max msg arrived flushing")
		p.flush()
	}
	return nil
}

func (p *Producer) flush() {
	if p.isShutdown {
		return
	}
	var batchesToDispach []*MessageBatch
	p.mu.Lock()
	for _, batch := range p.messages.batches {
		if len(batch.Messages) >= batch.MaxMsg || time.Since(batch.Lifetime) > 0 {
			if p.isShutdown {
				return
			}
			batchesToDispach = append(batchesToDispach, batch)
		}
	}
	p.mu.Unlock()
	for _, batch := range batchesToDispach {
		batch.Done = p.dispatchBatch(batch)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, batch := range batchesToDispach {
		if batch.Done {
			delete(p.messages.batches, batch.Partition)
		}
	}

}

func (p *Producer) dispatchBatch(batch *MessageBatch) bool {
	fmt.Println("dispatching batch")
	leader, err := p.findLeader(batch)
	if err != nil {
		fmt.Println("err finding leader: ", err)
		return false
	}
	if leader == "" {
		fmt.Println("cannot find leader")
		return false
	}
	err = p.sendBatchMessageToBroker(leader, batch)
	if err != nil {
		fmt.Println("err sending Message: ", err)
		return false
	}
	return true
}

func (p *Producer) sendBatchMessageToBroker(brokerID string, batch *MessageBatch) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req := &pb.SendBatchRequest{
		Topic:     p.messages.topic,
		Partition: batch.Partition,
		Ack:       p.acks,
		Messages:  batch.Messages,
	}

	cli, err := p.getBrokerClient(brokerID)
	if err != nil {
		return err
	}
	res, err := cli.HandleSendBatch(ctx, req, grpc.UseCompressor(gzip.Name))
	if err != nil {
		fmt.Println("client - send msg erro: ", err)
		fn := func() error { return p.sendBatchMessageToBroker(brokerID, batch) }
		return p.checkErrAndRedirect(err, fn)
	}
	if !res.Success {
		return fmt.Errorf("%s", res.ErrorMsg)
	}
	fmt.Println("Message sent")

	return nil
}

func (p *Producer) createVisualizerClient(address string) {
	nodeType := "producer"
	viCli := vC.NewVisualizerClient(nodeType, address)
	p.visualizeClient = viCli
}

func (p *Producer) Produce() error {
	p.communicate("start-producing")
	if p.producing {
		return nil
	}
	p.produceCh = make(chan any)
	p.producing = true
	go p.produce()
	return nil
}
func (p *Producer) produce() {
	defer p.communicate("stop-producing")
	ticker := time.NewTicker(250 * time.Millisecond)
	for {
		select {
		case <-p.produceCh:
			p.producing = false
			return
		case <-ticker.C:
			err := p.SendMessage("", "test-value")
			if err != nil {
				return
			}
		}
	}
}
func (p *Producer) StopProducing() {
	if p.producing {
		close(p.produceCh)
	}
}

func (p *Producer) communicate(action string) {
	if p.visualizeClient != nil {
		target := p.id
		msg := fmt.Sprintf("controller %s just become alive", target)
		p.visualizeClient.SendMessage(action, target, []byte(msg))
	}
}
func (p *Producer) Shutdown() error {
	var shutErr error
	p.shutdownOnce.Do(func() {
		if p.isShutdown {
			return
		}
		p.isShutdown = true

		close(p.shutdownCh)

		done := make(chan any)

		go func() {
			p.wg.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			shutErr = fmt.Errorf("timeout: some goroutines didin't finish within 5 seconds")
		}

	})
	return shutErr
}
func generateProducerID() string {
	timestamp := time.Now().UnixNano()
	randomID := generateShortID()
	return fmt.Sprintf("producer-%d-%s", timestamp, randomID)
}

func generateShortID() string {
	bytes := make([]byte, 4)
	cr.Read(bytes)
	return fmt.Sprintf("%x", bytes)
}

func getAcks(config *config.Config) pb.ACKLevel {
	switch config.Producer.ACKS {
	case "0":
		return pb.ACKLevel_ACK_0
	case "1":
		return pb.ACKLevel_ACK_1
	case "all":
		return pb.ACKLevel_ACK_ALL
	default:
		return pb.ACKLevel_ACK_1
	}
}
