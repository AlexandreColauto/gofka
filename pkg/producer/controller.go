package producer

import (
	"context"
	"crypto/rand"
	"fmt"
	"time"

	"github.com/alexandrecolauto/gofka/model"
	vC "github.com/alexandrecolauto/gofka/pkg/visualizer_client"
	pb "github.com/alexandrecolauto/gofka/proto/broker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
)

type Producer struct {
	id        string
	bootstrap Bootstrap
	cluster   Cluster
	messages  Messages
	acks      pb.ACKLevel

	waitCh  chan any
	waiting bool

	visualizeClient *vC.VisualizerClient
}

type Messages struct {
	topic             string
	roundRobinCounter int
	batches           map[int32]*MessageBatch
}

func NewProducer(topic string, brokerAddress string, acks pb.ACKLevel, vc *vC.VisualizerClient) *Producer {
	producerID := generateProducerID()
	mt := model.NewClusterMetadata()
	b := make(map[int32]*MessageBatch)
	cc := make(map[string]pb.ProducerServiceClient)
	msgs := Messages{
		topic:   topic,
		batches: b,
	}
	boot := Bootstrap{
		address: brokerAddress,
	}
	clu := Cluster{
		metadata: mt,
		clients:  cc,
	}
	p := &Producer{bootstrap: boot, cluster: clu, messages: msgs, acks: acks, id: producerID, visualizeClient: vc}
	go p.startMetadataFetcher()
	return p
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
	partition, err := p.getPartition(key)
	if err != nil {
		return err
	}
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
	if len(b.Messages) == b.MaxMsg {
		p.flush()
	}
	return nil
}

func (p *Producer) flush() {
	for _, batch := range p.messages.batches {
		if len(batch.Messages) >= batch.MaxMsg || time.Since(batch.Lifetime) > 0 {
			fmt.Println("Dispatch batch")
			batch.Done = p.dispatchBatch(batch)
		}
	}
}

func (p *Producer) dispatchBatch(batch *MessageBatch) bool {
	leader, err := p.findLeader(batch)
	fmt.Println("flushing batch to : ", batch, leader)
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

func generateProducerID() string {
	timestamp := time.Now().UnixNano()
	randomID := generateShortID()
	return fmt.Sprintf("producer-%d-%s", timestamp, randomID)
}

func generateShortID() string {
	bytes := make([]byte, 4)
	rand.Read(bytes)
	return fmt.Sprintf("%x", bytes)
}
