package producer

import (
	"context"
	"fmt"
	"hash/fnv"
	"log"
	"strings"
	"time"

	"github.com/alexandrecolauto/gofka/model"
	pb "github.com/alexandrecolauto/gofka/proto/broker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/status"
)

type Producer struct {
	grpcConn      *grpc.ClientConn
	grpcClient    pb.ProducerServiceClient
	clusterClient map[string]pb.ProducerServiceClient

	roundRobinCounter int

	batches map[int32]*MessageBatch

	clusterMetadata *model.ClusterMetadata
	topic           string
	brokerAddress   string
	waitCh          chan any
	waiting         bool
}

type MessageBatch struct {
	Topic     string
	Partition int32
	MaxMsg    int
	Lifetime  time.Time
	Duration  time.Duration
	Done      bool
	Messages  []*pb.Message
	flush     func()
}

func NewProducer(topic string, brokerAddress string) *Producer {
	mt := model.NewClusterMetadata()
	b := make(map[int32]*MessageBatch)
	cc := make(map[string]pb.ProducerServiceClient)
	p := &Producer{topic: topic, brokerAddress: brokerAddress, clusterMetadata: mt, batches: b, clusterClient: cc}
	go p.startMetadataFetcher()
	return p
}

func NewMessageBatch(duration time.Duration, flush func()) *MessageBatch {
	m := &MessageBatch{MaxMsg: 10, Lifetime: time.Now().Add(duration), Duration: duration, flush: flush}
	go m.flushTimer()
	return m
}

func (m *MessageBatch) flushTimer() {
	time.Sleep(m.Duration)
	if !m.Done {
		m.flush()
	}
}

func (p *Producer) ConnectToBroker() error {
	conn, err := grpc.NewClient(p.brokerAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return err
	}

	cli := pb.NewProducerServiceClient(conn)
	p.grpcClient = cli
	return nil
}

func (p *Producer) startMetadataFetcher() {
	ticker := time.NewTicker(250 * time.Millisecond)
	for _ = range ticker.C {
		p.syncMetadata()
	}

}
func (p *Producer) syncMetadata() {
	if p.grpcClient == nil {
		if err := p.ConnectToBroker(); err != nil {
			fmt.Println("err connecting to broker: ", err)
			return
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := &pb.FetchMetadataRequest{
		LastIndex: p.clusterMetadata.Metadata.LastIndex,
	}
	res, err := p.grpcClient.FetchMetadata(ctx, req)
	if err != nil {
		panic(err)
	}
	if !res.Success {
		// fmt.Println("Failed to update metadata: ", res.ErrorMsg)
		return
	}
	mt := res.Metadata
	p.clusterMetadata.UpdateMetadata(mt)
	_, ok := p.clusterMetadata.Metadata.Topics[p.topic]
	if p.waiting && ok {
		fmt.Println("Cosing ch")
		close(p.waitCh)
	}
}

func (p *Producer) SendMessage(key, value string) error {
	partition, err := p.getPartition(key)
	if err != nil {
		return err
	}
	return p.addMessageToBatch(partition, key, value)
}

func (p *Producer) getPartition(key string) (int, error) {
	n_parts, err := p.clusterMetadata.PartitionCount(p.topic)
	if err != nil {
		p.autoCreateTopic(p.topic)
		fmt.Println("Cannot find topic yet, create first")
		return p.getPartition(key)
	}
	fmt.Println("found n parts: ", n_parts)
	if key == "" {
		p.roundRobinCounter++
		return int(p.roundRobinCounter % n_parts), nil
	}
	hasher := fnv.New32a()
	hasher.Write([]byte(key))
	return int(hasher.Sum32()) % n_parts, nil
}

func (p *Producer) addMessageToBatch(partition int, key, value string) error {
	b := p.getCurrentBatchFor(partition)
	msg := &pb.Message{
		Partition: int32(partition),
		Topic:     p.topic,
		Key:       key,
		Value:     value,
	}
	b.Messages = append(b.Messages, msg)
	if len(b.Messages) == b.MaxMsg {
		p.flush()
	}
	return nil
}

func (p *Producer) getCurrentBatchFor(partition int) *MessageBatch {
	bat, ok := p.batches[int32(partition)]
	if !ok {
		return p.newBatch(partition)
	}
	return bat
}

func (p *Producer) newBatch(partition int) *MessageBatch {
	b := NewMessageBatch(1*time.Second, p.flush)
	b.Partition = int32(partition)
	b.Topic = p.topic
	p.batches[int32(partition)] = b
	return b
}

func (p *Producer) flush() {
	for _, batch := range p.batches {
		if len(batch.Messages) >= batch.MaxMsg || time.Now().Sub(batch.Lifetime) > 0 {
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

func (p *Producer) findLeader(batch *MessageBatch) (string, error) {
	t, ok := p.clusterMetadata.Metadata.Topics[batch.Topic]
	if !ok {
		p.autoCreateTopic(batch.Topic)
		return "", fmt.Errorf("cannot find topic: %s", batch.Topic)
	}
	part, ok := t.Partitions[batch.Partition]
	if !ok {
		return "", fmt.Errorf("cannot find partition on topic %s: %d", batch.Topic, batch.Partition)
	}
	return part.Leader, nil
}

func (p *Producer) autoCreateTopic(topic string) error {
	fmt.Println("Auto creating new topic")
	p.waiting = true
	p.waitCh = make(chan any)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := &pb.CreateTopicRequest{
		Topic:     p.topic,
		Partition: 1,
	}
	res, err := p.grpcClient.HandleCreateTopic(ctx, req)
	if err != nil {
		fmt.Println("error creating topic: ", err.Error())
		return err
	}
	if !res.Success {
		fmt.Println("error creating topic2: ", res.ErrorMsg)
		return fmt.Errorf(res.ErrorMsg)
	}
	<-p.waitCh
	fmt.Println("Resuming")
	return nil
}

func (p *Producer) sendBatchMessageToBroker(brokerID string, batch *MessageBatch) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req := &pb.SendBatchRequest{
		Topic:     p.topic,
		Partition: batch.Partition,
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
		return fmt.Errorf(res.ErrorMsg)
	}
	fmt.Println("Message sent")

	return nil
}

func (p *Producer) getBrokerClient(brokerID string) (pb.ProducerServiceClient, error) {
	cli, ok := p.clusterClient[brokerID]
	if !ok {
		return p.connectToBroker(brokerID)
	}
	return cli, nil
}
func (p *Producer) connectToBroker(brokerID string) (pb.ProducerServiceClient, error) {
	brokerInfo, ok := p.clusterMetadata.Metadata.Brokers[brokerID]
	if !ok {
		return nil, fmt.Errorf("cannot find broker in metadata %s", brokerID)
	}
	address := brokerInfo.Address
	conn, err := grpc.NewClient(address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("cannot create broker client", err)
	}
	cli := pb.NewProducerServiceClient(conn)
	p.clusterClient[brokerID] = cli
	return cli, nil
}

func (s *Producer) checkErrAndRedirect(err error, fun func() error) error {
	if st, ok := status.FromError(err); ok {
		if st.Code() == codes.FailedPrecondition {
			// Parse the error message for leader info
			parts := strings.Split(st.Message(), "|")
			if len(parts) == 3 && parts[0] == "not leader" {
				leaderID := parts[1]
				leaderAddr := parts[2]

				log.Printf("Redirecting to leader %s at %s", leaderID, leaderAddr)
				s.updateAddress(leaderAddr)
				return fun()
			}
		}
	}
	return err
}
func (p *Producer) updateAddress(address string) {
	p.brokerAddress = address
}
