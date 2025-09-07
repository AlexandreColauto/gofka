package producer

import (
	"context"
	"fmt"
	"hash/fnv"
	"math/rand"
	"time"

	"github.com/alexandrecolauto/gofka/common/model"
	pb "github.com/alexandrecolauto/gofka/common/proto/broker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Cluster struct {
	clients  map[string]pb.ProducerServiceClient
	metadata *model.ClusterMetadata
}

func (p *Producer) startMetadataFetcher() {
	defer p.wg.Done()
	ticker := time.NewTicker(250 * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			if p.isShutdown {
				return
			}
			p.syncMetadata()
		case <-p.shutdownCh:
			ticker.Stop()
			return
		}
	}
}

func (p *Producer) syncMetadata() {
	if p.bootstrap.client == nil {
		if err := p.ConnectToBootstrap(); err != nil {
			fmt.Println("err connecting to broker: ", err)
			return
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := &pb.FetchMetadataRequest{
		LastIndex: p.cluster.metadata.Metadata.LastIndex,
	}
	res, err := p.bootstrap.client.FetchMetadata(ctx, req)
	if err != nil {
		return
	}
	if !res.Success {
		// fmt.Println("Failed to update metadata: ", res.ErrorMsg)
		return
	}
	mt := res.Metadata
	p.cluster.metadata.UpdateMetadata(mt)
	_, ok := p.cluster.metadata.Metadata.Topics[p.messages.topic]
	if p.waiting && ok {
		fmt.Println("Cosing ch")
		close(p.waitCh)
	}
}

func (p *Producer) getPartition(key string) (int, error) {
	n_parts, err := p.cluster.metadata.PartitionCount(p.messages.topic)
	if err != nil {
		p.autoCreateTopic(p.messages.topic)
		fmt.Println("Cannot find topic yet, create first")
		return p.getPartition(key)
	}
	if key == "" {
		if p.messages.partition == nil {
			p.messages.partition = p.newStickyPartition(n_parts)
		}
		if time.Since(p.messages.partition.expires) > 0 {
			p.messages.partition = p.newStickyPartition(n_parts)
		}
		return p.messages.partition.partition, nil
	}
	hasher := fnv.New32a()
	hasher.Write([]byte(key))
	return int(hasher.Sum32()) % n_parts, nil
}

func (p *Producer) autoCreateTopic(topic string) error {
	fmt.Println("Auto creating new topic")
	p.waiting = true
	p.waitCh = make(chan any)
	n_partitions := 1
	replication := 1
	err := p.createTopicAtBroker(topic, n_partitions, replication)
	<-p.waitCh
	if err != nil {
		return err
	}
	fmt.Println("Resuming")
	return nil
}

func (p *Producer) createTopicAtBroker(topic string, n_partitions, replication int) error {
	fmt.Println("creating new topic", topic, n_partitions, replication)
	if n_partitions <= 0 {
		n_partitions = 1
	}
	if replication <= 0 {
		replication = 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := &pb.CreateTopicRequest{
		Topic:       topic,
		Partition:   int32(n_partitions),
		Replication: int32(replication),
	}
	res, err := p.bootstrap.client.HandleCreateTopic(ctx, req)
	if err != nil {
		fmt.Println("error creating topic: ", err.Error())
		return err
	}
	if !res.Success {
		fmt.Println("error creating topic2: ", res.ErrorMsg)
		return fmt.Errorf("%s", res.ErrorMsg)
	}
	return nil
}

func (p *Producer) findLeader(batch *MessageBatch) (string, error) {
	t, ok := p.cluster.metadata.Metadata.Topics[batch.Topic]
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

func (p *Producer) getBrokerClient(brokerID string) (pb.ProducerServiceClient, error) {
	cli, ok := p.cluster.clients[brokerID]
	if !ok {
		return p.connectToBroker(brokerID)
	}
	return cli, nil
}

func (p *Producer) connectToBroker(brokerID string) (pb.ProducerServiceClient, error) {
	brokerInfo, ok := p.cluster.metadata.Metadata.Brokers[brokerID]
	if !ok {
		return nil, fmt.Errorf("cannot find broker in metadata %s", brokerID)
	}
	address := brokerInfo.Address
	conn, err := grpc.NewClient(address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("cannot create broker client %s", err)
	}
	cli := pb.NewProducerServiceClient(conn)
	p.cluster.clients[brokerID] = cli
	return cli, nil
}
func (p *Producer) newStickyPartition(n int) *StickyPartition {
	part := rand.Intn(n)
	return &StickyPartition{
		partition: part,
		expires:   time.Now().Add(500 * time.Millisecond),
	}
}
