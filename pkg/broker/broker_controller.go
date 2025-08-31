package broker

import (
	"fmt"

	"github.com/alexandrecolauto/gofka/model"
	"github.com/alexandrecolauto/gofka/pkg/topic"
	"github.com/alexandrecolauto/gofka/proto/broker"
	pb "github.com/alexandrecolauto/gofka/proto/controller"
)

type GofkaBroker struct {
	clusterMetadata BrokerMetadata
	internalTopics  BrokerTopics
	consumerGroups  BrokerConsumerGroups
	replicaManager  *ReplicaManager
}

func NewBroker(brokerID string, cli BrokerClient) *GofkaBroker {
	mt := model.NewClusterMetadata()
	t := make(map[string]*topic.Topic)
	bt := BrokerTopics{topics: t}
	bmt := BrokerMetadata{metadata: mt}
	rm := NewReplicaManager(brokerID, cli)
	cg := BrokerConsumerGroups{}
	b := &GofkaBroker{
		clusterMetadata: bmt,
		internalTopics:  bt,
		replicaManager:  rm,
		consumerGroups:  cg,
	}
	b.scanDisk()
	return b
}

func (g *GofkaBroker) SendMessageBatch(topic string, partition int, batch []*broker.Message) error {
	t, err := g.GetTopic(topic)
	if err != nil {
		return err
	}
	return t.AppendBatch(partition, batch)
}

func (g *GofkaBroker) RegisterConsumer(id, group_id string, topics []string) *broker.RegisterConsumerResponse {
	cg := g.GetOrCreateConsumerGroup(group_id)
	doneCh := make(chan any)
	go cg.ResetConsumerGroup(doneCh)
	cg.AddConsumer(id, topics)
	<-doneCh
	res := cg.GetRegisterResponse(id)
	return res
}

func (g *GofkaBroker) FetchMessages(id string, topics []*broker.FromTopic, opt *broker.ReadOptions) ([]*broker.Message, error) {
	var msgs []*broker.Message
	for _, topic := range topics {
		t, err := g.GetTopic(topic.Topic)
		if err != nil {
			return nil, fmt.Errorf("Cannot find topic %s", topic.Topic)
		}
		var items []*broker.Message
		items, err = t.ReadFromPartition(int(topic.Partition), int(topic.Offset), opt)
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, items...)
	}
	if len(msgs) > 0 {
		return msgs, nil
	}
	return nil, nil
}

func (g *GofkaBroker) ProcessControllerLogs(logs []*pb.LogEntry) {
	for _, log := range logs {
		if log.Index > g.clusterMetadata.index {
			g.clusterMetadata.index = log.Index
		}
		if log.Command != nil {
			g.clusterMetadata.metadata.DecodeLog(log, g)
		}
	}
}
