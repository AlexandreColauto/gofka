package broker

import (
	"fmt"
	"sync"
	"time"

	"github.com/alexandrecolauto/gofka/common/model"
	"github.com/alexandrecolauto/gofka/common/pkg/topic"
	vC "github.com/alexandrecolauto/gofka/common/pkg/visualizer_client"
	"github.com/alexandrecolauto/gofka/common/proto/broker"
	pb "github.com/alexandrecolauto/gofka/common/proto/controller"
	"google.golang.org/protobuf/proto"
)

type GofkaBroker struct {
	clusterMetadata  BrokerMetadata
	internalTopics   BrokerTopics
	consumerGroups   BrokerConsumerGroups
	replicaManager   *ReplicaManager
	visualizerClient *vC.VisualizerClient
	mu               sync.RWMutex
}

func NewBroker(brokerID string, cli BrokerClient, vc *vC.VisualizerClient) *GofkaBroker {
	mt := model.NewClusterMetadata()
	t := make(map[string]*topic.Topic)
	bt := BrokerTopics{topics: t, maxLagTimeout: 5 * time.Second}
	bmt := BrokerMetadata{metadata: mt}
	rm := NewReplicaManager(brokerID, cli)
	cg := NewBrokerConsumerGroup()
	b := &GofkaBroker{
		clusterMetadata:  bmt,
		internalTopics:   bt,
		replicaManager:   rm,
		consumerGroups:   cg,
		visualizerClient: vc,
	}
	b.scanDisk()
	go b.startSessionMonitor()
	return b
}

func (g *GofkaBroker) SendMessageBatch(topic string, partition int, batch []*broker.Message) error {
	fmt.Println("SENDING MSG", topic)
	t, err := g.GetTopic(topic)
	if err != nil {
		return err
	}
	err = t.AppendBatch(partition, batch)
	if err != nil {
		return err
	}
	offset, err := t.GetLEO(partition)
	if err != nil {
		return err
	}
	return g.clusterMetadata.metadata.UpdateOffset(topic, partition, offset)
}

func (g *GofkaBroker) SendMessageBatchAndWaitForReplicas(topic string, partition int, batch []*broker.Message) error {
	err := g.SendMessageBatch(topic, partition, batch)
	if err != nil {
		return err
	}
	lastMsg := batch[len(batch)-1]
	return g.waitForReplicas(topic, partition, int(lastMsg.Offset))
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
			if log.Command != nil {
				g.clusterMetadata.metadata.DecodeLog(log, g)
			}
		}
	}

	if g.visualizerClient != nil {
		action := "metadata"
		target := g.replicaManager.brokerID
		mt := g.clusterMetadata.metadata.FetchMetadata(0)
		// fmt.Println("returning metadata: ", mt)
		// for _, t := range mt.Topics {
		// 	for _, p := range t.Partitions {
		// 	}
		// }
		val, err := proto.Marshal(mt)
		if err != nil {
			return
		}
		msg := val
		g.visualizerClient.SendMessage(action, target, msg)
	}
}
