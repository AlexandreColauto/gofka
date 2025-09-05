package broker

import (
	"fmt"
	"os"

	"github.com/alexandrecolauto/gofka/common/model"
	"github.com/alexandrecolauto/gofka/common/proto/broker"
	pb "github.com/alexandrecolauto/gofka/common/proto/controller"
)

type BrokerMetadata struct {
	metadata      *model.ClusterMetadata
	index         int64
	pendingTopics map[string]chan any
}

func (g *GofkaBroker) scanDisk() error {
	files, err := os.ReadDir("data")
	if err != nil {
		return err
	}
	for _, file := range files {
		if file.IsDir() {
			topic := file.Name()
			if topic == "__cluster_metadata" {
				continue
			}
			topicDir, _ := os.ReadDir("data/" + topic)
			n_parts := 0
			for _, file := range topicDir {
				if file.IsDir() {
					n_parts++
				}
			}
			cmd := pb.Command_CreateTopic{
				CreateTopic: &pb.CreateTopicCommand{
					Topic:       topic,
					NPartitions: int32(n_parts),
				},
			}
			g.clusterMetadata.metadata.CreateTopic(cmd)
			err := g.createTopicInternal(topic, n_parts)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (g *GofkaBroker) TopicsMetadata(topics []string) ([]*broker.TopicInfo, error) {
	all_topics := g.clusterMetadata.metadata.Topics()
	if len(all_topics) == 0 {
		return nil, fmt.Errorf("there is no topic to look for")
	}
	res := make([]*broker.TopicInfo, 0)
	for _, topic := range topics {
		t, ok := all_topics[topic]
		if !ok {
			return nil, fmt.Errorf("cannot find metadata for %s", topic)
		}
		res = append(res, t)
	}

	return res, nil
}

func (g *GofkaBroker) CommitOffset(topics []*broker.FromTopic) error {
	return g.clusterMetadata.metadata.CommitOffset(topics)
}

func (g *GofkaBroker) ApplyRegisterBroker(ctc *pb.Command_RegisterBroker) {
	if g.replicaManager.brokerID == ctc.RegisterBroker.Id {
		return
	}
	brk := model.BrokerInfo{
		ID:       ctc.RegisterBroker.Id,
		Address:  ctc.RegisterBroker.Address,
		Alive:    ctc.RegisterBroker.Alive,
		LastSeen: ctc.RegisterBroker.LastSeen.AsTime(),
	}
	g.replicaManager.client.UpdateBroker(brk)
}

func (g *GofkaBroker) ApplyUpdateBroker(ctc *pb.Command_UpdateBroker) {
	if g.replicaManager.brokerID == ctc.UpdateBroker.Id {
		return
	}
	brk := model.BrokerInfo{
		ID:       ctc.UpdateBroker.Id,
		Address:  ctc.UpdateBroker.Address,
		Alive:    ctc.UpdateBroker.Alive,
		LastSeen: ctc.UpdateBroker.LastSeen.AsTime(),
	}
	g.replicaManager.client.UpdateBroker(brk)
}

func (g *GofkaBroker) ApplyCreateTopic(cmd *pb.Command_CreateTopic) {
	g.createTopicInternal(cmd.CreateTopic.Topic, int(cmd.CreateTopic.NPartitions))

	if ch, exists := g.clusterMetadata.pendingTopics[cmd.CreateTopic.Topic]; exists {
		fmt.Println("closing pending ch")
		close(ch)
	}
}

func (g *GofkaBroker) ApplyUpdatePartitionLeader(ctc *pb.Command_ChangePartitionLeader) {
	for _, asgn := range ctc.ChangePartitionLeader.Assignments {
		for _, replica := range asgn.NewReplicas {
			if replica == g.replicaManager.brokerID {
				g.replicaManager.HandleLeaderChange(asgn.TopicId, int(asgn.PartitionId), asgn.NewLeader, int64(asgn.NewEpoch), asgn.NewReplicas)
			}
		}
	}
}
