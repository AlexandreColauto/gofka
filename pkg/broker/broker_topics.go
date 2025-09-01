package broker

import (
	"fmt"
	"time"

	"github.com/alexandrecolauto/gofka/pkg/topic"
)

type BrokerTopics struct {
	topics        map[string]*topic.Topic
	maxLagTimeout time.Duration
}

func (g *GofkaBroker) createTopicInternal(name string, n_parts int) error {
	t, err := topic.NewTopic(name, n_parts)
	if err != nil {
		return err
	}
	g.internalTopics.topics[name] = t
	for _, part := range t.Partitions() {
		g.replicaManager.AddPartition(name, part)
	}
	return nil
}

func (g *GofkaBroker) GetTopic(topic string) (*topic.Topic, error) {
	t, ok := g.internalTopics.topics[topic]
	if !ok {
		return nil, fmt.Errorf("cannot find topic %s", topic)
	}
	return t, nil

}
