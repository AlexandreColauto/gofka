package broker

import (
	"fmt"
	"time"

	"github.com/alexandrecolauto/gofka/common/pkg/topic"
)

type BrokerTopics struct {
	topics         map[string]*topic.Topic
	maxLagTimeout  time.Duration
	batchTimeout   time.Duration
	maxBatchMsg    int
	segmentBytes   int64
	indexInterval  int32
	retentionBytes int64
	retentionTime  time.Duration
}

func (g *GofkaBroker) createTopicInternal(name string, n_parts int) error {
	config := topic.LogConfig{
		MaxBatchMsg:    g.internalTopics.maxBatchMsg,
		BatchTimeout:   g.internalTopics.batchTimeout,
		SegmentBytes:   g.internalTopics.segmentBytes,
		IndexInterval:  g.internalTopics.indexInterval,
		RetentionBytes: g.internalTopics.retentionBytes,
		RetentionTime:  g.internalTopics.retentionTime,
	}
	t, err := topic.NewTopic(name, n_parts, g.shutdownCh, &config)
	if err != nil {
		return err
	}
	g.mu.Lock()
	g.internalTopics.topics[name] = t
	g.mu.Unlock()
	for _, part := range t.Partitions() {
		g.replicaManager.AddPartition(name, part)
		offset, err := t.GetLEO(part.ID())
		if err != nil {
			return err
		}
		g.clusterMetadata.metadata.UpdateOffset(t.Name, part.ID(), offset)
	}
	return nil
}

func (g *GofkaBroker) GetTopic(topic string) (*topic.Topic, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	t, ok := g.internalTopics.topics[topic]
	if !ok {
		return nil, fmt.Errorf("cannot find topic %s", topic)
	}
	return t, nil

}
