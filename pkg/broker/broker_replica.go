package broker

import (
	"fmt"

	"github.com/alexandrecolauto/gofka/proto/broker"
)

func (g *GofkaBroker) FetchMessagesReplica(topic string, partitionID int, offset int64, opt *broker.ReadOptions) ([]*broker.Message, error) {
	t, err := g.GetTopic(topic)
	if err != nil {
		return nil, fmt.Errorf("cannot find topic: %s - %w", topic, err)
	}

	if partitionID >= t.N_partitions {
		return nil, fmt.Errorf("cannot find partition: %d", partitionID)
	}

	msgs, err := t.ReadFromPartitionReplica(partitionID, int(offset), opt)
	if err != nil {
		return nil, fmt.Errorf("error reading from replica: %w", err)
	}

	return msgs, nil
}

func (g *GofkaBroker) UpdateFollowerState(topic, followerID string, partitionID int, fetchOffset, longEndOffset int64) error {
	t, err := g.GetTopic(topic)
	if err != nil {
		return err
	}
	err = t.UpdateFollowerState(followerID, partitionID, fetchOffset, longEndOffset)
	return err
}
