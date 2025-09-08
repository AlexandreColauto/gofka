package broker

import (
	"fmt"
	"time"

	"github.com/alexandrecolauto/gofka/common/proto/broker"
	"github.com/alexandrecolauto/gofka/common/proto/controller"
)

func (g *GofkaBroker) FetchMessagesReplica(topic string, partitionID int, offset int64, opt *broker.ReadOptions) (*broker.FetchRecordsResponse, error) {
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
	hwm, leo, err := t.PartitionInfo(partitionID)
	if err != nil {
		return nil, err
	}
	res := &broker.FetchRecordsResponse{
		Message:       msgs,
		Highwatermark: hwm,
		Longendoffset: leo,
		Success:       true,
	}

	return res, nil
}

func (g *GofkaBroker) UpdateFollowerState(topic, followerID string, partitionID int, fetchOffset, longEndOffset int64) error {
	t, err := g.GetTopic(topic)
	if err != nil {
		return err
	}
	err = t.UpdateFollowerState(followerID, partitionID, fetchOffset, longEndOffset)
	if err != nil {
		return err
	}

	return nil
}

func (g *GofkaBroker) waitForReplicas(topic string, partition int, lastOffset int) error {
	fmt.Println("waiting for replicas")
	t, err := g.GetTopic(topic)
	initalBackoff := 250 * time.Millisecond
	retries := 0
	if err != nil {
		return err
	}
	var last_hwm int64
	for retries <= 5 {
		parts := t.Partitions()
		p := parts[partition]
		hwm := p.HWM()
		last_hwm = hwm
		if hwm >= int64(lastOffset) {
			fmt.Println("everyone in sync!", last_hwm, lastOffset)
			return nil
		}
		fmt.Printf("retry #%d, current hwm %d lastoffset %d\n", retries, last_hwm, lastOffset)
		time.Sleep(initalBackoff)
		retries++
		initalBackoff *= 2
	}
	return fmt.Errorf("cannot sync messages, last highwatermark: %d message offset %d", last_hwm, lastOffset)
}

func (g *GofkaBroker) findLaggingReplicas() []*controller.AlterPartition {
	partitionsToAlter := []*controller.AlterPartition{}
	g.mu.RLock()
	topics := g.internalTopics.topics
	g.mu.RUnlock()
	for _, t := range topics {
		for _, p := range t.Partitions() {
			if p.Leader() {
				isr := p.Isr()
				req := &controller.AlterPartition{
					Topic:     t.Name,
					Partition: int64(p.ID()),
					NewIsr:    isr,
				}
				partitionsToAlter = append(partitionsToAlter, req)
			}
		}
	}
	return partitionsToAlter
}
