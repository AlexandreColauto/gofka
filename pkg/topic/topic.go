package topic

import (
	"fmt"

	"github.com/alexandrecolauto/gofka/proto/broker"
)

type Topic struct {
	Name         string
	partitions   []*Partition
	N_partitions int

	roundRobinCounter int64
}

func NewTopic(name string, n_partitions int) (*Topic, error) {
	if n_partitions <= 0 {
		n_partitions = 1
	}
	partitions := make([]*Partition, n_partitions)
	for i := range n_partitions {
		p, err := NewPartition(name, i)
		if err != nil {
			return nil, err

		}
		partitions[i] = p

	}
	return &Topic{Name: name, partitions: partitions, N_partitions: n_partitions}, nil
}

func (t *Topic) Partitions() []*Partition {
	return t.partitions
}

func (t *Topic) PartitionInfo(id int) (hwm int64, leo int64, err error) {
	if id >= t.N_partitions {
		err = fmt.Errorf("cannot find partition %d, bigger than available parts %d", id, len(t.partitions))
		return
	}
	p := t.partitions[id]
	hwm = p.hwm
	leo = p.leo
	return
}

func (t *Topic) AppendBatch(partition int, batch []*broker.Message) error {
	if len(batch) == 0 {
		return fmt.Errorf("empty message")
	}
	p := t.partitions[partition]
	_, err := p.AppendBatch(batch)
	return err
}

func (t *Topic) ReadFromPartition(p_id, offset int, opt *broker.ReadOptions) ([]*broker.Message, error) {
	if p_id < 0 || p_id >= t.N_partitions {
		return nil, nil
	}
	p := t.partitions[p_id]
	items, err := p.ReadFrom(int64(offset), opt)
	if err != nil {
		return nil, err
	}
	for _, item := range items {
		item.Topic = t.Name
		item.Partition = int32(p_id)
	}
	return items, nil
}

func (t *Topic) ReadFromPartitionReplica(p_id, offset int, opt *broker.ReadOptions) ([]*broker.Message, error) {
	if p_id < 0 || p_id >= t.N_partitions {
		return nil, nil
	}
	p := t.partitions[p_id]
	items, err := p.ReadFromReplica(int64(offset), opt)
	if err != nil {
		return nil, err
	}
	for _, item := range items {
		item.Topic = t.Name
		item.Partition = int32(p_id)
	}
	return items, nil
}

func (t *Topic) UpdateFollowerState(followerID string, p_id int, fetchOffset, leo int64) error {
	if p_id < 0 || p_id >= t.N_partitions {
		return nil
	}
	p := t.partitions[p_id]
	return p.UpdateFollowersState(followerID, fetchOffset, leo)
}

func (t *Topic) GetPartition(index int) (*Partition, error) {
	if index >= len(t.partitions) {
		return nil, fmt.Errorf("cannot find partition")
	}
	return t.partitions[index], nil
}
