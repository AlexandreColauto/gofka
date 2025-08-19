package broker

import (
	"fmt"
	"hash/fnv"

	"github.com/alexandrecolauto/gofka/pkg/log"
)

type Topic struct {
	name         string
	partitions   []*Partition
	n_partitions int

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
	return &Topic{name: name, partitions: partitions, n_partitions: n_partitions}, nil
}

func (t *Topic) Append(message *log.Message) error {
	if message == nil {
		return fmt.Errorf("empty message")
	}
	p_id := t.getPartition(message)
	p := t.partitions[p_id]
	_, err := p.Append(message)
	if err != nil {
		return err
	}

	t.roundRobinCounter++
	return nil
}

func (t *Topic) getPartition(message *log.Message) int {
	if message.Key == "" {
		return int(t.roundRobinCounter % int64(t.n_partitions))
	}
	hasher := fnv.New32a()
	hasher.Write([]byte(message.Key))
	return int(hasher.Sum32()) % t.n_partitions
}

func (t *Topic) ReadFromPartition(p_id, offset int, opt *log.ReadOpts) ([]*log.Message, error) {
	if p_id < 0 || p_id >= t.n_partitions {
		return nil, nil
	}
	p := t.partitions[p_id]
	items, err := p.ReadFrom(int64(offset), opt)
	if err != nil {
		return nil, err
	}
	for _, item := range items {
		item.UpdateMetadata(t.name, p_id)
	}
	return items, nil
}

func (t *Topic) ReadFromPartitionReplica(p_id, offset int, opt *log.ReadOpts) ([]*log.Message, error) {
	if p_id < 0 || p_id >= t.n_partitions {
		return nil, nil
	}
	p := t.partitions[p_id]
	items, err := p.ReadFromReplica(int64(offset), opt)
	if err != nil {
		return nil, err
	}
	for _, item := range items {
		item.UpdateMetadata(t.name, p_id)
	}
	return items, nil
}

func (t *Topic) AddPartitions(new_partitions int) error {
	if new_partitions <= t.n_partitions {
		fmt.Println("can only increase the number of partitions")
	}
	partitions := make([]*Partition, new_partitions)
	copy(partitions, t.partitions)
	for i := t.n_partitions; i < new_partitions; i++ {
		p, err := NewPartition(t.name, i)
		if err != nil {
			return err
		}
		partitions[i] = p
	}
	fmt.Println("New partitions: ", len(partitions))
	t.partitions = partitions
	t.n_partitions = new_partitions
	return nil
}

func (t *Topic) UpdateFollowerState(followerID string, p_id int, fetchOffset, leo int64) error {
	if p_id < 0 || p_id >= t.n_partitions {
		return nil
	}
	p := t.partitions[p_id]
	return p.UpdateFollowersState(followerID, fetchOffset, leo)
}
