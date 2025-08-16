package broker

import (
	"fmt"
	"hash/fnv"
)

type Topic struct {
	name         string
	partitions   []*Partition
	n_partitions int
}

func NewTopic(name string, n_partitions int) *Topic {
	if n_partitions <= 0 {
		n_partitions = 1
	}
	partitions := make([]*Partition, n_partitions)
	for i := range n_partitions {
		partitions[i] = NewPartition(i)

	}
	return &Topic{name: name, partitions: partitions, n_partitions: n_partitions}
}

func (t *Topic) Append(message *Message) {
	if message == nil {
		return
	}
	p_id := t.getPartition(message)
	p := t.partitions[p_id]
	p.Append(message)
}

func (t *Topic) getPartition(message *Message) int {
	if message.Key == "" {
		return int(t.getTotalMessages() % int64(t.n_partitions))
	}
	hasher := fnv.New32a()
	hasher.Write([]byte(message.Key))
	return int(hasher.Sum32()) % t.n_partitions
}

func (t *Topic) getTotalMessages() int64 {
	var total int64
	for _, part := range t.partitions {
		part.mutex.Lock()
		total += int64(part.size)
		part.mutex.Unlock()
	}
	return total
}

func (t *Topic) ReadFromPartition(id, offset int) []*Message {
	if id < 0 || id >= t.n_partitions {
		return nil
	}
	p := t.partitions[id]
	items := p.ReadFrom(offset)
	for _, item := range items {
		item.UpdateMetadata(t.name, id)
	}
	return items
}
func (t *Topic) AddPartitions(new_partitions int) {
	if new_partitions <= t.n_partitions {
		fmt.Println("can only increase the number of partitions")
	}
	partitions := make([]*Partition, new_partitions)
	copy(partitions, t.partitions)
	for i := t.n_partitions; i < new_partitions; i++ {
		partitions[i] = NewPartition(i)
	}
	fmt.Println("New partitions: ", len(partitions))
	t.partitions = partitions
	t.n_partitions = new_partitions
}
