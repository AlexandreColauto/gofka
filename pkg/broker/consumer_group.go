package broker

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

type ConsumerGroup struct {
	id        string
	topics    map[string]*Topic
	consumers map[string]*ConsumerSession
	offsets   map[OffsetKey]int
	mu        sync.RWMutex
}

type OffsetKey struct {
	GroupID   string
	Topic     string
	Partition int
}

func NewConsumerGroup(id string) *ConsumerGroup {
	co := make(map[string]*ConsumerSession)
	ts := make(map[string]*Topic)
	of := make(map[OffsetKey]int)
	return &ConsumerGroup{id: id, topics: ts, consumers: co, offsets: of}
}

func (cg *ConsumerGroup) AddConsumer(id string, msg_ch chan []*Message) {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	c, ok := cg.consumers[id]
	if !ok {
		c = NewConsumerSession(id)
	}
	c.last_heartbeat = time.Now()
	c.msg_ch = msg_ch
	cg.consumers[id] = c
	cg.rebalance()
}

func (cg *ConsumerGroup) RemoveConsumer(id string, topic *Topic) {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	if _, ok := cg.consumers[id]; ok {
		delete(cg.consumers, id)
		if len(cg.consumers) > 0 {
			cg.rebalance()
		}
	}
}

func (cg *ConsumerGroup) rebalance() {
	if len(cg.consumers) == 0 {
		return
	}

	var all_partitions []TopicPartition
	for topicName, topic := range cg.topics {
		for i := range topic.n_partitions {
			all_partitions = append(all_partitions, TopicPartition{Topic: topicName, Partition: i})

		}
	}

	sort.Slice(all_partitions, func(i, j int) bool {
		if all_partitions[i].Topic == all_partitions[j].Topic {
			return all_partitions[i].Partition < all_partitions[j].Partition
		}
		return all_partitions[i].Topic < all_partitions[j].Topic
	})

	consumerIDs := make([]string, 0, len(cg.consumers))
	for id := range cg.consumers {
		consumerIDs = append(consumerIDs, id)
	}
	sort.Strings(consumerIDs)

	for _, consumer := range cg.consumers {
		consumer.partitions = make([]TopicPartition, 0)
	}

	for i, partition := range all_partitions {
		consumerIdx := i % len(consumerIDs)
		consumerID := consumerIDs[consumerIdx]
		consumer := cg.consumers[consumerID]
		consumer.partitions = append(consumer.partitions, partition)
		fmt.Printf("Assigning partition %v to user\n", partition)
	}
}

func (cg *ConsumerGroup) GetAssignedPartitions(consumerID string) []TopicPartition {
	cg.mu.RLock()
	defer cg.mu.RUnlock()

	if consumer, ok := cg.consumers[consumerID]; ok {
		partitions := make([]TopicPartition, len(consumer.partitions))
		copy(partitions, consumer.partitions)
		return partitions
	}
	return nil
}

func (cg *ConsumerGroup) FetchMessages(id string) {
	c := cg.consumers[id]
	var msgs []*Message
	for _, part := range c.partitions {
		topic := cg.topics[part.Topic]
		key := OffsetKey{Topic: part.Topic, Partition: part.Partition, GroupID: cg.id}
		offset := cg.offsets[key]
		items := topic.ReadFromPartition(part.Partition, offset)
		msgs = append(msgs, items...)
	}
	fmt.Println("Final messages", len(msgs))
	if len(msgs) > 0 {
		c.msg_ch <- msgs
	}
}

func (cg *ConsumerGroup) UpdateOffset(topic string, partition, offset int) {
	key := OffsetKey{
		Topic:     topic,
		Partition: partition,
		GroupID:   cg.id,
	}
	fmt.Printf("New offset for %s, p: %d, offset: %d\n", topic, partition, offset)
	cg.offsets[key] = offset
}

func (cg *ConsumerGroup) Subscribe(topic *Topic) {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	cg.topics[topic.name] = topic
	cg.rebalance()
}
func (cg *ConsumerGroup) Unsubscribe(topic_name string) {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	delete(cg.topics, topic_name)
	cg.rebalance()
}

func (cg *ConsumerGroup) SendHeartbeat(id string) {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	c, ok := cg.consumers[id]
	if ok {
		c.last_heartbeat = time.Now()
	}
}

func (cg *ConsumerGroup) UnregisterConsumer(id string) {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	delete(cg.consumers, id)
	cg.rebalance()
}

func (cg *ConsumerGroup) ClearDeadSession() {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	deadSessionTimeout := 15 * time.Second
	now := time.Now()
	for _, consumer := range cg.consumers {
		if now.Sub(consumer.last_heartbeat) > deadSessionTimeout {
			cg.UnregisterConsumer(consumer.id)
		}
	}
}

func (cg *ConsumerGroup) DeleteTopic(topic_name string) {
	if _, ok := cg.topics[topic_name]; !ok {
		fmt.Println("cant find topic")
		return
	}
	delete(cg.topics, topic_name)
	cg.rebalance()
}
