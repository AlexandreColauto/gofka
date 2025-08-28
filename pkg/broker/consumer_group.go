package broker

import (
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/alexandrecolauto/gofka/proto/broker"
)

type ConsumerGroup struct {
	id       string
	leaderId string
	topics   map[string]*Topic
	offsets  map[OffsetKey]int

	topicList  map[string]bool
	consumers  map[string]*ConsumerSession
	joining    bool
	inSync     bool
	consumerCh chan *ConsumerSession
	doneChList []chan any
	mu         sync.RWMutex
}

type OffsetKey struct {
	GroupID   string
	Topic     string
	Partition int
}

func NewConsumerGroup(id string) *ConsumerGroup {
	co := make(map[string]*ConsumerSession)
	ts := make(map[string]*Topic)
	tl := make(map[string]bool)
	of := make(map[OffsetKey]int)
	c_ch := make(chan *ConsumerSession)
	d_ch := make([]chan any, 0)
	return &ConsumerGroup{id: id, topics: ts, consumers: co, offsets: of, consumerCh: c_ch, doneChList: d_ch, topicList: tl}
}

func (cg *ConsumerGroup) ResetConsumerGroup(doneCh chan any) {
	cg.doneChList = append(cg.doneChList, doneCh)
	if cg.joining {
		return
	}
	fmt.Println("Consumer group reseted")
	co := make(map[string]*ConsumerSession)
	cg.consumers = co
	cg.joining = true
	cg.inSync = false
	timeout := time.NewTicker(5500 * time.Millisecond)
	for {
		select {
		case con := <-cg.consumerCh:
			fmt.Println("New consumer joined: ", con)
			if cg.leaderId == "" {
				cg.leaderId = con.id
			}
			con.last_heartbeat = time.Now()
			cg.AddTopics(con.topics)
			cg.consumers[con.id] = con

		case <-timeout.C:
			for _, d_ch := range cg.doneChList {
				fmt.Println("finished ")
				close(d_ch)
			}
			cg.joining = false
			return
		}
	}
}

func (cg *ConsumerGroup) AddTopics(topics []string) {
	for _, tp := range topics {
		cg.topicList[tp] = true
	}
}

func (cg *ConsumerGroup) AddConsumer(id string, topics []string) {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	c, ok := cg.consumers[id]
	if !ok {
		c = NewConsumerSession(id, topics)
	}
	c.last_heartbeat = time.Now()
	cg.consumerCh <- c
}

func (cg *ConsumerGroup) GetRegisterResponse() *broker.RegisterConsumerResponse {
	topicList := make([]string, 0)
	for topic := range cg.topicList {
		topicList = append(topicList, topic)
	}
	conumers := make([]*broker.ConsumerSession, 0)
	for _, consumer := range cg.consumers {
		conumers = append(conumers, &broker.ConsumerSession{
			Id:     consumer.id,
			Topics: consumer.topics,
		})
	}
	res := &broker.RegisterConsumerResponse{
		Success:   true,
		Leader:    cg.leaderId,
		AllTopics: topicList,
		Consumers: conumers,
	}
	return res
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

	var all_partitions []*broker.PartitionInfo
	for topicName, topic := range cg.topics {
		for i := range topic.n_partitions {
			all_partitions = append(all_partitions, &broker.PartitionInfo{Id: int32(i), TopicName: topicName})

		}
	}

	sort.Slice(all_partitions, func(i, j int) bool {
		if all_partitions[i].TopicName == all_partitions[j].TopicName {
			return all_partitions[i].Id < all_partitions[j].Id
		}
		return all_partitions[i].TopicName < all_partitions[j].TopicName
	})

	consumerIDs := make([]string, 0, len(cg.consumers))
	for id := range cg.consumers {
		consumerIDs = append(consumerIDs, id)
	}
	sort.Strings(consumerIDs)

	for _, consumer := range cg.consumers {
		consumer.partitions = make([]*broker.PartitionInfo, 0)
	}

	for i, partition := range all_partitions {
		consumerIdx := i % len(consumerIDs)
		consumerID := consumerIDs[consumerIdx]
		consumer := cg.consumers[consumerID]
		consumer.partitions = append(consumer.partitions, partition)
		fmt.Printf("Assigning partition %v to user\n", partition)
	}
}

func (cg *ConsumerGroup) GetAssignedPartitions(consumerID string) []*broker.PartitionInfo {
	cg.mu.RLock()
	defer cg.mu.RUnlock()

	if consumer, ok := cg.consumers[consumerID]; ok {
		partitions := make([]*broker.PartitionInfo, len(consumer.partitions))
		copy(partitions, consumer.partitions)
		return partitions
	}
	return nil
}

func (cg *ConsumerGroup) FetchMessages(id string, opt *broker.ReadOptions) ([]*broker.Message, error) {
	c := cg.consumers[id]
	var msgs []*broker.Message
	for _, part := range c.partitions {
		topic := cg.topics[part.TopicName]
		log.Println("fetching msg for:", part.TopicName)
		key := OffsetKey{Topic: part.TopicName, Partition: int(part.Id), GroupID: cg.id}
		offset := cg.offsets[key]
		var items []*broker.Message
		items, err := topic.ReadFromPartition(int(part.Id), offset, opt)
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, items...)
	}
	fmt.Println("Final messages", len(msgs), len(c.partitions))
	if len(msgs) > 0 {
		return msgs, nil
	}
	return nil, nil
}

func (cg *ConsumerGroup) SyncGroup(consumers []*broker.ConsumerSession) {
	for _, consumer := range consumers {
		for _, ass := range consumer.Assignments {
			cg.consumers[consumer.Id].partitions = append(cg.consumers[consumer.Id].partitions, ass)
		}
	}
	cg.inSync = true
}
func (cg *ConsumerGroup) UserAssignment(user_id string, retries int) (*broker.ConsumerSession, error) {
	if !cg.inSync {
		time.Sleep(time.Duration(retries) * 100 * time.Millisecond)
		if retries < 5 {
			retries++
			return cg.UserAssignment(user_id, retries)
		} else {
			return nil, fmt.Errorf("max retries without syncgroup of leader")
		}
	}

	cons, ok := cg.consumers[user_id]
	if !ok {
		return nil, fmt.Errorf("cannot find consumer with id: %s", user_id)
	}
	cs := &broker.ConsumerSession{
		Id:          cons.id,
		Leader:      cons.id == cg.leaderId,
		Topics:      cons.topics,
		Assignments: cons.partitions,
	}

	return cs, nil
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

func (cg *ConsumerGroup) ConsumerHeartbeat(id string) {
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
