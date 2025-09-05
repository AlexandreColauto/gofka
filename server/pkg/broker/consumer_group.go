package broker

import (
	"fmt"
	"sync"
	"time"

	"github.com/alexandrecolauto/gofka/common/pkg/topic"
	"github.com/alexandrecolauto/gofka/common/proto/broker"
)

type ConsumerGroup struct {
	id       string
	leaderId string
	topics   map[string]*topic.Topic

	topicList  map[string]bool
	consumers  map[string]*ConsumerSession
	joining    bool
	inSync     bool
	consumerCh chan *ConsumerSession
	doneChList []chan any
	mu         sync.RWMutex
}

func NewConsumerGroup(id string) *ConsumerGroup {
	co := make(map[string]*ConsumerSession)
	ts := make(map[string]*topic.Topic)
	tl := make(map[string]bool)
	c_ch := make(chan *ConsumerSession)
	d_ch := make([]chan any, 0)
	return &ConsumerGroup{id: id, topics: ts, consumers: co, consumerCh: c_ch, doneChList: d_ch, topicList: tl}
}

func (cg *ConsumerGroup) ResetConsumerGroup(doneCh chan any) {
	cg.doneChList = append(cg.doneChList, doneCh)
	if cg.joining {
		return
	}
	co := make(map[string]*ConsumerSession)
	tl := make(map[string]bool)
	cg.consumers = co
	cg.joining = true
	cg.inSync = false
	cg.topicList = tl
	timeout := time.NewTicker(550 * time.Millisecond)
	for {
		select {
		case con := <-cg.consumerCh:
			if cg.leaderId == "" {
				cg.leaderId = con.id
			}
			con.last_heartbeat = time.Now()
			cg.AddTopics(con.topics)
			cg.consumers[con.id] = con

		case <-timeout.C:
			for _, d_ch := range cg.doneChList {
				close(d_ch)
			}
			cg.joining = false
			cg.doneChList = make([]chan any, 0)
			return
		}
	}
}

func (cg *ConsumerGroup) AddConsumer(id string, topics []string) {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	c, ok := cg.consumers[id]
	if !ok {
		c = NewConsumerSession(id, topics)
	}
	c.topics = topics
	c.last_heartbeat = time.Now()
	cg.consumerCh <- c
}

func (cg *ConsumerGroup) GetRegisterResponse(id string) *broker.RegisterConsumerResponse {
	cg.mu.RLock()
	defer cg.mu.RUnlock()
	topicList := make([]string, 0)
	for topic := range cg.topicList {
		// for internalT := range cg.topics {
		// 	if topic == internalT {
		// 	}
		// }
		topicList = append(topicList, topic)
	}
	if len(topicList) == 0 {
		fmt.Println("cannot find topics: ")
		fmt.Println(cg.topics)
		res := &broker.RegisterConsumerResponse{
			Success:      false,
			ErrorMessage: "cannot find topics",
		}
		return res
	}
	conumers_list := []*broker.ConsumerSession{}
	for c_id := range cg.consumers {
		c := cg.consumers[c_id]
		newConsumer := &broker.ConsumerSession{
			Id:     c.id,
			Topics: c.topics,
		}
		conumers_list = append(conumers_list, newConsumer)
	}
	res := &broker.RegisterConsumerResponse{
		Success:   true,
		Leader:    cg.leaderId,
		AllTopics: topicList,
		Consumers: conumers_list,
	}
	return res
}

func (cg *ConsumerGroup) AddTopics(topics []string) {
	for _, tp := range topics {
		cg.topicList[tp] = true
	}
}

// func (cg *ConsumerGroup) Subscribe(topic *topic.Topic) {
// 	_, ok := cg.topics[topic.Name]
// 	if ok {
// 		return
// 	}
// 	cg.mu.Lock()
// 	defer cg.mu.Unlock()
// 	cg.topics[topic.Name] = topic
// }

func (cg *ConsumerGroup) SyncGroup(consumers []*broker.ConsumerSession) {
	for _, consumer := range consumers {
		cg.consumers[consumer.Id].partitions = consumer.Assignments
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

func (cg *ConsumerGroup) ConsumerHeartbeat(id string) {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	c, ok := cg.consumers[id]
	if ok {
		c.last_heartbeat = time.Now()
	}
}
func (cg *ConsumerGroup) ClearDeadSession() {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	deadSessionTimeout := 15 * time.Second
	for _, consumer := range cg.consumers {
		if time.Since(consumer.last_heartbeat) > deadSessionTimeout {
			cg.UnregisterConsumer(consumer.id)
		}
	}
}
func (cg *ConsumerGroup) UnregisterConsumer(id string) {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	delete(cg.consumers, id)
}
