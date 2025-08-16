package broker

import (
	"fmt"
	"sync"
	"time"

	"github.com/alexandrecolauto/gofka/pkg/log"
)

type Gofka struct {
	topics         map[string]*Topic
	consumer_group map[string]*ConsumerGroup

	mu sync.RWMutex
}

type ReadOpts struct {
	log.ReadOpts
}

func NewReadOpts(maxMessages, maxBytes, minBytes int32) *ReadOpts {
	return &ReadOpts{
		log.ReadOpts{
			MaxMessages: maxMessages,
			MaxBytes:    maxBytes,
			MinBytes:    minBytes,
		},
	}
}

func (r *ReadOpts) ToOpt() *log.ReadOpts {
	return &log.ReadOpts{
		MaxBytes:    r.MaxBytes,
		MaxMessages: r.MaxMessages,
		MinBytes:    r.MinBytes,
	}
}

func NewGofka() *Gofka {
	topics := make(map[string]*Topic)
	consumers := make(map[string]*ConsumerGroup)
	g := Gofka{topics: topics, consumer_group: consumers}
	g.startSessionMonitor()
	return &g
}

func (g *Gofka) RegisterConsumer(id, group_id string, messageCh chan []*log.Message) {
	g.mu.Lock()
	defer g.mu.Unlock()
	cg := g.GetOrCreateConsumerGroup(group_id)
	cg.AddConsumer(id, messageCh)
}

func (g *Gofka) SendMessage(topic, key, value string) error {
	t, err := g.GetOrCreateTopic(topic)
	if err != nil {
		return err
	}
	message := log.NewMessage(key, value)
	t.Append(message)
	return nil
}

func (g *Gofka) Subscribe(topic, group_id string) error {
	t, err := g.GetOrCreateTopic(topic)
	if err != nil {
		return err
	}
	cg := g.GetOrCreateConsumerGroup(group_id)
	cg.Subscribe(t)
	return nil
}

func (g *Gofka) Unsubscribe(topic, group_id string) {
	cg := g.GetOrCreateConsumerGroup(group_id)
	cg.Unsubscribe(topic)
}

func (g *Gofka) GetOrCreateConsumerGroup(group_id string) *ConsumerGroup {
	cg, ok := g.consumer_group[group_id]
	if ok {
		return cg
	}
	cg = NewConsumerGroup(group_id)
	g.consumer_group[group_id] = cg
	return cg
}

func (g *Gofka) GetOrCreateTopic(topic string) (*Topic, error) {
	t, ok := g.topics[topic]
	if ok {
		return t, nil
	}
	t, err := NewTopic(topic, 1)
	if err != nil {
		return nil, err
	}
	g.topics[topic] = t
	return t, nil
}

func (g *Gofka) FetchMessages(id, group_id string, opt *log.ReadOpts) {
	cg := g.GetOrCreateConsumerGroup(group_id)
	cg.FetchMessages(id, opt)
}

func (g *Gofka) CommitOffset(group_id, topic string, partition, offset int) {
	cg := g.GetOrCreateConsumerGroup(group_id)
	cg.UpdateOffset(topic, partition, offset)
}

func (g *Gofka) SendHeartbeat(id, group_id string) {
	cg := g.GetOrCreateConsumerGroup(group_id)
	cg.SendHeartbeat(id)
}

func (g *Gofka) UnregisterConsumer(id, group_id string) {
	cg := g.GetOrCreateConsumerGroup(group_id)
	cg.UnregisterConsumer(id)
}

func (g *Gofka) startSessionMonitor() {
	ticker := time.NewTicker(10 * time.Second)
	go func() {
		for range ticker.C {
			g.cleanupDeadSession()
		}
	}()
}

func (g *Gofka) cleanupDeadSession() {
	for _, con_group := range g.consumer_group {
		con_group.ClearDeadSession()
	}
}

func (g *Gofka) DeleteTopic(topic_name string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.topics[topic_name]; !ok {
		return
	}
	for _, cg := range g.consumer_group {
		for name := range cg.topics {
			if name == topic_name {
				cg.DeleteTopic(topic_name)
			}
		}
	}
	delete(g.topics, topic_name)
}
func (g *Gofka) ChangeTopic(topic_name string, partitions int) {
	t, ok := g.topics[topic_name]
	if !ok {
		fmt.Println("cant find topic")
		return
	}
	t.AddPartitions(partitions)
}
