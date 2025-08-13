package gofka

import (
	"sync"
)

type Gofka struct {
	topics    map[string]*Topic
	consumers map[string]*consumer

	mu sync.RWMutex
}

func NewGofka() *Gofka {
	topics := make(map[string]*Topic)
	consumers := make(map[string]*consumer)
	return &Gofka{topics: topics, consumers: consumers}
}

func (g *Gofka) RegisterConsumer(id, topic string, messageCh chan []*Message) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	cs, ok := g.consumers[id]
	if !ok {
		cs = NewConsumer(id, topic)
	}
	cs.msg_ch = messageCh
	g.consumers[id] = cs
	return cs.offset
}

func (g *Gofka) SyncConsumer(id string, offset int) {
	cs := g.getConsumer(id)
	t := g.GetOrCreateTopic(cs.topic)
	t.ReadFrom(offset, cs.msg_ch)
}

func (g *Gofka) UpdateOffset(id string, offset int) {
	cs := g.getConsumer(id)
	cs.offset = offset
}

func (g *Gofka) getConsumer(id string) *consumer {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.consumers[id]
}

func (g *Gofka) SendMessage(topic, key, value string) {
	t := g.GetOrCreateTopic(topic)
	message := NewMessage(key, value)
	t.Append(message)
}

func (g *Gofka) GetOrCreateTopic(topic string) *Topic {
	t, ok := g.topics[topic]
	if ok {
		return t
	}
	t = NewTopic(topic)
	g.topics[topic] = t
	return t
}
