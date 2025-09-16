package producer

import (
	pb "github.com/alexandrecolauto/gofka/common/proto/broker"
	"time"
)

type MessageBatch struct {
	Topic     string
	Partition int32
	MaxMsg    int
	Lifetime  time.Time
	Duration  time.Duration
	Done      bool
	Messages  []*pb.Message
	flushTick *time.Ticker
	flush     func()
}

func NewMessageBatch(maxMsg int, duration time.Duration, flush func()) *MessageBatch {
	m := &MessageBatch{MaxMsg: maxMsg, Lifetime: time.Now().Add(duration), Duration: duration, flush: flush}
	go m.flushTimer()
	m.flushTick = time.NewTicker(m.Duration)
	return m
}

func (m *MessageBatch) Reset() {
	m.Done = false
	m.flushTick.Reset(m.Duration)
}

func (m *MessageBatch) flushTimer() {
	for range m.flushTick.C {
		if !m.Done {
			m.flush()
			m.flushTick.Stop()
		}
	}
}

func (p *Producer) getCurrentBatchFor(partition int) *MessageBatch {
	bat, ok := p.messages.batches[int32(partition)]
	if !ok {
		return p.newBatch(partition)
	}
	bat.Reset()
	return bat
}

func (p *Producer) newBatch(partition int) *MessageBatch {
	b := NewMessageBatch(p.maxMsg, p.batchTimeout, p.flush)
	b.Partition = int32(partition)
	b.Topic = p.messages.topic
	p.messages.batches[int32(partition)] = b
	return b
}
