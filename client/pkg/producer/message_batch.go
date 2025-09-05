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
	flush     func()
}

func NewMessageBatch(duration time.Duration, flush func()) *MessageBatch {
	m := &MessageBatch{MaxMsg: 10, Lifetime: time.Now().Add(duration), Duration: duration, flush: flush}
	go m.flushTimer()
	return m
}

func (m *MessageBatch) Reset() {
	m.Messages = make([]*pb.Message, 0)
	m.Done = false
	go m.flushTimer()
}

func (m *MessageBatch) flushTimer() {
	time.Sleep(m.Duration)
	if !m.Done {
		m.flush()
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
	b := NewMessageBatch(1*time.Second, p.flush)
	b.Partition = int32(partition)
	b.Topic = p.messages.topic
	p.messages.batches[int32(partition)] = b
	return b
}
