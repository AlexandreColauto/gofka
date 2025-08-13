package gofka

type consumer struct {
	offset int
	msg_ch chan []*Message
	id     string
	topic  string
}

func NewConsumer(id, topic string) *consumer {
	return &consumer{id: id, topic: topic}
}
