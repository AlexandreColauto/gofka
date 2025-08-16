package producer

import (
	"fmt"

	broker "github.com/alexandrecolauto/gofka/pkg/broker"
)

type Producer struct {
	topic  string
	broker *broker.Gofka
}

func NewProducer(topic string, gofka *broker.Gofka) *Producer {
	return &Producer{topic: topic, broker: gofka}
}

func (p *Producer) SendMessage(key, value string) {
	p.broker.SendMessage(p.topic, key, value)
	fmt.Println("Message sent")
}
