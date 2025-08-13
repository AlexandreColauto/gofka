package producer

import (
	"fmt"

	"github.com/alexandrecolauto/gofka/pkg/gofka"
)

type Producer struct {
	topic string
	gofka *gofka.Gofka
}

func NewProducer(topic string, gofka *gofka.Gofka) *Producer {
	return &Producer{topic: topic, gofka: gofka}
}

func (p *Producer) SendMessage(key, value string) {
	p.gofka.SendMessage(p.topic, key, value)
	fmt.Println("Message sent")
}
