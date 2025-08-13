package consumer

import (
	"log"
	"time"

	"github.com/alexandrecolauto/gofka/pkg/gofka"
)

type Consumer struct {
	id         string
	topic      string
	gofka      *gofka.Gofka
	messagesch chan []*gofka.Message

	offset int
}

func NewConsumer(topic, consumerID string, gf *gofka.Gofka) *Consumer {
	m_ch := make(chan []*gofka.Message, 100)
	offset := gf.RegisterConsumer(consumerID, topic, m_ch)
	return &Consumer{id: consumerID, topic: topic, gofka: gf, messagesch: m_ch, offset: offset}
}

func (c *Consumer) Poll(timeout time.Duration) []*gofka.Message {
	c.gofka.SyncConsumer(c.id, c.offset)
	select {
	case messages := <-c.messagesch:
		log.Println("consumer - got messages: ", messages)
		c.offset += len(messages)
		c.gofka.UpdateOffset(c.id, c.offset)

		return messages
	case <-time.After(timeout):
		log.Println("consumer - timeout")
		return []*gofka.Message{}
	}
}
func (c *Consumer) Offset() int {
	return c.offset
}
