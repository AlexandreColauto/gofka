package example

import (
	"fmt"
	"log"
	"time"

	"github.com/alexandrecolauto/gofka/client/config"
	"github.com/alexandrecolauto/gofka/client/pkg/consumer"
	"github.com/alexandrecolauto/gofka/common/proto/broker"
)

func Consume() {
	cfg := config.NewConsumerConfig()
	cfg.BootstrapAddress = "localhost:42069"
	cfg.GroupID = "foo-group"
	cfg.Topics = []string{"foo-topic", "bar-topic"}

	c := consumer.NewConsumer(cfg)

	opt := &broker.ReadOptions{
		MaxMessages: 100,
		MaxBytes:    10 * 1024 * 1024, //10MB max
		MinBytes:    256 * 1024,       // 256kB min
	}
	msgs, err := c.Poll(5*time.Second, opt)
	if err != nil {
		log.Println("Error pooling message: %w", err)
		return
	}

	fmt.Printf("Yehaaa got %d messages ", len(msgs))

	//commit offset back to the broker
	c.CommitOffsets()

	//add topics
	c.AddTopic("baz-topic")
	//remove topic
	c.RemoveTopic("foo-topic")
}
