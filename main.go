package main

import (
	"fmt"
	"log"
	"time"

	broker "github.com/alexandrecolauto/gofka/pkg/broker"
	"github.com/alexandrecolauto/gofka/pkg/consumer"
	"github.com/alexandrecolauto/gofka/pkg/producer"
)

func consume(n int, cons *consumer.Consumer) {
	for i := range n {
		log.Println("consumer - pooling", i)
		readOpt := broker.NewReadOpts(15, 1024, 1024)
		msgs := cons.Poll(time.Second*1, readOpt)

		for _, msg := range msgs {
			log.Println("consumer - Msg: ", msg.Value)
		}
	}
}

func send(n int, prod *producer.Producer) {
	for i := range n {
		prod.SendMessage("key", fmt.Sprintf("message_%d", i))
		time.Sleep(time.Millisecond * 150)
	}
}

func main() {
	topic := "foo_topic"
	gfk := broker.NewGofka()
	prod := producer.NewProducer(topic, gfk)
	go send(4, prod)
	cons := consumer.NewConsumer("bar_group", gfk)
	cons.Subscribe(topic)
	go consume(4, cons)
	time.Sleep(time.Second * 2)
	// log.Println("Creating new consumer")
	// cons = consumer.NewConsumer("bar_group", gfk)
	// cons.Subscribe(topic)
	// go consume(4, cons)
	time.Sleep(time.Second * 10)

}
