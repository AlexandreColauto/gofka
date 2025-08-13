package main

import (
	"fmt"
	"log"
	"time"

	"github.com/alexandrecolauto/gofka/pkg/consumer"
	"github.com/alexandrecolauto/gofka/pkg/gofka"
	"github.com/alexandrecolauto/gofka/pkg/producer"
)

func consume(n int, cons *consumer.Consumer) {
	for i := range n {
		log.Println("consumer - pooling", i)
		msgs := cons.Poll(time.Second * 1)

		log.Println("consumer - Received Item: ", cons.Offset())
		for _, msg := range msgs {
			log.Println("consumer - Msg: ", msg.Value)
		}
	}
}

func send(n int, prod *producer.Producer) {
	for i := range n {
		log.Println("producer - Sent message: ", i)
		prod.SendMessage("key", fmt.Sprintf("message_%d", i))
		time.Sleep(time.Millisecond * 1500)
	}
}

func main() {
	gfk := gofka.NewGofka()
	prod := producer.NewProducer("foo_topic", gfk)
	cons := consumer.NewConsumer("foo_topic", "consumer_id", gfk)
	go consume(2, cons)
	go send(3, prod)
	time.Sleep(time.Second * 2)
	log.Println("Creating new consumer")
	cons = consumer.NewConsumer("foo_topic", "consumer_id", gfk)
	go consume(4, cons)
	time.Sleep(time.Second * 10)

}
