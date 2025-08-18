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

func testBroker() {
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

// func main() {
//
// 	peers := make(map[string]string)
// 	c := raft.NewController("node-id", "localhost:42069", peers)
// 	http.HandleFunc("/raft/vote", c.HandleVoteRequest)
// 	http.HandleFunc("/raft/append", c.HandleAppendEntries)
//
// 	//we have the controller and the raft module.
//
// 	bs := broker.NewBrokerServer()
//
// 	http.HandleFunc("/fetch-replica", bs.HandleFetchReplica)
// 	http.HandleFunc("/update-follower", bs.HandleUpdateFollower)
//
// 	// Core Producer/Consumer APIs
// 	http.HandleFunc("/produce", bs.HandleProduce)
//
// 	// Metadata & Offset APIs
// 	http.HandleFunc("/metadata", bs.HandleMetadata)
// 	http.HandleFunc("/offsets", bs.HandleListOffsets)
//
// 	// Consumer Group APIs
// 	http.HandleFunc("/group/join", bs.HandleJoinGroup)
// 	http.HandleFunc("/group/sync", bs.HandleSyncGroup)
// 	http.HandleFunc("/group/heartbeat", bs.HandleHeartbeat)
// 	http.HandleFunc("/group/leave", bs.HandleLeaveGroup)
// 	http.HandleFunc("/group/commit", bs.HandleOffsetCommit)
// 	http.HandleFunc("/group/fetch", bs.HandleOffsetFetch)
//
// 	// Admin APIs
// 	http.HandleFunc("/topics/create", bs.HandleCreateTopics)
// 	http.HandleFunc("/topics/delete", bs.HandleDeleteTopics)
//
// 	log.Println("🚀 gofka broker starting on :42069")
//
// 	log.Fatal(http.ListenAndServe(":42069", nil))
// }
