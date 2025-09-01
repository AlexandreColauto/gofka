package broker

import (
	"context"
	"fmt"
	"time"

	"github.com/alexandrecolauto/gofka/pkg/topic"
	pb "github.com/alexandrecolauto/gofka/proto/broker"
)

type ReplicaFetcher struct {
	brokerID       string
	leaderBrokerID string
	topic          string
	partition      *topic.Partition
	fetchInterval  time.Duration
	client         BrokerClient

	ctx    context.Context
	cancel context.CancelFunc
}

func NewReplicaFetcher(brokerID, leaderBrokerID, topic string, partition *topic.Partition, client BrokerClient) *ReplicaFetcher {
	ctx, cancel := context.WithCancel(context.Background())
	return &ReplicaFetcher{
		brokerID:       brokerID,
		leaderBrokerID: leaderBrokerID,
		partition:      partition,
		topic:          topic,
		fetchInterval:  1000 * time.Millisecond,
		client:         client,
		ctx:            ctx,
		cancel:         cancel,
	}
}

func (rf *ReplicaFetcher) Start(parentCtx context.Context) {
	ticker := time.NewTicker(rf.fetchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-parentCtx.Done():
			fmt.Println("parent ctx done")
			return
		case <-rf.ctx.Done():
			fmt.Println("ctx done")
			return
		case <-ticker.C:
			rf.fetchFromLeader()
		}
	}
}

func (rf *ReplicaFetcher) Stop() {
	rf.cancel()
}

func (rf *ReplicaFetcher) fetchFromLeader() {
	currentLEO := rf.partition.Leo()

	req := &pb.FetchRecordsRequest{
		BrokerId:  rf.leaderBrokerID,
		Topic:     rf.topic,
		Partition: int32(rf.partition.ID()),
		Offset:    currentLEO,
		MaxBytes:  1024 * 1024,
	}

	response, err := rf.client.FetchRecordsRequest(req)
	if err != nil {
		// fmt.Println("err fetching record", err)
		rf.sendFetchResponse(currentLEO, currentLEO)
		return
	}
	if len(response.Message) > 0 {
		// fmt.Println("Fetched from leader", response)
		newLEO, err := rf.partition.AppendBatch(response.Message)
		if err != nil {
			return
		}
		currentLEO = newLEO
	}
	rf.sendFetchResponse(currentLEO, response.Longendoffset)
}

func (rf *ReplicaFetcher) sendFetchResponse(fetchOffest, longEndOffset int64) {
	req := &pb.UpdateFollowerStateRequest{
		BrokerId:      rf.leaderBrokerID,
		Topic:         rf.topic,
		Partition:     int32(rf.partition.ID()),
		FollowerId:    rf.brokerID,
		FetchOffset:   fetchOffest,
		LongEndOffset: longEndOffset,
	}
	_, err := rf.client.UpdateFollowerStateRequest(req)
	if err != nil {
		panic(err)
	}
}
