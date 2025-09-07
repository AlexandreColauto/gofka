package broker

import (
	"context"
	"fmt"
	"time"

	"github.com/alexandrecolauto/gofka/common/pkg/topic"
	pb "github.com/alexandrecolauto/gofka/common/proto/broker"
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

func NewReplicaFetcher(brokerID, leaderBrokerID, topic string, partition *topic.Partition, client BrokerClient, fetchInterval time.Duration) *ReplicaFetcher {
	ctx, cancel := context.WithCancel(context.Background())
	return &ReplicaFetcher{
		brokerID:       brokerID,
		leaderBrokerID: leaderBrokerID,
		partition:      partition,
		topic:          topic,
		fetchInterval:  fetchInterval,
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
			return
		case <-rf.ctx.Done():
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
	offset := rf.partition.Leo() - 1

	req := &pb.FetchRecordsRequest{
		BrokerId:  rf.leaderBrokerID,
		Topic:     rf.topic,
		Partition: int32(rf.partition.ID()),
		Offset:    offset,
		MaxBytes:  1024 * 1024 * 102,
	}

	response, err := rf.client.FetchRecordsRequest(req)
	if err != nil {
		// fmt.Println("err fetching record", err)
		rf.sendFetchResponse(offset, offset)
		return
	}
	if len(response.Message) > 0 {
		_, err := rf.partition.AppendBatch(response.Message)
		if err != nil {
			return
		}
	}
	rf.sendFetchResponse(offset, response.Longendoffset)
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
		fmt.Println("error updating follower  request", err)
	}
}
