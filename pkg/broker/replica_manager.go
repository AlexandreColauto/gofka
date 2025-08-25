package broker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/alexandrecolauto/gofka/model"
	"github.com/alexandrecolauto/gofka/pkg/log"
)

type ReplicaManager struct {
	brokerID    string
	partitions  map[string]*Partition
	fetcherPool map[string]*ReplicaFetcher

	client BrokerClient

	mutex  sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
}

type ReplicaFetcher struct {
	brokerID       string
	leaderBrokerID string
	topic          string
	partition      *Partition
	fetchInterval  time.Duration
	client         BrokerClient

	ctx    context.Context
	cancel context.CancelFunc
}

type BrokerClient interface {
	FetchRecords(brokerID, topic string, partition int, offset int64, maxBytes int) (*FetchResponse, error)
	UpdateBroker(brokers model.BrokerInfo)
	UpdateFollowerState(brokerID, topic string, partition int, followerID string, fetchOffset, longEndOffset int64) error
}

type FetchResponse struct {
	Message       []*log.Message
	HighWaterMark int64
	LongEndOffset int64
	Error         error
}

func NewReplicaManager(brokerID string, client BrokerClient) *ReplicaManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &ReplicaManager{
		brokerID:    brokerID,
		partitions:  make(map[string]*Partition),
		fetcherPool: make(map[string]*ReplicaFetcher),
		ctx:         ctx,
		client:      client,
		cancel:      cancel,
	}
}
func (r *ReplicaManager) AddPartition(topic string, partition *Partition) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	key := fmt.Sprintf("%s-%d", topic, partition.id)
	r.partitions[key] = partition
}

func (r *ReplicaManager) StartReplication(topic string, partitionID int, leaderBrokerID string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	key := fmt.Sprintf("%s-%d", topic, partitionID)
	p, exists := r.partitions[key]
	if !exists {
		return fmt.Errorf("Cannot find partition")
	}
	if p.leader {
		return fmt.Errorf("Cannot replicate if you are the leader")
	}

	if f, exists := r.fetcherPool[key]; exists {
		f.Stop()
	}

	f := NewReplicaFetcher(r.brokerID, leaderBrokerID, topic, p, r.client)
	r.fetcherPool[key] = f

	go f.Start(r.ctx)
	return nil
}
func (r *ReplicaManager) StopReplication(topic string, partitionID int) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	key := fmt.Sprintf("%s-%d", topic, partitionID)
	if f, exists := r.fetcherPool[key]; exists {
		f.Stop()
		delete(r.fetcherPool, key)
	}
}

func (r *ReplicaManager) HandleLeaderChange(topic string, partitionID int, newLeaderBrokerID string, epoch int64) error {
	key := fmt.Sprintf("%s-%d", topic, partitionID)
	r.mutex.Lock()
	partition, exist := r.partitions[key]
	r.mutex.Unlock()
	if !exist {
		return fmt.Errorf("Cannot find partition")
	}
	if newLeaderBrokerID == r.brokerID {
		fmt.Printf("%s Becoming leader of %d \n", r.brokerID, partitionID)
		partition.BecomeLeader(r.brokerID, epoch)
		r.StopReplication(topic, partitionID)
	} else {
		if partition.leader {
			fmt.Println("Losing leadership of", partitionID)
		}
		partition.BecomeFollower(r.brokerID, epoch)
		r.StartReplication(topic, partitionID, newLeaderBrokerID)
	}
	return nil
}

func (r *ReplicaManager) Shutdown() {
	r.cancel()

	r.mutex.Lock()
	defer r.mutex.Unlock()

	for _, f := range r.fetcherPool {
		f.Stop()
	}
}

func NewReplicaFetcher(brokerID, leaderBrokerID, topic string, partition *Partition, client BrokerClient) *ReplicaFetcher {
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
	currentLEO := rf.partition.leo

	response, err := rf.client.FetchRecords(
		rf.leaderBrokerID,
		rf.topic,
		rf.partition.id,
		currentLEO,
		1024*1024,
	)
	if err != nil {
		return
	}
	for _, message := range response.Message {
		_, err := rf.partition.Append(message)
		if err != nil {
			panic(err)
		}
		rf.partition.leo++
	}
	rf.sendFetchResponse(currentLEO, response.LongEndOffset)
}

func (rf *ReplicaFetcher) sendFetchResponse(fetchOffest, longEndOffset int64) {
	//to-do: improve this
	rf.client.UpdateFollowerState(rf.leaderBrokerID, rf.topic, rf.partition.id, rf.brokerID, fetchOffest, longEndOffset)
}
