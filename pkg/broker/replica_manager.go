package broker

import (
	"context"
	"fmt"
	"sync"

	"github.com/alexandrecolauto/gofka/model"
	"github.com/alexandrecolauto/gofka/pkg/topic"
	pb "github.com/alexandrecolauto/gofka/proto/broker"
)

type ReplicaManager struct {
	brokerID    string
	partitions  map[string]*topic.Partition
	fetcherPool map[string]*ReplicaFetcher

	client BrokerClient

	mutex  sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
}

type BrokerClient interface {
	FetchRecordsRequest(req *pb.FetchRecordsRequest) (*pb.FetchRecordsResponse, error)
	UpdateBroker(brokers model.BrokerInfo)
	UpdateFollowerStateRequest(req *pb.UpdateFollowerStateRequest) (*pb.UpdateFollowerStateResponse, error)
}

type FetchResponse struct {
	Message       []*topic.Message
	HighWaterMark int64
	LongEndOffset int64
	Error         error
}

func NewReplicaManager(brokerID string, client BrokerClient) *ReplicaManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &ReplicaManager{
		brokerID:    brokerID,
		partitions:  make(map[string]*topic.Partition),
		fetcherPool: make(map[string]*ReplicaFetcher),
		ctx:         ctx,
		client:      client,
		cancel:      cancel,
	}
}

func (r *ReplicaManager) AddPartition(topic string, partition *topic.Partition) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	key := fmt.Sprintf("%s-%d", topic, partition.ID())
	r.partitions[key] = partition
}

func (r *ReplicaManager) HandleLeaderChange(topic string, partitionID int, newLeaderBrokerID string, epoch int64, replicas []string) error {
	key := fmt.Sprintf("%s-%d", topic, partitionID)
	r.mutex.Lock()
	partition, exist := r.partitions[key]
	r.mutex.Unlock()
	if !exist {
		return fmt.Errorf("Cannot find partition")
	}
	if newLeaderBrokerID == r.brokerID {
		partition.BecomeLeader(r.brokerID, epoch, replicas)
		r.StopReplication(topic, partitionID)
	} else {
		partition.BecomeFollower(newLeaderBrokerID, epoch, replicas)
		r.StartReplication(topic, partitionID, newLeaderBrokerID)
	}
	return nil
}

func (r *ReplicaManager) StartReplication(topic string, partitionID int, leaderBrokerID string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	key := fmt.Sprintf("%s-%d", topic, partitionID)
	p, exists := r.partitions[key]
	if !exists {
		return fmt.Errorf("Cannot find partition")
	}
	if p.Leader() {
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
