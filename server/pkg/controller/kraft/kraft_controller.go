package kraft

import (
	"fmt"
	"log"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/alexandrecolauto/gofka/common/model"
	"github.com/alexandrecolauto/gofka/common/pkg/topic"
	vC "github.com/alexandrecolauto/gofka/common/pkg/visualizer_client"
	"github.com/alexandrecolauto/gofka/common/proto/broker"
	pb "github.com/alexandrecolauto/gofka/common/proto/broker"
	pc "github.com/alexandrecolauto/gofka/common/proto/controller"
	pr "github.com/alexandrecolauto/gofka/common/proto/raft"

	"github.com/alexandrecolauto/gofka/server/pkg/config"
	"github.com/alexandrecolauto/gofka/server/pkg/controller/raft"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type KraftController struct {
	raftModule      *raft.RaftModule
	clusterMetadata *model.ClusterMetadata
	metadataLog     *topic.Topic

	timeout         time.Duration
	gracePeriod     time.Duration
	startupTime     time.Time
	visualizeClient *vC.VisualizerClient

	shutdownOnce sync.Once
	shutdownCh   chan any
	wg           sync.WaitGroup
	isShutdown   bool

	mu sync.RWMutex
}

func NewManager(
	config *config.Config,
	shutdownCh chan any,
	sendAppendEntriesRequest func(address string, request *pr.AppendEntriesRequest) (*pr.AppendEntriesResponse, error),
	sendVoteRequest func(address string, request *pr.VoteRequest) (*pr.VoteResponse, error),
	vsualizerClient *vC.VisualizerClient,
) (*KraftController, error) {
	if config.Server.NodeID == "" || config.Server.Address == "" {
		return nil, fmt.Errorf("nodeID and address cannot be empty")
	}
	applyCh := make(chan *pc.LogEntry)
	metadata := model.NewClusterMetadata(shutdownCh)

	log, err := createMetadataTopic(config.Server.NodeID, shutdownCh)
	if err != nil {
		return nil, err
	}
	k := &KraftController{
		clusterMetadata: metadata,
		metadataLog:     log,
		timeout:         config.Kraft.Timeout,
		gracePeriod:     config.Kraft.GracePeriod,
		startupTime:     time.Now(),
		visualizeClient: vsualizerClient,
		shutdownCh:      shutdownCh,
	}
	r := raft.NewRaftModule(config, applyCh, shutdownCh, sendAppendEntriesRequest, sendVoteRequest, k.resetStartupTime, vsualizerClient)
	k.raftModule = r
	err = k.readFromDisk()
	if err != nil {
		fmt.Println("Err initializing", err)
	}

	//track go routines for graceful shutdown
	k.wg.Add(2)
	go k.applyCommands(applyCh)
	go k.monitorDeadSessions()

	metadata.SetStableFunc(k.OnClusterStable)
	return k, nil
}

func (c *KraftController) applyCommands(ch chan *pc.LogEntry) {
	defer c.wg.Done()
	for {
		select {
		case <-c.shutdownCh:
			return
		case entry := <-ch:
			if c.isShutdown {
				return
			}
			if entry.Command == nil {
				continue
			}
			c.clusterMetadata.DecodeLog(entry, c)
			c.saveLog(entry)
		}
	}
}

func (c *KraftController) saveLog(log *pc.LogEntry) {
	val, err := proto.Marshal(log)
	if err != nil {
		panic(err)
	}
	msg := &pb.Message{
		Key:   fmt.Sprintf("%d", log.Index),
		Value: string(val),
	}
	partition := 0

	c.metadataLog.AppendBatch(partition, []*broker.Message{msg})
}

func (c *KraftController) ApplyCreateTopic(ctc *pc.Command_CreateTopic) {
	if c.raftModule.IsLeader() {
		c.electPartitionsLeaders()
		c.updateFavoriteLeaders()
	}
}

func (c *KraftController) ApplyRegisterBroker(ctc *pc.Command_RegisterBroker) {
	if c.raftModule.IsLeader() {
		c.electPartitionsLeaders()
		c.updateFavoriteLeaders()
	}
}

func (c *KraftController) ApplyUpdateBroker(ctc *pc.Command_UpdateBroker) {
	brk := &model.BrokerInfo{
		ID:       ctc.UpdateBroker.Id,
		Address:  ctc.UpdateBroker.Address,
		Alive:    ctc.UpdateBroker.Alive,
		LastSeen: ctc.UpdateBroker.LastSeen.AsTime(),
	}
	if !brk.Alive {
		c.removeFromISR(brk)
		if c.raftModule.IsLeader() {
			c.brokerFailOver(ctc.UpdateBroker.Id)
		}
	}
}

func (c *KraftController) ApplyUpdatePartitionLeader(ctc *pc.Command_ChangePartitionLeader) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, assignment := range ctc.ChangePartitionLeader.Assignments {
		t, ok := c.clusterMetadata.Topic(assignment.TopicId)
		if !ok {
			continue
		}
		p, ok := t.Partitions[assignment.PartitionId]
		if !ok {
			continue
		}
		p.Epoch = int64(assignment.NewEpoch)
		p.Leader = assignment.NewLeader
		p.Replicas = assignment.NewReplicas
		p.Isr = assignment.NewIsr
	}
}

func (c *KraftController) OnClusterStable() {
	if c.raftModule.IsLeader() {
		fmt.Println("Cluster Stable")
		c.updateFavoriteLeaders()
	}
}

func (c *KraftController) electPartitionsLeaders() {
	brokers := c.getAliveBrokers()
	if len(brokers) == 0 {
		return // No alive brokers
	}

	sort.Slice(brokers, func(i, j int) bool {
		return brokers[i].Id < brokers[j].Id
	})

	assigments := c.assignPartitions(brokers)

	c.submitAssignments(assigments)
}

func (c *KraftController) updateFavoriteLeaders() {
	brokers := c.getAliveBrokers()
	if len(brokers) == 0 {
		return // No alive brokers
	}

	sort.Slice(brokers, func(i, j int) bool {
		return brokers[i].Id < brokers[j].Id
	})

	assigments := c.updateFavorite(brokers)

	c.submitAssignments(assigments)
}

func (c *KraftController) getAliveBrokers() []*pb.BrokerInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	brokers := make([]*pb.BrokerInfo, 0)
	for _, brkr := range c.clusterMetadata.Brokers() {
		if brkr.Alive {
			brokers = append(brokers, brkr)
		}
	}
	return brokers
}
func (c *KraftController) assignPartitions(brokers []*pb.BrokerInfo) []*pc.PartitionAssignment {
	c.mu.Lock()
	defer c.mu.Unlock()
	brokerIndex := 0

	assigments := make([]*pc.PartitionAssignment, 0)

	t := c.clusterMetadata.Topics()
	for _, topic := range t {
		for _, partition := range topic.Partitions {
			if partition.Leader == "" {
				replicationFactor := topic.ReplicationFactor // Assuming this field exists
				if replicationFactor > int32(len(brokers)) {
					fmt.Printf("Warning: Not enough brokers (%d) to satisfy replication factor (%d) for topic %s. Using all available brokers.\n", len(brokers), replicationFactor, topic.Name)
					replicationFactor = int32(len(brokers))
				}

				replicas := make([]string, 0, replicationFactor)

				// Assign unique brokers
				for i := 0; i < int(replicationFactor); i++ {
					candidateBroker := brokers[(brokerIndex+i)%len(brokers)]
					if !slices.Contains(replicas, candidateBroker.Id) {
						replicas = append(replicas, candidateBroker.Id)
					}
				}

				leaderID := replicas[0]

				ass := &pc.PartitionAssignment{
					TopicId:     topic.Name,
					PartitionId: int32(partition.Id),
					NewLeader:   leaderID,
					NewReplicas: replicas,
					NewIsr:      replicas,
					NewEpoch:    int32(partition.Epoch + 1),
				}
				assigments = append(assigments, ass)
				brokerIndex = (brokerIndex + 1) % len(brokers)
			}
		}
	}
	return assigments
}

func (c *KraftController) updateFavorite(aliveBrokers []*pb.BrokerInfo) []*pc.PartitionAssignment {
	c.mu.Lock()
	defer c.mu.Unlock()

	assigments := make([]*pc.PartitionAssignment, 0)

	t := c.clusterMetadata.Topics()
	for _, topic := range t {
		for _, partition := range topic.Partitions {
			if partition.Leader != "" {
				favLeader := partition.Replicas[0]
				isAlive := false
				for _, b := range aliveBrokers {
					if b.Id == favLeader {
						isAlive = true
						break
					}
				}
				if partition.Leader != favLeader {
					if slices.Contains(partition.Isr, favLeader) && isAlive {
						ass := &pc.PartitionAssignment{
							TopicId:     topic.Name,
							PartitionId: int32(partition.Id),
							NewLeader:   favLeader,
							NewReplicas: partition.Replicas,
							NewIsr:      partition.Isr,
							NewEpoch:    int32(partition.Epoch + 1),
						}
						assigments = append(assigments, ass)
					}
				}
			}
		}
	}
	return assigments
}

func (c *KraftController) submitAssignments(assigments []*pc.PartitionAssignment) {
	if len(assigments) > 0 {
		payload := &pc.ChangePartitionLeaderCommand{
			Assignments: assigments,
		}
		cmd := &pc.Command{
			Type: pc.Command_CHANGE_PARTITION_LEADER,
			Payload: &pc.Command_ChangePartitionLeader{
				ChangePartitionLeader: payload,
			},
		}
		c.SubmitCommandGRPC(cmd)
	}
}

func (c *KraftController) removeFromISR(brokerInfo *model.BrokerInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, t := range c.clusterMetadata.Topics() {
		for _, p := range t.Partitions {
			newISR := make([]string, 0)
			for _, isr := range p.Isr {
				if isr != brokerInfo.ID {
					newISR = append(newISR, isr)
				}
			}
			p.Isr = newISR
		}
	}
}
func (c *KraftController) brokerFailOver(leaderID string) {
	c.mu.Lock()
	if len(c.clusterMetadata.Brokers()) == 0 {
		return // No alive brokers
	}
	t := c.clusterMetadata.Topics()
	brks := c.clusterMetadata.Brokers()
	assigments := make([]*pc.PartitionAssignment, 0)
	for _, topic := range t {
		for _, partition := range topic.Partitions {
			if partition.Leader == leaderID {
				b, ok := brks[partition.Leader]
				if !ok || !b.Alive {
					newLeader := ""
					// Elect a new leader from the In-Sync Replicas
					for _, replicaID := range partition.Isr {
						r, ok := brks[replicaID]
						if ok && r.Alive && replicaID != partition.Leader {
							newLeader = replicaID
							break
						}
					}

					if newLeader != "" {
						// The new ISR is the old ISR minus the failed leader.
						newISR := make([]string, 0, len(partition.Isr)-1)
						for _, isrBrokerID := range partition.Isr {
							if isrBrokerID != leaderID {
								newISR = append(newISR, isrBrokerID)
							}
						}

						ass := &pc.PartitionAssignment{
							TopicId:     topic.Name,
							PartitionId: int32(partition.Id),
							NewLeader:   newLeader,
							NewReplicas: partition.Replicas, // Use the new, re-ordered replica list
							NewIsr:      newISR,
							NewEpoch:    int32(partition.Epoch) + 1,
						}

						assigments = append(assigments, ass)

					} else {
						// CRITICAL: No available leader in ISR. This partition is now offline
						// until a broker in the ISR comes back online.
						fmt.Printf("CRITICAL: No live replica in ISR for topic %s, partition %d. %+v Partition is offline.\n", topic.Name, partition.Id, partition.Isr)
					}
				}
			}
		}
	}

	c.mu.Unlock()
	c.submitAssignments(assigments)
}

func (c *KraftController) monitorDeadSessions() {
	defer c.wg.Done()
	ticker := time.NewTicker(500 * time.Millisecond)
	for {
		select {
		case <-c.shutdownCh:
			ticker.Stop()
			return
		case <-ticker.C:
			c.cleanDeadSessions()
		}
	}
}

func (c *KraftController) cleanDeadSessions() {
	if !c.raftModule.IsLeader() {
		return
	}
	if time.Since(c.startupTime) < c.gracePeriod {
		return
	}
	c.mu.RLock()
	brokers := c.clusterMetadata.Brokers()
	c.mu.RUnlock()
	for _, broker := range brokers {
		if time.Since(broker.LastSeen.AsTime()) > c.timeout && broker.Alive {
			fmt.Println("FOUND DEAD BROKER REMOVING", c.ID(), broker.Id)
			brokerChange := &pb.BrokerInfo{
				Id:       broker.Id,
				Address:  broker.Address,
				Alive:    false,
				LastSeen: broker.LastSeen,
			}
			c.commandChangeBroker(brokerChange)
		}
	}
}

func (c *KraftController) commandChangeBroker(brokerInfo *pb.BrokerInfo) {
	pyld := &pc.UpdateBrokerCommand{
		Id:       brokerInfo.Id,
		Address:  brokerInfo.Address,
		LastSeen: brokerInfo.LastSeen,
		Alive:    brokerInfo.Alive,
	}
	cmd := &pc.Command{
		Type: pc.Command_UPDATE_BROKER,
		Payload: &pc.Command_UpdateBroker{
			UpdateBroker: pyld,
		},
	}
	c.SubmitCommandGRPC(cmd)
}

func (c *KraftController) brokerHeartbeat(brokerID string) error {
	if err := c.isLeader(); err != nil {
		return err
	}

	c.mu.RLock()
	brsk := c.clusterMetadata.Brokers()
	b, ok := brsk[brokerID]
	if !ok {
		c.mu.RUnlock()
		return fmt.Errorf("cannot find broker with id: %s", brokerID)
	}
	oldAlive := b.Alive
	b.Alive = true
	b.LastSeen = timestamppb.New(time.Now())
	c.mu.RUnlock()
	if !oldAlive {
		log.Println("reviving broker:", b)
		c.commandChangeBroker(b)
	}
	return nil
}

func (c *KraftController) brokerMetadata(index int64) ([]*pc.LogEntry, error) {
	return c.raftModule.LogFromIndex(index)
}

func (c *KraftController) registerBroker(r *pc.BrokerRegisterRequest) error {
	pyld := &pc.RegisterBrokerCommand{
		Id:       r.Id,
		Address:  r.Address,
		LastSeen: timestamppb.New(time.Now()),
		Alive:    true,
	}
	cmd := &pc.Command{
		Type: pc.Command_REGISTER_BROKER,
		Payload: &pc.Command_RegisterBroker{
			RegisterBroker: pyld,
		},
	}
	return c.SubmitCommandGRPC(cmd)
}
func (s *KraftController) SubmitCommandGRPC(cmd *pc.Command) error {
	err := s.raftModule.SubmitCommand(cmd)
	if err != nil {
		return s.isLeader()
	}
	return nil
}

func (s *KraftController) ID() string {
	return s.raftModule.ID()
}
func (s *KraftController) isLeader() error {
	if s.raftModule.IsLeader() {
		return nil
	}
	leader := s.raftModule.Leader()
	if leader == "" {
		return status.Error(codes.Unavailable, "no leader available")
	}

	addr, ok := s.raftModule.GetAddress(leader)
	if !ok {
		return status.Error(codes.Unavailable, "leader address not found")
	}

	errorMsg := fmt.Sprintf("not leader|%s|%s", leader, addr)
	return status.Error(codes.FailedPrecondition, errorMsg)
}

func (s *KraftController) resetStartupTime() {
	s.startupTime = time.Now()
}

func (s *KraftController) Shutdown() error {
	var shutDownErr error
	s.shutdownOnce.Do(func() {
		s.isShutdown = true

		done := make(chan any)

		go func() {
			s.wg.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			shutDownErr = fmt.Errorf("timeout: some goroutines didin't finish within 5 seconds")
		}

		s.raftModule.Shutdown()

	})
	return shutDownErr

}
