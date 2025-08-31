package kraft

import (
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/alexandrecolauto/gofka/model"
	"github.com/alexandrecolauto/gofka/pkg/controller/raft"
	"github.com/alexandrecolauto/gofka/pkg/topic"
	"github.com/alexandrecolauto/gofka/proto/broker"
	pb "github.com/alexandrecolauto/gofka/proto/broker"
	pc "github.com/alexandrecolauto/gofka/proto/controller"
	pr "github.com/alexandrecolauto/gofka/proto/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type KraftController struct {
	raftModule      *raft.RaftModule
	clusterMetadata *model.ClusterMetadata
	metadataLog     *topic.Topic

	timeout time.Duration
}

func NewManager(
	nodeID, address string,
	peers map[string]string,
	sendAppendEntriesRequest func(address string, request *pr.AppendEntriesRequest) (*pr.AppendEntriesResponse, error),
	sendVoteRequest func(address string, request *pr.VoteRequest) (*pr.VoteResponse, error),
) (*KraftController, error) {
	if nodeID == "" || address == "" {
		return nil, fmt.Errorf("nodeID and address cannot be empty")
	}
	applyCh := make(chan *pc.LogEntry)
	metadata := model.NewClusterMetadata()

	log, err := createMetadataTopic(nodeID)
	if err != nil {
		return nil, err
	}
	r := raft.NewRaftModule(nodeID, peers, applyCh, sendAppendEntriesRequest, sendVoteRequest)
	k := &KraftController{
		raftModule:      r,
		clusterMetadata: metadata,
		metadataLog:     log,
		timeout:         2 * time.Second,
	}
	err = k.readFromDisk()
	if err != nil {
		fmt.Println("Err initializing", err)
	}
	go k.applyCommands(applyCh)
	go k.monitorDeadSessions()
	return k, nil
}

func (c *KraftController) applyCommands(ch chan *pc.LogEntry) {
	for entry := range ch {
		if entry.Command == nil {
			continue
		}
		c.clusterMetadata.DecodeLog(entry, c)
		c.saveLog(entry)
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

	c.metadataLog.AppendBatch(0, []*broker.Message{msg})
}

func (c *KraftController) ApplyCreateTopic(ctc *pc.Command_CreateTopic) {
	if c.raftModule.IsLeader() {
		c.electPartitionsLeaders()
	}
}

func (c *KraftController) ApplyRegisterBroker(ctc *pc.Command_RegisterBroker) {
	if c.raftModule.IsLeader() {
		c.electPartitionsLeaders()
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

func (c *KraftController) getAliveBrokers() []*pb.BrokerInfo {
	brokers := make([]*pb.BrokerInfo, 0)
	for _, brkr := range c.clusterMetadata.Brokers() {
		if brkr.Alive {
			brokers = append(brokers, brkr)
		}
	}
	return brokers
}

func (c *KraftController) assignPartitions(brokers []*pb.BrokerInfo) []*pc.PartitionAssignment {
	brokerIndex := 0

	assigments := make([]*pc.PartitionAssignment, 0)

	t := c.clusterMetadata.Topics()
	for _, topic := range t {
		for _, partition := range topic.Partitions {
			if partition.Leader == "" {
				leaderID := brokers[brokerIndex].Id
				replicas := make([]string, len(partition.Replicas))
				replicas[0] = leaderID
				for i := 1; i < len(replicas); i++ {
					index := (brokerIndex + i) % len(brokers)
					replicas[i] = brokers[index].Id
				}
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
	if len(c.clusterMetadata.Brokers()) == 0 {
		return // No alive brokers
	}
	assigments := make([]*pc.PartitionAssignment, 0)
	for _, topic := range c.clusterMetadata.Topics() {
		for _, partition := range topic.Partitions {
			if !c.isBrokerAlive(partition.Leader) && partition.Leader == leaderID {
				newLeader := ""
				for _, replicaID := range partition.Isr {
					if c.isBrokerAlive(replicaID) && replicaID != partition.Leader {
						newLeader = replicaID
						break
					}
				}

				if newLeader != "" {
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
						NewReplicas: partition.Replicas,
						NewIsr:      newISR,
						NewEpoch:    int32(partition.Epoch) + 1,
					}

					assigments = append(assigments, ass)

				} else {
					// CRITICAL: No available leader in ISR. Partition is offline.
					fmt.Println("NO ISR AVAILABLE FOR LEADERSHIP")
				}
			}
		}
	}

	c.submitAssignments(assigments)
}

func (c *KraftController) isBrokerAlive(brokerID string) bool {
	brks := c.clusterMetadata.Brokers()
	b, ok := brks[brokerID]
	if !ok {
		return false
	}
	return b.Alive
}

func (c *KraftController) monitorDeadSessions() {
	ticker := time.NewTicker(500 * time.Millisecond)
	for range ticker.C {
		c.cleanDeadSessions()
	}
}

func (c *KraftController) cleanDeadSessions() {
	if !c.raftModule.IsLeader() {
		return
	}
	for _, broker := range c.clusterMetadata.Brokers() {
		if time.Since(broker.LastSeen.AsTime()) > c.timeout && broker.Alive {
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
	brsk := c.clusterMetadata.Brokers()
	b, ok := brsk[brokerID]
	if !ok {
		return fmt.Errorf("cannot find broker with id: %s", brokerID)
	}
	oldAlive := b.Alive
	b.Alive = true
	b.LastSeen = timestamppb.New(time.Now())
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
