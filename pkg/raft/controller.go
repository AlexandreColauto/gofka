package raft

import (
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/alexandrecolauto/gofka/model"
	"github.com/alexandrecolauto/gofka/pkg/broker"
	pb "github.com/alexandrecolauto/gofka/proto/broker"
	pc "github.com/alexandrecolauto/gofka/proto/controller"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type RaftController struct {
	Raft *RaftModule

	Metadata    *model.ClusterMetadata
	logMetadata *broker.Topic

	timeout time.Duration

	NodeId   string
	Peers    map[string]string
	HttpAddr string
}

type TopicMetadata struct {
	Name       string
	Partitions map[int]*PartitionMetadata
}

type PartitionMetadata struct {
	ID       int
	Leader   string
	Replicas []string
	ISR      []string
	Epoch    int64
}

func NewController(nodeID, address string, peers map[string]string) *RaftController {
	applyCh := make(chan *pc.LogEntry)
	mt := model.NewClusterMetadata()
	logTopic, err := broker.NewTopic(fmt.Sprintf("__cluster_metadata/%s", nodeID), 1)

	if err != nil {
		panic(err)
	}

	p, err := logTopic.GetPartition(0)

	if err != nil {
		panic(err)
	}
	p.BecomeLeader(nodeID, 1)

	r := NewRaftModule(nodeID, peers, applyCh)
	c := &RaftController{
		Raft:     r,
		Metadata: mt,

		timeout:     2 * time.Second,
		logMetadata: logTopic,

		NodeId:   nodeID,
		Peers:    peers,
		HttpAddr: address,
	}
	err = c.readFromDisk()
	if err != nil {
		fmt.Println("Err initializing", err)
	}
	go c.applyCommands(applyCh)
	go c.monitorDeadSessions()
	return c
}

func (c *RaftController) applyCommands(ch chan *pc.LogEntry) {
	for entry := range ch {
		if entry.Command == nil {
			continue
		}
		c.Metadata.DecodeLog(entry, c)
		c.saveLog(entry)
	}
}

func (c *RaftController) saveLog(log *pc.LogEntry) {
	val, err := proto.Marshal(log)
	if err != nil {
		panic(err)
	}
	msg := &pb.Message{
		Key:   fmt.Sprintf("%d", log.Index),
		Value: string(val),
	}

	c.logMetadata.Append(msg)
}

func (c *RaftController) readFromDisk() error {
	p, err := c.logMetadata.GetPartition(0)
	if err != nil {
		return err
	}
	opt := &pb.ReadOptions{
		MaxMessages: 1000,
		MaxBytes:    10024 * 1024,
		MinBytes:    10024 * 1024,
	}
	msgs, err := p.ReadFromReplica(0, opt)
	if err != nil {
		fmt.Println("error reading", err)
		return err
	}
	if len(msgs) == 0 {
		fmt.Println("No logs found on disk, starting fresh")
		return nil
	}

	var recoveredLogs []*pc.LogEntry
	var maxTerm int64
	var maxIndex int64
	for _, msg := range msgs {
		val := msg.Value
		var log pc.LogEntry
		err := proto.Unmarshal([]byte(val), &log)
		if err != nil {
			panic(err)
		}
		// Track maximum term and index
		if log.Term > maxTerm {
			maxTerm = log.Term
		}
		if log.Index > maxIndex {
			maxIndex = log.Index
		}

		recoveredLogs = append(recoveredLogs, &log)

	}
	sort.Slice(recoveredLogs, func(i, j int) bool {
		return recoveredLogs[i].Index < recoveredLogs[j].Index
	})

	if err := c.validateLogContinuity(recoveredLogs); err != nil {
		return fmt.Errorf("log validation failed: %w", err)
	}
	c.Raft.currentTerm = maxTerm
	c.Raft.lastApplied = 0        // Will be updated as we apply logs
	c.Raft.commitIndex = maxIndex // Assume all recovered logs are committed
	c.Raft.votedFor = ""          // Reset vote on recovery

	for _, log := range recoveredLogs {
		c.Raft.log = append(c.Raft.log, log)

		if err := c.applyLogEntry(log); err != nil {
			return fmt.Errorf("failed to apply log entry %d: %w", log.Index, err)
		}

		c.Raft.lastApplied = log.Index

	}

	fmt.Printf("Recovery complete: %d entries, term=%d, lastApplied=%d\n",
		len(recoveredLogs), maxTerm, maxIndex)
	return nil
}

func (c *RaftController) applyLogEntry(log *pc.LogEntry) error {
	c.Metadata.DecodeLog(log, c)
	return nil
}

func (c *RaftController) validateLogContinuity(logs []*pc.LogEntry) error {
	if len(logs) == 0 {
		return nil
	}
	expectedIndex := logs[0].Index
	for i, log := range logs {
		if log.Index != expectedIndex {
			return fmt.Errorf("log gap detected: expected index %d, got %d at position %d",
				expectedIndex, log.Index, i)
		}
		expectedIndex++
	}

	return nil
}

func (c *RaftController) ApplyCreateTopic(ctc *pc.Command_CreateTopic) {
	if c.Raft.state == Leader {
		c.electPartitionsLeaders()
	}
}

func (c *RaftController) ApplyRegisterBroker(ctc *pc.Command_RegisterBroker) {
	if c.Raft.state == Leader {
		c.electPartitionsLeaders()
	}
}

func (c *RaftController) ApplyUpdateBroker(ctc *pc.Command_UpdateBroker) {
	brk := &model.BrokerInfo{
		ID:       ctc.UpdateBroker.Id,
		Address:  ctc.UpdateBroker.Address,
		Alive:    ctc.UpdateBroker.Alive,
		LastSeen: ctc.UpdateBroker.LastSeen.AsTime(),
	}
	if !brk.Alive {
		c.removeFromISR(brk)
		if c.Raft.state == Leader {
			c.brokerFailOver(ctc.UpdateBroker.Id)
		}
	}
}

func (c *RaftController) ApplyUpdatePartitionLeader(ctc *pc.Command_ChangePartitionLeader) {
	for _, assignment := range ctc.ChangePartitionLeader.Assignments {
		t, ok := c.Metadata.Topic(assignment.TopicId)
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

func (c *RaftController) electPartitionsLeaders() {
	brokers := make([]*pb.BrokerInfo, 0)
	for _, brkr := range c.Metadata.Brokers() {
		if brkr.Alive {
			brokers = append(brokers, brkr)
		}
	}

	if len(brokers) == 0 {
		return // No alive brokers
	}

	sort.Slice(brokers, func(i, j int) bool {
		return brokers[i].Id < brokers[j].Id
	})

	brokerIndex := 0

	assigments := make([]*pc.PartitionAssignment, 0)

	t := c.Metadata.Topics()
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
		c.Raft.SubmitCommand(cmd)
	}
}

func (c *RaftController) brokerFailOver(leaderID string) {
	if len(c.Metadata.Brokers()) == 0 {
		return // No alive brokers
	}
	assigments := make([]*pc.PartitionAssignment, 0)
	for _, topic := range c.Metadata.Topics() {
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
		c.Raft.SubmitCommand(cmd)
	}
}

func (c *RaftController) isBrokerAlive(brokerID string) bool {
	brks := c.Metadata.Brokers()
	b, ok := brks[brokerID]
	if !ok {
		return false
	}
	return b.Alive
}

func (c *RaftController) RegisterBroker(r *pc.BrokerRegisterRequest) error {
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
	return c.submitCommandGRPC(cmd)
}

func (s *RaftController) submitCommandGRPC(cmd *pc.Command) error {
	err := s.Raft.SubmitCommand(cmd)
	if err != nil {
		return s.isLeader()
	}
	return nil
}

func (s *RaftController) isLeader() error {
	if s.Raft.state == Leader {
		return nil
	}
	leader := s.Raft.leaderID
	if leader == "" {
		return status.Error(codes.Unavailable, "no leader available")
	}

	addr, ok := s.Peers[leader]
	if !ok {
		return status.Error(codes.Unavailable, "leader address not found")
	}

	// Create status with metadata containing leader address
	errorMsg := fmt.Sprintf("not leader|%s|%s", leader, addr)
	return status.Error(codes.FailedPrecondition, errorMsg)
}

func (c *RaftController) brokerHeartbeat(brokerID string) error {
	if notLeader := c.isLeader(); notLeader != nil {
		log.Println("broker heartbeat", brokerID, notLeader)
		return notLeader
	}
	brsk := c.Metadata.Brokers()
	b, ok := brsk[brokerID]
	if !ok {
		log.Println("cannot find broker", brokerID)
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
func (c *RaftController) brokerMetadata(index int64) ([]*pc.LogEntry, error) {
	return c.Raft.LogFromIndex(index)
}

func (c *RaftController) monitorDeadSessions() {
	ticker := time.NewTicker(500 * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			c.cleanDeadSessions()

		}
	}
}

func (c *RaftController) cleanDeadSessions() {
	if !c.Raft.IsLeader() {
		return
	}
	for _, broker := range c.Metadata.Brokers() {
		if time.Now().Sub(broker.LastSeen.AsTime()) > c.timeout && broker.Alive {
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

func (c *RaftController) commandChangeBroker(brokerInfo *pb.BrokerInfo) {
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
	c.Raft.SubmitCommand(cmd)
}

func (c *RaftController) removeFromISR(brokerInfo *model.BrokerInfo) {
	for _, t := range c.Metadata.Topics() {
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
