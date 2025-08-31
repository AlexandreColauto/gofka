package topic

import (
	"fmt"
	pb "github.com/alexandrecolauto/gofka/proto/broker"
	"path/filepath"
	"sync"
	"time"
)

type Partition struct {
	id  int
	dir string
	log *Log

	leader      bool
	replicas    []string
	isr         []string
	leaderEpoch int64
	hwm         int64
	leo         int64

	followerStates map[string]*FollowerState

	mutex sync.RWMutex
}

type FollowerState struct {
	lastFetchOffset time.Time
	fetchOffset     int64
	longEndOffset   int64
	inSync          bool
}

func NewPartition(topicName string, id int) (*Partition, error) {
	partitionDir := filepath.Join(topicName, fmt.Sprintf("%d", id))
	l, err := NewLog(partitionDir)
	if err != nil {
		return nil, err
	}
	return &Partition{
		id:             id,
		log:            l,
		dir:            partitionDir,
		leader:         false,
		replicas:       []string{},
		isr:            []string{},
		leaderEpoch:    0,
		hwm:            0,
		leo:            0,
		followerStates: make(map[string]*FollowerState),
	}, nil
}

func (p *Partition) ID() int {
	return p.id
}

func (p *Partition) Leader() bool {
	return p.leader
}

func (p *Partition) AppendBatch(batch []*pb.Message) (int64, error) {
	if len(batch) == 0 {
		return 0, fmt.Errorf("empty message")
	}
	p.mutex.Lock()
	defer p.mutex.Unlock()

	offset, err := p.log.AppendBatch(batch)
	if err != nil {
		return 0, err
	}

	p.leo = offset + 1
	return offset, nil
}

func (p *Partition) ReadFrom(offset int64, opt *pb.ReadOptions) ([]*pb.Message, error) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	if offset > p.hwm {
		return []*pb.Message{}, nil
	}
	return p.log.ReadBatch(offset, opt)
}

func (p *Partition) ReadFromReplica(offset int64, opt *pb.ReadOptions) ([]*pb.Message, error) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.log.ReadBatch(offset, opt)
}

func (p *Partition) UpdateFollowersState(followerID string, fetchOffset, logEndOffset int64) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if !p.leader {
		return fmt.Errorf("Not leader, cant update state")
	}

	state, exists := p.followerStates[followerID]
	if !exists {
		state = &FollowerState{}
		p.followerStates[followerID] = state
	}

	state.lastFetchOffset = time.Now()
	state.fetchOffset = fetchOffset
	state.longEndOffset = logEndOffset

	state.inSync = (p.leo - logEndOffset) <= 1

	err := p.updateISR()
	if err != nil {
		return err
	}
	err = p.updateHWM()
	return err
}

func (p *Partition) updateISR() error {
	if !p.leader {
		return fmt.Errorf("Not leader, cant update ISR")
	}

	newISR := []string{}
	for followerID, f_state := range p.followerStates {
		if f_state.inSync {
			newISR = append(newISR, followerID)
		}
	}

	p.isr = newISR
	return nil
}

func (p *Partition) updateHWM() error {
	if !p.leader {
		return fmt.Errorf("Not leader, cant update HWM")
	}
	if len(p.isr) == 0 {
		return fmt.Errorf("empty isr list")
	}

	minOffset := p.leo
	for _, replicaID := range p.isr {
		if state, exists := p.followerStates[replicaID]; exists {
			if state.longEndOffset < minOffset {
				minOffset = state.longEndOffset
			}
		}
	}
	p.hwm = minOffset
	return nil
}
func (p *Partition) Leo() int64 {
	return p.leo
}

func (p *Partition) BecomeLeader(brokerID string, epoch int64) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.leader = true
	p.leaderEpoch = epoch
	p.leo = p.log.Size()
	p.hwm = p.leo

	p.followerStates = make(map[string]*FollowerState)
	for _, replicaID := range p.replicas {
		p.followerStates[replicaID] = &FollowerState{
			lastFetchOffset: time.Now(),
		}
	}

	p.isr = []string{brokerID}
}

func (p *Partition) BecomeFollower(brokerID string, epoch int64) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.leader = false
	p.leaderEpoch = epoch
	p.followerStates = make(map[string]*FollowerState)
	p.isr = []string{}
}
