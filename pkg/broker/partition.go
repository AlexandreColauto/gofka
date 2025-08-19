package broker

import (
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/alexandrecolauto/gofka/pkg/log"
)

type Partition struct {
	id  int
	dir string
	log *log.Log

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
	l, err := log.NewLog(partitionDir)
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

func (p *Partition) ReadFrom(offset int64, opt *log.ReadOpts) ([]*log.Message, error) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	// Consumers can only read up to the high water mark
	if offset > p.hwm {
		return []*log.Message{}, nil
	}
	return p.log.ReadBatch(offset, opt)
}

func (p *Partition) ReadFromReplica(offset int64, opt *log.ReadOpts) ([]*log.Message, error) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.log.ReadBatch(offset, opt)
}

func (p *Partition) Append(message *log.Message) (int64, error) {
	if message == nil {
		return 0, fmt.Errorf("empty message")
	}
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if !p.leader {
		return 0, fmt.Errorf("not leader of partition %d", p.id)
	}

	offset, err := p.log.Append(message)
	if err != nil {
		return 0, err
	}

	message.UpdateOffset(offset)

	p.leo = offset + 1
	return offset, nil
}

func (p *Partition) InitializeReplication(bokerId string, replicas []string, isLeader bool) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.replicas = replicas
	p.leader = isLeader

	if isLeader {
		for _, replicaID := range replicas {
			if replicaID != bokerId {
				p.followerStates[replicaID] = &FollowerState{
					lastFetchOffset: time.Now(),
				}
			}
		}

		p.isr = []string{bokerId}
	}

	p.leo = p.log.Size()
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

func (p *Partition) BecomeLeader(brokerID string, epoch int64) {
	fmt.Printf("Becoming leader of %d\n", p.id)
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

func (p *Partition) TruncateToOffset(offset int64) error {
	return nil
}

func (p *Partition) RemoveFromISR(replicaID string) error {
	return nil
}

func (p *Partition) ExpireFollowers(timeout time.Duration) error {
	return nil
}
