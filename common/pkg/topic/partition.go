package topic

import (
	"fmt"
	"path/filepath"
	"slices"
	"sync"
	"time"

	pb "github.com/alexandrecolauto/gofka/common/proto/broker"
)

type Partition struct {
	id  int
	dir string
	log *Log

	leader      bool
	leaderID    string
	replicas    []string
	isr         []string
	leaderEpoch int64
	hwm         int64
	leo         int64

	followerStates map[string]*FollowerState

	mutex sync.RWMutex

	shutdownOnce sync.Once
	shutdownCh   chan any
	isShutdown   bool
}

type FollowerState struct {
	lastFetchOffset time.Time
	fetchOffset     int64
	longEndOffset   int64
	inSync          bool
}

func NewPartition(topicName string, id int, shutdownCh chan any) (*Partition, error) {
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
		leo:            l.Size(),
		followerStates: make(map[string]*FollowerState),
		shutdownCh:     shutdownCh,
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
	if len(p.replicas) <= 1 {
		p.hwm = p.leo
	}
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
	// fmt.Println("updating follower state: ", followerID, p.leaderID)

	state.lastFetchOffset = time.Now()
	state.fetchOffset = fetchOffset
	state.longEndOffset = logEndOffset

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

	newISR := []string{p.leaderID}
	for followerID, f_state := range p.followerStates {
		if followerID == p.leaderID {
			continue
		}
		f_state.inSync = (p.leo - f_state.longEndOffset) <= 1
		if f_state.inSync {
			newISR = append(newISR, followerID)
			// fmt.Printf("%s - %s in sync: %d %d (%d)  \n", p.leaderID, followerID, f_state.longEndOffset, p.leo, p.leo-f_state.longEndOffset)
		} else {
			fmt.Printf("%s - %s out of sync: %d %d (%d)  \n", p.leaderID, followerID, f_state.longEndOffset, p.leo, p.leo-f_state.longEndOffset)
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
		if replicaID == p.leaderID {
			continue
		}
		if state, exists := p.followerStates[replicaID]; exists {
			if state.longEndOffset < minOffset {
				fmt.Println("lower leo", state)
				minOffset = state.longEndOffset
			}
		}
	}
	if p.hwm != minOffset {
		fmt.Println("New HWM: ", p.hwm)
	}
	p.hwm = minOffset
	return nil
}

func (p *Partition) Leo() int64 {
	return p.leo
}

func (p *Partition) HWM() int64 {
	return p.hwm
}

func (p *Partition) BecomeLeader(brokerID string, epoch int64, replicas []string) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.leader = true
	p.leaderEpoch = epoch
	p.leo = p.log.Size()
	p.hwm = p.leo
	p.replicas = replicas

	p.followerStates = make(map[string]*FollowerState)
	for _, replicaID := range p.replicas {
		p.followerStates[replicaID] = &FollowerState{
			lastFetchOffset: time.Now(),
		}
	}
	p.leaderID = brokerID

	p.isr = []string{brokerID}
}

func (p *Partition) BecomeFollower(brokerID string, epoch int64, replicas []string) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.leader = false
	p.leaderID = brokerID
	p.leaderEpoch = epoch
	p.leo = p.log.Size()
	p.followerStates = make(map[string]*FollowerState)
	p.isr = []string{}
	p.replicas = replicas
}

func (p *Partition) Isr() []string {
	return p.isr
}
func (p *Partition) LaggingReplicas(timeout time.Duration) (bool, []string) {
	is_lagging := false
	outOfSync := []string{}
	if !p.leader {
		return false, outOfSync
	}
	for _, replica := range p.replicas {
		if !p.isISR(replica) {
			outOfSync = append(outOfSync, replica)
		}
	}
	for _, replicaID := range outOfSync {
		fs := p.followerStates[replicaID]
		if !fs.inSync && time.Since(fs.lastFetchOffset) > timeout {
			is_lagging = true
			fmt.Println("FOUND LAGGING ITEM", replicaID, p.leaderID, time.Since(fs.lastFetchOffset), fs.inSync, fs.fetchOffset, p.leo)
			fmt.Println("All replicas ", p.replicas)
			fmt.Println("isr ", p.isr)
			for id, st := range p.followerStates {
				fmt.Println("follower state ", id, st)
			}

		}
	}
	return is_lagging, p.isr
}

func (p *Partition) isISR(id string) bool {
	return slices.Contains(p.isr, id)
}

func (p *Partition) Shutdown() error {
	var shutErr error
	p.shutdownOnce.Do(func() {
		if p.isShutdown {
			return
		}
		p.isShutdown = true

		err := p.log.Shutdown()
		if shutErr == nil {
			shutErr = err
		}
	})
	return shutErr
}
