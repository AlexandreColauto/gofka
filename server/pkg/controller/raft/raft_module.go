package raft

import (
	"fmt"
	"sync"
	"time"

	vC "github.com/alexandrecolauto/gofka/common/pkg/visualizer_client"
	pb "github.com/alexandrecolauto/gofka/common/proto/controller"
	pc "github.com/alexandrecolauto/gofka/common/proto/controller"
	pr "github.com/alexandrecolauto/gofka/common/proto/raft"
	"github.com/alexandrecolauto/gofka/server/pkg/config"
)

type State string

const (
	Follower  State = "Follower"
	Candidate State = "Canditate"
	Leader    State = "Leader"
)

type RaftModule struct {
	mu sync.RWMutex

	id    string
	state State

	peers map[string]string

	election         Election
	raftLog          RaftLog
	replication      Replication
	timers           Timers
	server           ServerFuncs
	visualizerClient *vC.VisualizerClient
	shutdownOnce     sync.Once
	wg               sync.WaitGroup
	isShutdown       bool
}

type ServerFuncs struct {
	shutdownCh               chan any
	onLeadershipChange       func(isLeader bool)
	sendVoteRequest          func(address string, request *pr.VoteRequest) (*pr.VoteResponse, error)
	sendAppendEntriesRequest func(address string, request *pr.AppendEntriesRequest) (*pr.AppendEntriesResponse, error)
	resetStartupTime         func()
}

func NewRaftModule(
	config *config.Config,
	applyCh chan *pb.LogEntry,
	shutdownCh chan any,
	sendAppendEntriesRequest func(address string, request *pr.AppendEntriesRequest) (*pr.AppendEntriesResponse, error),
	sendVoteRequest func(address string, request *pr.VoteRequest) (*pr.VoteResponse, error),
	resetStartupTime func(),
	vsualizerClient *vC.VisualizerClient,
) *RaftModule {
	e := Election{
		currentTerm: 0,
		votedFor:    "",
	}
	l := RaftLog{
		log:     make([]*pb.LogEntry, 1),
		applyCh: applyCh,
	}
	r := Replication{
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make(map[string]int64),
		matchIndex:  make(map[string]int64),
	}

	t := Timers{}

	s := ServerFuncs{
		shutdownCh:               shutdownCh,
		sendAppendEntriesRequest: sendAppendEntriesRequest,
		sendVoteRequest:          sendVoteRequest,
		resetStartupTime:         resetStartupTime,
	}

	rm := &RaftModule{
		id:               config.Server.NodeID,
		peers:            config.Server.Cluster.Peers,
		state:            Follower,
		election:         e,
		raftLog:          l,
		replication:      r,
		timers:           t,
		server:           s,
		visualizerClient: vsualizerClient,
	}

	rm.raftLog.log[0] = &pb.LogEntry{Term: 0, Index: 0, Command: nil}
	return rm
}

func (rm *RaftModule) Start() {
	rm.resetElectionTimer()
	rm.wg.Add(2)
	go rm.runElectionTimer()
	go rm.applyLogs()
}

func (rm *RaftModule) applyLogs() {
	defer rm.wg.Done()
	for {
		select {
		case <-rm.server.shutdownCh:
			return
		case <-time.After(250 * time.Millisecond):
			if rm.isShutdown {
				return
			}
			var entriesToApply []*pb.LogEntry

			rm.mu.Lock()
			if rm.replication.lastApplied < rm.replication.commitIndex {
				start := rm.replication.lastApplied + 1
				end := rm.replication.commitIndex
				entriesToApply = make([]*pb.LogEntry, end-start+1)
				copy(entriesToApply, rm.raftLog.log[start:end+1])
				rm.replication.lastApplied = end
			}
			rm.mu.Unlock()

			for _, entry := range entriesToApply {
				select {
				case rm.raftLog.applyCh <- entry:
				case <-rm.server.shutdownCh:
					return
				}
			}
		}
	}
}

func (rm *RaftModule) SetCurrentTerm(term int64) {
	rm.election.currentTerm = term
}

func (rm *RaftModule) SetLastApplied(lastApplied int64) {
	rm.replication.lastApplied = lastApplied
}

func (rm *RaftModule) SetCommitIndex(commitIndex int64) {
	rm.replication.commitIndex = commitIndex
}

func (rm *RaftModule) SetVotedFor(votedFor string) {
	rm.election.votedFor = votedFor
}

func (rm *RaftModule) AppendLog(log *pc.LogEntry) {
	rm.raftLog.log = append(rm.raftLog.log, log)
}

func (rm *RaftModule) Shutdown() error {
	var shutDownErr error
	rm.shutdownOnce.Do(func() {
		rm.isShutdown = true

		done := make(chan any)

		go func() {
			rm.wg.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			shutDownErr = fmt.Errorf("timeout: some goroutines didin't finish within 5 seconds")
		}

		close(rm.raftLog.applyCh)

	})
	return shutDownErr

}
