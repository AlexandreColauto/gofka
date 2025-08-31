package raft

import (
	"sync"
	"time"

	pb "github.com/alexandrecolauto/gofka/proto/controller"
	pc "github.com/alexandrecolauto/gofka/proto/controller"
	pr "github.com/alexandrecolauto/gofka/proto/raft"
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

	election    Election
	raftLog     RaftLog
	replication Replication
	timers      Timers
	server      ServerFuncs
}

type ServerFuncs struct {
	shutdownCh               chan any
	onLeadershipChange       func(isLeader bool)
	sendVoteRequest          func(address string, request *pr.VoteRequest) (*pr.VoteResponse, error)
	sendAppendEntriesRequest func(address string, request *pr.AppendEntriesRequest) (*pr.AppendEntriesResponse, error)
}

func NewRaftModule(
	id string,
	peers map[string]string,
	applyCh chan *pb.LogEntry,
	sendAppendEntriesRequest func(address string, request *pr.AppendEntriesRequest) (*pr.AppendEntriesResponse, error),
	sendVoteRequest func(address string, request *pr.VoteRequest) (*pr.VoteResponse, error),
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
		shutdownCh:               make(chan any),
		sendAppendEntriesRequest: sendAppendEntriesRequest,
		sendVoteRequest:          sendVoteRequest,
	}

	rm := &RaftModule{
		id:          id,
		peers:       peers,
		state:       Follower,
		election:    e,
		raftLog:     l,
		replication: r,
		timers:      t,
		server:      s,
	}

	rm.raftLog.log[0] = &pb.LogEntry{Term: 0, Index: 0, Command: nil}
	return rm
}

func (rm *RaftModule) Start() {
	rm.resetElectionTimer()
	go rm.runElectionTimer()
	go rm.applyLogs()
}

func (rm *RaftModule) applyLogs() {
	for {
		select {
		case <-rm.server.shutdownCh:
			return
		case <-time.After(11 * time.Millisecond):
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
