package raft

import (
	"fmt"
	"testing"
	"time"

	"github.com/alexandrecolauto/gofka/common/proto/controller"
	pr "github.com/alexandrecolauto/gofka/common/proto/raft"

	"github.com/stretchr/testify/assert"
)

func AddressPool() []string {
	return []string{
		"localhost:42069",
		"localhost:42070",
		"localhost:42071",
		"localhost:42072",
		"localhost:42073",
	}
}

type MockServer struct {
	applyCh map[string]chan *controller.LogEntry
	raft    map[string]*RaftModule
}

func NewMockServer() *MockServer {
	ch := make(map[string]chan *controller.LogEntry)
	rf := make(map[string]*RaftModule)
	return &MockServer{
		applyCh: ch,
		raft:    rf,
	}
}

func (s *MockServer) sendAppendEntriesRequest(address string, request *pr.AppendEntriesRequest) (*pr.AppendEntriesResponse, error) {
	r, ok := s.raft[address]
	if !ok {
		panic(fmt.Sprintf("cannot find raft with address %s", address))
	}
	res := r.ProcessAppendRequest(request)
	return res, nil
}

func (s *MockServer) sendVoteRequest(address string, request *pr.VoteRequest) (*pr.VoteResponse, error) {
	r, ok := s.raft[address]
	if !ok {
		panic(fmt.Sprintf("cannot find raft with address %s", address))
	}
	res := r.ProcessVoteRequest(request)
	return res, nil
}

func (sv *MockServer) SetupRaftTest(n int) {
	adddress := AddressPool()
	peers := make(map[string]string)
	for id, peer := range adddress {
		peers[fmt.Sprintf("raft-%d", id)] = peer
	}
	id := fmt.Sprintf("raft-%d", n)

	ch := make(chan *controller.LogEntry)
	timeout := time.Duration(BASE_ELECTION_TIME+(n*50)) * time.Millisecond
	rf := NewRaftModule(id, peers, ch, sv.sendAppendEntriesRequest, sv.sendVoteRequest)
	rf.timers.testTimeout = timeout
	sv.applyCh[id] = ch
	sv.raft[id] = rf
}

func (sv *MockServer) Start() {
	for _, raft := range sv.raft {
		raft.Start()
	}
}

func TestRaftFirstElection(t *testing.T) {
	t.Log("Testing raft")
	sv := NewMockServer()
	for i := range 5 {
		sv.SetupRaftTest(i)
	}
	t.Log("Starting raft")
	sv.Start()

	leaderID := sv.waitForLeader(t)

	assert.NotEmpty(t, leaderID, "Cannot elect leader after setup")
	assert.Equal(t, "raft-0", leaderID, "Cannot elect leader after setup")
}

func TestRaftFirstElectionWithHigherLog(t *testing.T) {
	t.Log("Testing raft")
	sv := NewMockServer()
	for i := range 5 {
		sv.SetupRaftTest(i)
	}
	t.Log("Starting raft with a single above all")
	leaderID := ""
	r := sv.raft["raft-1"]
	r.raftLog.log = append(r.raftLog.log, &controller.LogEntry{Term: 1, Index: 1, Command: nil})
	r.raftLog.log = append(r.raftLog.log, &controller.LogEntry{Term: 1, Index: 2, Command: nil})
	r.election.currentTerm = 1
	r.replication.commitIndex = 1
	r.replication.lastApplied = 1
	t.Log(r.id, "is above all ")
	leaderID = r.id
	sv.Start()

	leader := sv.waitForGivenLeader(leaderID, t)
	assert.True(t, leaderID == leader, "Cannot elect leader after setup")
}

func TestLowerLogCannotBeLeader(t *testing.T) {
	sv := NewMockServer()
	for i := range 5 {
		sv.SetupRaftTest(i)
	}
	t.Log("Starting raft with a single above all")
	leaderID := "raft-0"
	for _, r := range sv.raft {
		if r.id != leaderID {
			initLog(r)
		}
	}
	sv.Start()

	leader := sv.waitForLeader(t)
	assert.True(t, leaderID != leader, "Cannot elect leader after setup")
}

func initLog(r *RaftModule) {
	r.raftLog.log = append(r.raftLog.log, &controller.LogEntry{Term: 1, Index: 1, Command: nil})
	r.raftLog.log = append(r.raftLog.log, &controller.LogEntry{Term: 1, Index: 2, Command: nil})
	r.election.currentTerm = 2
	r.replication.commitIndex = 2
	r.replication.lastApplied = 2
}

func (sv *MockServer) waitForLeader(t *testing.T) string {
	timeout := time.After(3 * time.Second)           // A generous timeout for a local test
	ticker := time.NewTicker(100 * time.Millisecond) // Poll every 100ms
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			// If we time out, fail the test with a helpful message.
			t.Fatal("timed out while waiting for a leader to be elected")
			return ""
		case <-ticker.C:
			var leaders []string
			for id, r := range sv.raft {
				// We check the internal state here. Note that in a real Raft implementation,
				// you might have a public method like r.IsLeader().
				if r.state == Leader {
					leaders = append(leaders, id)
				}
			}
			// We found exactly one leader. Success!
			if len(leaders) == 1 {
				t.Logf("Leader elected: %s", leaders[0])
				return leaders[0]
			}
			// This case is also important: a "split brain" scenario where multiple leaders exist.
			// This indicates a bug in the Raft implementation.
			if len(leaders) > 1 {
				t.Fatalf("multiple leaders found: %v", leaders)
				return ""
			}
		}
	}
}
func (sv *MockServer) waitForGivenLeader(leader string, t *testing.T) string {
	timeout := time.After(3 * time.Second)           // A generous timeout for a local test
	ticker := time.NewTicker(100 * time.Millisecond) // Poll every 100ms
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			// If we time out, fail the test with a helpful message.
			t.Fatal("timed out while waiting for a leader to be elected")
			return ""
		case <-ticker.C:
			var leaders []string
			for id, r := range sv.raft {
				// We check the internal state here. Note that in a real Raft implementation,
				// you might have a public method like r.IsLeader().
				if r.state == Leader {
					leaders = append(leaders, id)
				}
			}
			// We found exactly one leader. Success!
			if len(leaders) == 1 {
				t.Logf("Leader elected: %s", leaders[0])
				l := leaders[0]
				if l == leader {
					return leaders[0]
				}
			}
			// This case is also important: a "split brain" scenario where multiple leaders exist.
			// This indicates a bug in the Raft implementation.
			if len(leaders) > 1 {
				t.Fatalf("multiple leaders found: %v", leaders)
				return ""
			}
		}
	}
}
