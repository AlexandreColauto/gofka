package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/alexandrecolauto/gofka/model"
)

type State string

const (
	Follower  State = "Follower"
	Candidate State = "Canditate"
	Leader    State = "Leader"
)

type RaftModule struct {
	mu sync.RWMutex

	currentTerm int64
	votedFor    string
	log         []model.LogEntry

	state          State
	id             string
	leaderID       string
	peers          map[string]string
	commitIndex    int64
	lastApplied    int64
	electionTimer  *time.Timer
	heartbeatTimer *time.Ticker

	nextIndex  map[string]int64
	matchIndex map[string]int64

	applyCh      chan model.LogEntry
	shutdownCh   chan any
	leadershipCh chan bool

	onLeadershipChange       func(isLeader bool)
	sendVoteRequest          func(address string, request *model.VoteRequest) (*model.VoteResponse, error)
	sendAppendEntriesRequest func(address string, request *model.AppendEntriesRequest) (*model.AppendEntriesResponse, error)
}

func NewRaftModule(id string, peers map[string]string, applyCh chan model.LogEntry) *RaftModule {
	rm := &RaftModule{
		id:                 id,
		peers:              peers,
		state:              Follower,
		currentTerm:        0,
		votedFor:           "",
		log:                make([]model.LogEntry, 1),
		commitIndex:        0,
		lastApplied:        0,
		nextIndex:          make(map[string]int64),
		matchIndex:         make(map[string]int64),
		applyCh:            applyCh,
		shutdownCh:         make(chan any),
		leadershipCh:       make(chan bool),
		onLeadershipChange: func(isLeader bool) {},
	}

	rm.log[0] = model.LogEntry{Term: 0, Index: 0, Command: nil}
	rm.resetElectionTimer()
	go rm.runElectionTimer()
	go rm.applyLogs()
	return rm
}

func (rm *RaftModule) resetElectionTimer() {
	timeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
	if rm.electionTimer == nil {
		rm.electionTimer = time.NewTimer(timeout)
	} else {
		rm.electionTimer.Reset(timeout)
	}
}
func (rm *RaftModule) runElectionTimer() {
	for {
		select {
		case <-rm.electionTimer.C:
			rm.runElection()
		case <-rm.shutdownCh:
			return
		}
	}

}

func (rm *RaftModule) runElection() {
	rm.mu.Lock()
	if rm.state == Leader {
		rm.mu.Unlock()
		return
	}

	rm.state = Candidate
	rm.currentTerm++
	rm.votedFor = rm.id
	rm.resetElectionTimer()

	term := rm.currentTerm
	lastLogIndex := int64(len(rm.log) - 1)
	lastLogTerm := rm.log[lastLogIndex].Term

	rm.mu.Unlock()

	votes := 1
	voteCh := make(chan bool, len(rm.peers))

	for peerID, peerAddr := range rm.peers {
		go func(id, address string) {
			vote := rm.requestVote(address, term, lastLogIndex, lastLogTerm)
			voteCh <- vote

		}(peerID, peerAddr)
	}

	for i := 0; i < len(rm.peers); i++ {
		select {
		case v := <-voteCh:
			if v {
				votes++
			}
		case <-time.After(100 * time.Millisecond):
		}
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()
	if rm.state == Candidate && rm.currentTerm == term {
		majority := len(rm.peers)/2 + 1
		if votes >= majority {
			rm.becomeLeader()
		} else {
			rm.becomeFollower(rm.currentTerm)
		}
	}

}

func (rm *RaftModule) becomeLeader() {
	rm.state = Leader

	lastLogIndex := int64(len(rm.log) - 1)
	for peerID := range rm.peers {
		rm.nextIndex[peerID] = lastLogIndex + 1
		rm.matchIndex[peerID] = 0
	}
	if rm.electionTimer != nil {
		rm.electionTimer.Stop()
	}

	rm.heartbeatTimer = time.NewTicker(50 * time.Millisecond)
	go rm.sendHeartbeats()

	rm.onLeadershipChange(true)
}
func (rm *RaftModule) becomeFollower(term int64) {
	wasLeader := rm.state == Leader

	rm.state = Follower
	rm.currentTerm = term
	rm.votedFor = ""
	if rm.heartbeatTimer != nil {
		rm.heartbeatTimer.Stop()
		rm.heartbeatTimer = nil
	}

	rm.resetElectionTimer()

	if wasLeader {
		rm.onLeadershipChange(false)
	}
}
func (rm *RaftModule) requestVote(address string, term, lastLogIndex, lastLogTerm int64) bool {
	request := &model.VoteRequest{
		CadidateID:   rm.id,
		Term:         term,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	response, err := rm.sendVoteRequest(address, request)
	if err != nil {
		return false
	}
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if response.Term > rm.currentTerm {
		rm.becomeFollower(response.Term)
	}

	return response.Vote && response.Term == term
}

func (rm *RaftModule) sendHeartbeats() {
	defer rm.heartbeatTimer.Stop()

	for {
		select {
		case <-rm.heartbeatTimer.C:
			rm.mu.RLock()
			if rm.state != Leader {
				rm.mu.RUnlock()
				return
			}

			term := rm.currentTerm
			rm.mu.RUnlock()
			for peerID, peerAddr := range rm.peers {
				go rm.sendAppendEntries(peerID, peerAddr, term)
			}
		}
	}
}

func (rm *RaftModule) sendAppendEntries(peerID, address string, term int64) {
	rm.mu.RLock()
	if rm.state != Leader || rm.currentTerm != term {
		rm.mu.RUnlock()
		return
	}

	nextIndex := rm.nextIndex[peerID]
	prevIndex := nextIndex - 1
	prevTerm := rm.log[prevIndex].Term
	var entries []model.LogEntry

	if nextIndex < int64(len(rm.log)) {
		entries = rm.log[nextIndex:]
	}

	request := &model.AppendEntriesRequest{
		Term:         rm.currentTerm,
		LeaderID:     rm.id,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: rm.commitIndex,
	}
	rm.mu.RUnlock()

	response, err := rm.sendAppendEntriesRequest(address, request)
	if err != nil {
		return
	}
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rm.state != Leader || rm.currentTerm != term {
		return
	}

	if response.Term > rm.currentTerm {
		rm.becomeFollower(response.Term)
		return
	}
	if response.Success {
		rm.nextIndex[peerID] = response.Index + 1
		rm.matchIndex[peerID] = response.Index
		rm.updateCommitIndex()
	} else {
		if rm.nextIndex[peerID] > 1 {
			rm.nextIndex[peerID]--
		}
	}

}

func (rm *RaftModule) setSendAppendEntriesRequest(sendAppendEntriesRequest func(address string, request *model.AppendEntriesRequest) (*model.AppendEntriesResponse, error)) {
	rm.sendAppendEntriesRequest = sendAppendEntriesRequest
}

func (rm *RaftModule) setSendVoteRequest(sendVoteRequest func(address string, request *model.VoteRequest) (*model.VoteResponse, error)) {
	rm.sendVoteRequest = sendVoteRequest
}

func (rm *RaftModule) updateCommitIndex() {
	if rm.state != Leader {
		return
	}
	for i := rm.commitIndex + 1; i < int64(len(rm.log)); i++ {
		if rm.log[i].Term != rm.currentTerm {
			continue
		}

		count := 1

		for peerID := range rm.peers {
			if rm.matchIndex[peerID] >= i {
				count++
			}
		}
		if count > len(rm.peers)/2 {
			rm.commitIndex = i
		}
	}
}

func (rm *RaftModule) applyLogs() {
	for {
		select {
		case <-rm.shutdownCh:
			return
		case <-time.After(11 * time.Millisecond):
			rm.mu.Lock()

			for rm.lastApplied < rm.commitIndex {
				rm.lastApplied++
				entry := rm.log[rm.lastApplied]

				select {
				case rm.applyCh <- entry:
				case <-rm.shutdownCh:
					fmt.Println("shuting down")
					rm.mu.Unlock()
					return
				}
			}

			rm.mu.Unlock()
		}
	}
}

func (rm *RaftModule) processVoteRequest(req model.VoteRequest) model.VoteResponse {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	response := model.VoteResponse{Term: rm.currentTerm, Vote: false}

	if req.Term > rm.currentTerm {
		rm.becomeFollower(req.Term)
	}

	if req.Term == rm.currentTerm &&
		(rm.votedFor == "" || rm.votedFor == req.CadidateID) &&
		rm.isLogUptoDate(req.LastLogIndex, req.LastLogTerm) {
		rm.votedFor = req.CadidateID
		response.Vote = true
		rm.resetElectionTimer()
	}

	response.Term = rm.currentTerm
	return response
}

func (rm *RaftModule) processAppendRequest(req model.AppendEntriesRequest) model.AppendEntriesResponse {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	response := model.AppendEntriesResponse{Term: rm.currentTerm, Success: false}
	if req.Term < rm.currentTerm {
		return response
	}
	if req.Term > rm.currentTerm {
		rm.becomeFollower(req.Term)
	}
	rm.resetElectionTimer()
	rm.leaderID = req.LeaderID

	if req.PrevLogIndex >= 0 {
		if req.PrevLogIndex >= int64(len(rm.log)) {
			response.Term = rm.currentTerm
			return response
		}

		if rm.log[req.PrevLogIndex].Term != req.PrevLogTerm {
			response.Term = rm.currentTerm
			return response
		}
	}

	if len(req.Entries) > 0 {
		insertIndex := req.PrevLogIndex + 1

		for i, newEntry := range req.Entries {
			logIndex := insertIndex + int64(i)

			if logIndex < int64(len(rm.log)) {
				if rm.log[logIndex].Term != newEntry.Term {
					rm.log = rm.log[:logIndex]
					rm.log = append(rm.log, req.Entries[i:]...)
					break
				}
			} else {
				rm.log = append(rm.log, req.Entries[i:]...)
				break
			}
		}
	}

	if req.LeaderCommit > rm.commitIndex {
		rm.commitIndex = min(req.LeaderCommit, int64(len(rm.log)-1))
	}

	response.Success = true
	response.Index = int64(len(rm.log) - 1)
	response.Term = rm.currentTerm
	return response
}

func (rm *RaftModule) isLogUptoDate(lastIndex, lastTerm int64) bool {
	myIndex := int64(len(rm.log) - 1)
	myTerm := rm.log[myIndex].Term
	if lastTerm != myTerm {
		return lastTerm > myTerm
	}
	return lastIndex >= myIndex
}

func (rm *RaftModule) SubmitCommand(command *model.Command) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rm.state != Leader {
		return fmt.Errorf("not the leader")
	}

	entry := model.LogEntry{
		Term:    rm.currentTerm,
		Index:   int64(len(rm.log)),
		Command: command,
	}

	rm.log = append(rm.log, entry)
	fmt.Printf("New log entry: %+v\n", entry)
	return nil
}
func (rm *RaftModule) IsLeader() bool {
	return rm.state == Leader
}
