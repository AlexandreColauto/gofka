package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	pb "github.com/alexandrecolauto/gofka/proto/controller"
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

	currentTerm int64
	votedFor    string
	log         []*pb.LogEntry

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

	applyCh      chan *pb.LogEntry
	shutdownCh   chan any
	leadershipCh chan bool

	onLeadershipChange       func(isLeader bool)
	sendVoteRequest          func(address string, request *pr.VoteRequest) (*pr.VoteResponse, error)
	sendAppendEntriesRequest func(address string, request *pr.AppendEntriesRequest) (*pr.AppendEntriesResponse, error)
}

func NewRaftModule(id string, peers map[string]string, applyCh chan *pb.LogEntry) *RaftModule {
	rm := &RaftModule{
		id:                 id,
		peers:              peers,
		state:              Follower,
		currentTerm:        0,
		votedFor:           "",
		log:                make([]*pb.LogEntry, 1),
		commitIndex:        0,
		lastApplied:        0,
		nextIndex:          make(map[string]int64),
		matchIndex:         make(map[string]int64),
		applyCh:            applyCh,
		shutdownCh:         make(chan any),
		leadershipCh:       make(chan bool),
		onLeadershipChange: func(isLeader bool) {},
	}

	rm.log[0] = &pb.LogEntry{Term: 0, Index: 0, Command: nil}
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
			vote := rm.requestVote(peerID, term, lastLogIndex, lastLogTerm)
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
			fmt.Printf("Node %s is becoming leader: %s\n", rm.id, rm.leaderID)
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
func (rm *RaftModule) requestVote(peerID string, term, lastLogIndex, lastLogTerm int64) bool {
	request := &pr.VoteRequest{
		Cadidateid:   rm.id,
		Term:         term,
		Lastlogindex: lastLogIndex,
		Lastlogterm:  lastLogTerm,
	}

	response, err := rm.sendVoteRequest(peerID, request)
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
			for peerID := range rm.peers {
				go rm.sendAppendEntries(peerID, term)
			}
		}
	}
}

func (rm *RaftModule) sendAppendEntries(peerID string, term int64) {
	rm.mu.RLock()
	if rm.state != Leader || rm.currentTerm != term {
		rm.mu.RUnlock()
		return
	}

	nextIndex := rm.nextIndex[peerID]
	prevIndex := nextIndex - 1
	if int(prevIndex) >= len(rm.log) {
		rm.mu.RUnlock()
		return
	}
	prevTerm := rm.log[prevIndex].Term
	var entries []*pb.LogEntry

	if nextIndex < int64(len(rm.log)) {
		entries = rm.log[nextIndex:]
	}

	request := &pr.AppendEntriesRequest{
		Term:         rm.currentTerm,
		LeaderID:     rm.id,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: rm.commitIndex,
	}
	rm.mu.RUnlock()

	response, err := rm.sendAppendEntriesRequest(peerID, request)
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

func (rm *RaftModule) setSendAppendEntriesRequest(sendAppendEntriesRequest func(address string, request *pr.AppendEntriesRequest) (*pr.AppendEntriesResponse, error)) {
	rm.sendAppendEntriesRequest = sendAppendEntriesRequest
}

func (rm *RaftModule) setSendVoteRequest(sendVoteRequest func(address string, request *pr.VoteRequest) (*pr.VoteResponse, error)) {
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

func (rm *RaftModule) processVoteRequest(req *pr.VoteRequest) *pr.VoteResponse {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	response := &pr.VoteResponse{Term: rm.currentTerm, Vote: false}

	if req.Term > rm.currentTerm {
		rm.becomeFollower(req.Term)
	}

	if req.Term == rm.currentTerm &&
		(rm.votedFor == "" || rm.votedFor == req.Cadidateid) &&
		rm.isLogUptoDate(req.Lastlogindex, req.Lastlogterm) {
		rm.votedFor = req.Cadidateid
		response.Vote = true
		fmt.Println("Voted for : ", req.Cadidateid)
		rm.resetElectionTimer()
	}

	response.Term = rm.currentTerm
	return response
}

func (rm *RaftModule) processAppendRequest(req *pr.AppendEntriesRequest) *pr.AppendEntriesResponse {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	response := &pr.AppendEntriesResponse{Term: rm.currentTerm, Success: false}
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

func (rm *RaftModule) SubmitCommand(command *pb.Command) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rm.state != Leader {
		return fmt.Errorf("not the leader")
	}

	entry := &pb.LogEntry{
		Term:    rm.currentTerm,
		Index:   int64(len(rm.log)),
		Command: command,
	}

	rm.log = append(rm.log, entry)
	return nil
}

func (rm *RaftModule) InitLog(command *pb.Command) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	entry := &pb.LogEntry{
		Term:    rm.currentTerm,
		Index:   int64(len(rm.log)),
		Command: command,
	}

	rm.log = append(rm.log, entry)
	return nil
}
func (rm *RaftModule) IsLeader() bool {
	return rm.state == Leader
}
func (rm *RaftModule) LogFromIndex(index int64) ([]*pb.LogEntry, error) {
	if index > int64(len(rm.log)) {
		return nil, fmt.Errorf("invalid index: %d %+v", index, rm.log)
	}
	if index == int64(len(rm.log)) {
		return nil, nil
	}
	res := make([]*pb.LogEntry, int64(len(rm.log))-index)
	copy(res, rm.log[index:])
	fmt.Println("Returning :", len(res), index)
	for _, log := range rm.log {
		fmt.Println("Returning :", log)
	}
	return res, nil
}
