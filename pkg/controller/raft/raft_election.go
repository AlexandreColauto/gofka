package raft

import (
	"fmt"
	"log"
	"time"

	pr "github.com/alexandrecolauto/gofka/proto/raft"
)

const BASE_ELECTION_TIME = 750

type Election struct {
	currentTerm int64
	votedFor    string
	leaderID    string
}

func (rm *RaftModule) runElection() {
	rm.mu.Lock()
	if rm.state == Leader {
		rm.mu.Unlock()
		return
	}

	rm.state = Candidate
	rm.election.currentTerm++
	rm.election.votedFor = rm.id
	rm.resetElectionTimer()

	term := rm.election.currentTerm
	lastLogIndex := int64(len(rm.raftLog.log) - 1)
	lastLogTerm := rm.raftLog.log[lastLogIndex].Term

	rm.mu.Unlock()

	votes := 1
	voteCh := make(chan bool, len(rm.peers))

	rm.requestVoteToPeers(voteCh, term, lastLogIndex, lastLogTerm)

	for i := 0; i < len(rm.peers); i++ {
		select {
		case v := <-voteCh:
			if v {
				votes++
			}
		case <-time.After(150 * time.Millisecond):
		}
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()
	if rm.state == Candidate && rm.election.currentTerm == term {
		majority := len(rm.peers)/2 + 1
		if votes >= majority {
			rm.becomeLeader()
			log.Printf("Node %s is becoming leader with %d votes: %s at term: %d\n", rm.id, votes, rm.election.leaderID, term)
		} else {
			rm.becomeFollower(rm.election.currentTerm)
		}
	}
}

func (rm *RaftModule) requestVoteToPeers(voteCh chan bool, term, lastLogIndex, lastLogTerm int64) {
	for peerID, peerAddr := range rm.peers {
		go func(id, address string) {
			vote := rm.requestVote(peerID, term, lastLogIndex, lastLogTerm)
			voteCh <- vote

		}(peerID, peerAddr)
	}
}
func (rm *RaftModule) requestVote(peerID string, term, lastLogIndex, lastLogTerm int64) bool {
	request := &pr.VoteRequest{
		Cadidateid:   rm.id,
		Term:         term,
		Lastlogindex: lastLogIndex,
		Lastlogterm:  lastLogTerm,
	}

	response, err := rm.server.sendVoteRequest(peerID, request)
	if err != nil {
		// log.Println("error sending vote", err)
		return false
	}
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if response.Term > rm.election.currentTerm {
		rm.becomeFollower(response.Term)
	}
	return response.Vote && response.Term == term
}

func (rm *RaftModule) becomeFollower(term int64) {
	rm.state = Follower
	rm.election.currentTerm = term
	rm.election.votedFor = ""
	if rm.timers.heartbeatTimer != nil {
		rm.timers.heartbeatTimer.Stop()
		rm.timers.heartbeatTimer = nil
	}
	rm.resetElectionTimer()
}

func (rm *RaftModule) becomeLeader() {
	rm.state = Leader

	lastLogIndex := int64(len(rm.raftLog.log) - 1)
	for peerID := range rm.peers {
		rm.replication.nextIndex[peerID] = lastLogIndex + 1
		rm.replication.matchIndex[peerID] = 0
	}
	if rm.timers.electionTimer != nil {
		rm.timers.electionTimer.Stop()
	}

	rm.timers.heartbeatTimer = time.NewTicker(50 * time.Millisecond)
	go rm.runHeartbeatTimer()
	rm.server.resetStartupTime()

	if rm.visualizerClient != nil {
		action := "leader"
		target := rm.id
		msg := fmt.Sprintf("controller %s just become leader", rm.id)
		rm.visualizerClient.SendMessage(action, target, []byte(msg))
	}
}

func (rm *RaftModule) ProcessVoteRequest(req *pr.VoteRequest) *pr.VoteResponse {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	response := &pr.VoteResponse{Term: rm.election.currentTerm, Vote: false}

	if req.Term > rm.election.currentTerm {
		rm.becomeFollower(req.Term)
	}

	if req.Term == rm.election.currentTerm &&
		(rm.election.votedFor == "" || rm.election.votedFor == req.Cadidateid) &&
		rm.isLogUptoDate(req.Lastlogindex, req.Lastlogterm) {
		rm.election.votedFor = req.Cadidateid
		response.Vote = true
		rm.resetElectionTimer()
	}

	response.Term = rm.election.currentTerm
	return response
}

func (rm *RaftModule) isLogUptoDate(lastIndex, lastTerm int64) bool {
	myIndex := int64(len(rm.raftLog.log) - 1)
	myTerm := rm.raftLog.log[myIndex].Term
	if lastTerm != myTerm {
		return lastTerm > myTerm
	}
	return lastIndex >= myIndex
}
