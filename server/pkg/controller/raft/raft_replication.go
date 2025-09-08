package raft

import (
	"fmt"
	"log"

	pb "github.com/alexandrecolauto/gofka/common/proto/controller"
	pr "github.com/alexandrecolauto/gofka/common/proto/raft"
)

type Replication struct {
	commitIndex int64
	lastApplied int64
	nextIndex   map[string]int64
	matchIndex  map[string]int64
}

func (rm *RaftModule) prepareAppendEntriesRequest(peerID string, term int64) (*pr.AppendEntriesRequest, error) {
	rm.mu.RLock()
	if rm.state != Leader || rm.election.currentTerm != term {
		rm.mu.RUnlock()
		return nil, fmt.Errorf("not leader / invalid term")
	}

	nextIndex := rm.replication.nextIndex[peerID]
	prevIndex := nextIndex - 1
	if int(prevIndex) >= len(rm.raftLog.log) {
		rm.mu.RUnlock()
		return nil, fmt.Errorf("error, previndex is bigger than log size")
	}
	prevTerm := rm.raftLog.log[prevIndex].Term
	var entries []*pb.LogEntry

	if nextIndex < int64(len(rm.raftLog.log)) {
		entries = rm.raftLog.log[nextIndex:]
	}

	request := &pr.AppendEntriesRequest{
		Term:         rm.election.currentTerm,
		LeaderID:     rm.id,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: rm.replication.commitIndex,
	}
	rm.mu.RUnlock()
	return request, nil
}

func (rm *RaftModule) sendAppendEntries(peerID string, term int64) error {
	request, err := rm.prepareAppendEntriesRequest(peerID, term)
	if err != nil {
		log.Println("error preparing entries: ", err)
		return err
	}

	response, err := rm.server.sendAppendEntriesRequest(peerID, request)
	if err != nil {
		// log.Println("error appending entries: ", err)
		return err
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rm.state != Leader || rm.election.currentTerm != term {
		return fmt.Errorf("not leader / invalid term")
	}

	if response.Term > rm.election.currentTerm {
		rm.becomeFollower(response.Term)
		return nil
	}
	if response.Success {
		rm.replication.nextIndex[peerID] = response.Index + 1
		rm.replication.matchIndex[peerID] = response.Index
		rm.updateCommitIndex()
	} else {
		if rm.replication.nextIndex[peerID] > 1 {
			rm.replication.nextIndex[peerID]--
		}
	}

	return nil
}

func (rm *RaftModule) updateCommitIndex() {
	if rm.state != Leader {
		return
	}

	for i := rm.replication.commitIndex + 1; i < int64(len(rm.raftLog.log)); i++ {
		if rm.raftLog.log[i].Term != rm.election.currentTerm {
			continue
		}

		count := 1

		for peerID := range rm.peers {
			if rm.replication.matchIndex[peerID] >= i {
				count++
			}
		}
		if count > len(rm.peers)/2 {
			rm.replication.commitIndex = i
		}
	}
}
