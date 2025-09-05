package raft

import (
	"fmt"
	"math/rand"
	"time"
)

type Timers struct {
	electionTimer  *time.Timer
	heartbeatTimer *time.Ticker

	testTimeout time.Duration
}

func (rm *RaftModule) resetElectionTimer() {
	timeout := time.Duration(BASE_ELECTION_TIME+rand.Intn(250)) * time.Millisecond
	if rm.timers.testTimeout != 0 {
		timeout = rm.timers.testTimeout
	}
	if rm.timers.electionTimer == nil {
		rm.timers.electionTimer = time.NewTimer(timeout)
	} else {
		rm.timers.electionTimer.Reset(timeout)
	}
}

func (rm *RaftModule) runElectionTimer() {
	for {
		select {
		case <-rm.timers.electionTimer.C:
			rm.runElection()
		case <-rm.server.shutdownCh:
			return
		}
	}
}

func (rm *RaftModule) runHeartbeatTimer() {
	defer rm.timers.heartbeatTimer.Stop()
	for range rm.timers.heartbeatTimer.C {
		rm.mu.RLock()
		if rm.state != Leader {
			rm.mu.RUnlock()
			return
		}

		term := rm.election.currentTerm
		rm.mu.RUnlock()
		for peerID := range rm.peers {
			go rm.sendAppendEntries(peerID, term)
		}
		if rm.visualizerClient != nil {
			action := "log_append"
			target := rm.id
			msg := fmt.Sprintf(`{"term": %d, "leo": %d, "last_applied": %d}`, rm.election.currentTerm, rm.replication.commitIndex, rm.replication.lastApplied)
			rm.visualizerClient.SendMessage(action, target, []byte(msg))
		}
	}
}
