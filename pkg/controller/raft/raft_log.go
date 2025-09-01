package raft

import (
	"fmt"

	pb "github.com/alexandrecolauto/gofka/proto/controller"
	pr "github.com/alexandrecolauto/gofka/proto/raft"
)

type RaftLog struct {
	log     []*pb.LogEntry
	applyCh chan *pb.LogEntry
}

func (rm *RaftModule) ProcessAppendRequest(req *pr.AppendEntriesRequest) *pr.AppendEntriesResponse {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	response := &pr.AppendEntriesResponse{Term: rm.election.currentTerm, Success: false}
	if req.Term < rm.election.currentTerm {
		return response
	}

	if req.Term > rm.election.currentTerm {
		rm.becomeFollower(req.Term)
	}
	rm.resetElectionTimer()
	rm.election.leaderID = req.LeaderID

	if req.PrevLogIndex >= 0 {
		if req.PrevLogIndex >= int64(len(rm.raftLog.log)) {
			response.Term = rm.election.currentTerm
			return response
		}

		if rm.raftLog.log[req.PrevLogIndex].Term != req.PrevLogTerm {
			response.Term = rm.election.currentTerm
			return response
		}
	}

	if len(req.Entries) > 0 {
		insertIndex := req.PrevLogIndex + 1

		for i, newEntry := range req.Entries {
			logIndex := insertIndex + int64(i)

			if logIndex < int64(len(rm.raftLog.log)) {
				if rm.raftLog.log[logIndex].Term != newEntry.Term {
					rm.raftLog.log = rm.raftLog.log[:logIndex]
					rm.raftLog.log = append(rm.raftLog.log, req.Entries[i:]...)
					break
				}
			} else {
				rm.raftLog.log = append(rm.raftLog.log, req.Entries[i:]...)
				break
			}
		}
	}

	if req.LeaderCommit > rm.replication.commitIndex {
		rm.replication.commitIndex = min(req.LeaderCommit, int64(len(rm.raftLog.log)-1))
	}

	response.Success = true
	response.Index = int64(len(rm.raftLog.log) - 1)
	response.Term = rm.election.currentTerm
	return response
}

func (rm *RaftModule) SubmitCommand(command *pb.Command) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rm.state != Leader {
		return fmt.Errorf("not the leader")
	}

	entry := &pb.LogEntry{
		Term:    rm.election.currentTerm,
		Index:   int64(len(rm.raftLog.log)),
		Command: command,
	}

	rm.raftLog.log = append(rm.raftLog.log, entry)
	return nil
}

func (rm *RaftModule) InitLog(command *pb.Command) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	entry := &pb.LogEntry{
		Term:    rm.election.currentTerm,
		Index:   int64(len(rm.raftLog.log)),
		Command: command,
	}

	rm.raftLog.log = append(rm.raftLog.log, entry)
	return nil
}
func (rm *RaftModule) IsLeader() bool {
	return rm.state == Leader
}
func (rm *RaftModule) Leader() string {
	return rm.election.leaderID
}

func (rm *RaftModule) GetAddress(id string) (string, bool) {
	a, ok := rm.peers[id]
	return a, ok
}

func (rm *RaftModule) LogFromIndex(index int64) ([]*pb.LogEntry, error) {
	if index > int64(len(rm.raftLog.log)) {
		return nil, fmt.Errorf("invalid index: %d %+v", index, rm.raftLog.log)
	}
	if index == int64(len(rm.raftLog.log)) {
		return nil, nil
	}
	res := make([]*pb.LogEntry, int64(len(rm.raftLog.log))-index)
	copy(res, rm.raftLog.log[index:])
	return res, nil
}
