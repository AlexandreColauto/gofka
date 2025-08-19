package model

import "time"

type LogEntry struct {
	Term    int64
	Index   int64
	Command *Command
}

type VoteRequest struct {
	Term         int64
	CadidateID   string
	LastLogIndex int64
	LastLogTerm  int64
}
type VoteResponse struct {
	Term int64
	Vote bool
}

type AppendEntriesRequest struct {
	Term         int64
	LeaderID     string
	PrevLogIndex int64
	PrevLogTerm  int64
	Entries      []LogEntry
	LeaderCommit int64
}

type AppendEntriesResponse struct {
	Term    int64
	Success bool
	Index   int64
}

type RegisterBrokerRequest struct {
	ID      string
	Address string
}

type CreateTopicRequest struct {
	Topic             string
	NPartition        int
	ReplicationFactor int
}

type BrokerInfo struct {
	ID       string
	Address  string
	Alive    bool
	LastSeen time.Time
}
