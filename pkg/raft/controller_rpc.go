package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

// --- Raft RPC Senders ---

func (c *RaftController) HandleVoteRequest(w http.ResponseWriter, r *http.Request) {
	var req VoteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp := c.Raft.processVoteRequest(req)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (c *RaftController) HandleAppendEntries(w http.ResponseWriter, r *http.Request) {
	var req AppendEntriesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp := c.Raft.processAppendRequest(req)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (c *RaftController) sendVoteRequest(address string, req *VoteRequest) (*VoteResponse, error) {
	jsonValue, _ := json.Marshal(req)
	url := fmt.Sprintf("http://%s/raft/vote", address)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonValue))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var voteResp VoteResponse
	if err := json.NewDecoder(resp.Body).Decode(&voteResp); err != nil {
		return nil, err
	}
	return &voteResp, nil
}

func (c *RaftController) sendAppendEntriesRequest(address string, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	jsonValue, _ := json.Marshal(req)
	url := fmt.Sprintf("http://%s/raft/append", address)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonValue))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var appendResp AppendEntriesResponse
	if err := json.NewDecoder(resp.Body).Decode(&appendResp); err != nil {
		return nil, err
	}
	return &appendResp, nil
}
