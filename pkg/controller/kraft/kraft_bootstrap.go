package kraft

import (
	"fmt"
	"sort"

	"github.com/alexandrecolauto/gofka/pkg/topic"
	pb "github.com/alexandrecolauto/gofka/proto/broker"
	pc "github.com/alexandrecolauto/gofka/proto/controller"
	"google.golang.org/protobuf/proto"
)

func createMetadataTopic(nodeID string) (*topic.Topic, error) {
	logTopic, err := topic.NewTopic(fmt.Sprintf("__cluster_metadata/%s", nodeID), 1)
	if err != nil {
		return nil, err
	}

	p, err := logTopic.GetPartition(0)
	if err != nil {
		return nil, err
	}

	p.BecomeLeader(nodeID, 1)
	return logTopic, nil
}

func (c *KraftController) readFromDisk() error {
	p, err := c.metadataLog.GetPartition(0)
	if err != nil {
		return err
	}
	opt := &pb.ReadOptions{
		MaxMessages: 1000,
		MaxBytes:    10024 * 1024,
		MinBytes:    10024 * 1024,
	}
	msgs, err := p.ReadFromReplica(0, opt)
	if err != nil {
		return err
	}
	if len(msgs) == 0 {
		fmt.Println("No logs found on disk, starting fresh")
		return nil
	}

	var recoveredLogs []*pc.LogEntry
	var maxTerm int64
	var maxIndex int64
	for _, msg := range msgs {
		val := msg.Value
		var log pc.LogEntry
		err := proto.Unmarshal([]byte(val), &log)
		if err != nil {
			panic(err)
		}
		if log.Term > maxTerm {
			maxTerm = log.Term
		}
		if log.Index > maxIndex {
			maxIndex = log.Index
		}

		recoveredLogs = append(recoveredLogs, &log)

	}

	sort.Slice(recoveredLogs, func(i, j int) bool {
		return recoveredLogs[i].Index < recoveredLogs[j].Index
	})

	if err := c.validateLogContinuity(recoveredLogs); err != nil {
		return fmt.Errorf("log validation failed: %w", err)
	}
	c.raftModule.SetCurrentTerm(maxTerm)
	c.raftModule.SetLastApplied(0)        // Will be updated as we apply logs)
	c.raftModule.SetCommitIndex(maxIndex) // Assume all recovered logs are committed
	c.raftModule.SetVotedFor("")          // Reset vote on recovery

	for _, log := range recoveredLogs {
		c.raftModule.AppendLog(log)

		if err := c.applyLogEntry(log); err != nil {
			return fmt.Errorf("failed to apply log entry %d: %w", log.Index, err)
		}

		c.raftModule.SetLastApplied(log.Index)
	}

	fmt.Printf("Recovery complete: %d entries, term=%d, lastApplied=%d\n",
		len(recoveredLogs), maxTerm, maxIndex)
	return nil
}

func (c *KraftController) validateLogContinuity(logs []*pc.LogEntry) error {
	if len(logs) == 0 {
		return nil
	}
	expectedIndex := logs[0].Index
	for i, log := range logs {
		if log.Index != expectedIndex {
			return fmt.Errorf("log gap detected: expected index %d, got %d at position %d",
				expectedIndex, log.Index, i)
		}
		expectedIndex++
	}

	return nil
}

func (c *KraftController) applyLogEntry(log *pc.LogEntry) error {
	c.clusterMetadata.DecodeLog(log, c)
	return nil
}
