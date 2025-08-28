package broker

import (
	"fmt"
	"hash/fnv"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/alexandrecolauto/gofka/model"
	"github.com/alexandrecolauto/gofka/pkg/log"
	"github.com/alexandrecolauto/gofka/proto/broker"
	pb "github.com/alexandrecolauto/gofka/proto/controller"
)

type Gofka struct {
	topics         map[string]*Topic
	consumer_group map[string]*ConsumerGroup

	pendingTopics map[string]chan any

	Metadata      *model.ClusterMetadata
	MetadataIndex int64

	RplManager *ReplicaManager

	createTopicFun func(string, int, int) error

	mu sync.RWMutex
}

type ReadOpts struct {
	log.ReadOpts
}

func NewReadOpts(maxMessages, maxBytes, minBytes int32) *ReadOpts {
	return &ReadOpts{
		log.ReadOpts{
			MaxMessages: maxMessages,
			MaxBytes:    maxBytes,
			MinBytes:    minBytes,
		},
	}
}

func (r *ReadOpts) ToOpt() *log.ReadOpts {
	return &log.ReadOpts{
		MaxBytes:    r.MaxBytes,
		MaxMessages: r.MaxMessages,
		MinBytes:    r.MinBytes,
	}
}

func NewGofka(brokerID string, cli BrokerClient) *Gofka {
	rm := NewReplicaManager(brokerID, cli)
	mt := model.NewClusterMetadata()
	topics := make(map[string]*Topic)
	pendingTopics := make(map[string]chan any)
	consumers := make(map[string]*ConsumerGroup)

	g := Gofka{topics: topics, consumer_group: consumers, RplManager: rm, Metadata: mt, pendingTopics: pendingTopics}
	g.ScanDisk()
	g.startSessionMonitor()
	return &g
}

func (g *Gofka) ScanDisk() error {
	files, err := os.ReadDir("data")
	if err != nil {
		return err
	}
	for _, file := range files {
		if file.IsDir() {
			topic := file.Name()
			if topic == "__cluster_metadata" {
				continue
			}
			topicDir, _ := os.ReadDir("data/" + topic)
			n_parts := 0
			for _, file := range topicDir {
				if file.IsDir() {
					n_parts++
				}
			}
			g.createTopic(topic, n_parts)
			cmd := pb.Command_CreateTopic{
				CreateTopic: &pb.CreateTopicCommand{
					Topic:       topic,
					NPartitions: int32(n_parts),
				},
			}
			g.Metadata.CreateTopic(cmd)
		}
	}
	return nil
}
func (g *Gofka) SendMessage(topic, key, value string) error {
	fmt.Println("Sending msg")
	t, err := g.GetOrCreateTopic(topic)
	if err != nil {
		return err
	}
	hd := make(map[string][]byte)
	message := &broker.Message{
		Key:     key,
		Value:   value,
		Headers: hd,
		Topic:   topic,
	}
	return t.Append(message)
}
func (g *Gofka) SendMessageBatch(topic string, partition int, batch []*broker.Message) error {
	t, err := g.GetTopic(topic)
	if err != nil {
		return err
	}
	return t.AppendBatch(partition, batch)
}

func (g *Gofka) RegisterConsumer(id, group_id string, topics []string) *broker.RegisterConsumerResponse {
	// g.mu.Lock()
	// defer g.mu.Unlock()
	fmt.Println("Start registering consumer", id)
	cg := g.GetOrCreateConsumerGroup(group_id)
	doneCh := make(chan any)
	go cg.ResetConsumerGroup(doneCh)
	cg.AddConsumer(id, topics)
	<-doneCh
	res := cg.GetRegisterResponse()
	fmt.Println("Finished registering consumer", id)
	return res
}

func (g *Gofka) GroupCoordinator(group_id string) (string, string, error) {
	brokers_map := g.Metadata.Brokers()
	n_brokers := len(brokers_map)
	if n_brokers == 0 {
		return "", "", fmt.Errorf("no brokers found")
	}
	hasher := fnv.New32a()
	hasher.Write([]byte(group_id))
	index := int(hasher.Sum32()) % n_brokers
	brokers := make([]*broker.BrokerInfo, 0, n_brokers)
	for _, brk := range brokers_map {
		brokers = append(brokers, brk)
	}
	sort.Slice(brokers, func(i, j int) bool {
		return brokers[i].Id < brokers[j].Id
	})
	if index >= n_brokers {
		return "", "", fmt.Errorf("invalid index")
	}

	id := brokers[index].Id
	address := brokers[index].Address
	return address, id, nil
}

func (g *Gofka) Subscribe(topic, group_id string) error {
	t, err := g.GetTopic(topic)
	if err != nil {
		fmt.Println("subscribe topic err: ", err)
		return err
	}
	cg := g.GetOrCreateConsumerGroup(group_id)
	cg.Subscribe(t)
	return nil
}

func (g *Gofka) Unsubscribe(topic, group_id string) {
	cg := g.GetOrCreateConsumerGroup(group_id)
	cg.Unsubscribe(topic)
}

func (g *Gofka) GetOrCreateConsumerGroup(group_id string) *ConsumerGroup {
	cg, ok := g.consumer_group[group_id]
	if ok {
		return cg
	}
	cg = NewConsumerGroup(group_id)
	g.consumer_group[group_id] = cg
	return cg
}

func (g *Gofka) GetTopic(topic string) (*Topic, error) {
	t, ok := g.topics[topic]
	if !ok {
		return nil, fmt.Errorf("cannot find topic %s", topic)
	}
	return t, nil

}

func (g *Gofka) GetOrCreateTopic(topic string) (*Topic, error) {
	panic("not used")
	t, ok := g.topics[topic]
	if ok {
		return t, nil
	}
	if ch, ok := g.pendingTopics[topic]; ok {
		<-ch
		return g.topics[topic], nil
	}

	done := make(chan any)
	g.pendingTopics[topic] = done

	fmt.Println("creating new topic at the controller")
	go g.createTopicFun(topic, 1, 1)

	fmt.Println("waiting for topic to be created")
	<-done
	fmt.Println("done, proceeding")

	delete(g.pendingTopics, topic)
	return g.topics[topic], nil
}

func (g *Gofka) FetchMessages(id, group_id string, opt *broker.ReadOptions) ([]*broker.Message, error) {
	cg := g.GetOrCreateConsumerGroup(group_id)
	return cg.FetchMessages(id, opt)
}

func (g *Gofka) TopicsMetadata(topics []string) ([]*broker.TopicInfo, error) {
	all_topics := g.Metadata.Topics()
	if len(all_topics) == 0 {
		return nil, fmt.Errorf("there is no topic to look for")
	}
	res := make([]*broker.TopicInfo, 0)
	for _, topic := range topics {
		t, ok := all_topics[topic]
		if !ok {
			return nil, fmt.Errorf("cannot find metadata for %s", topic)
		}
		res = append(res, t)
	}

	return res, nil
}

func (g *Gofka) SyncGroup(id, group_id string, consumers []*broker.ConsumerSession) (*broker.ConsumerSession, error) {
	cg := g.GetOrCreateConsumerGroup(group_id)
	if cg.leaderId == id {
		cg.SyncGroup(consumers)
	}
	return cg.UserAssignment(id, 0)
}
func (g *Gofka) FetchMessagesReplica(topic string, partitionID int, offset int64, opt *broker.ReadOptions) ([]*broker.Message, error) {
	t, err := g.GetTopic(topic)
	if err != nil {
		return nil, fmt.Errorf("cannot find topic: %s - %w", topic, err)
	}

	if partitionID >= t.n_partitions {
		return nil, fmt.Errorf("cannot find partition: %d", partitionID)
	}

	msgs, err := t.ReadFromPartitionReplica(partitionID, int(offset), opt)
	if err != nil {
		return nil, fmt.Errorf("error reading from replica: %w", err)
	}

	return msgs, nil
}

func (g *Gofka) UpdateFollowerState(topic, followerID string, partitionID int, fetchOffset, longEndOffset int64) error {
	t, err := g.GetTopic(topic)
	if err != nil {
		return err
	}
	err = t.UpdateFollowerState(followerID, partitionID, fetchOffset, longEndOffset)
	return err
}

func (g *Gofka) CommitOffset(group_id, topic string, partition, offset int) {
	cg := g.GetOrCreateConsumerGroup(group_id)
	cg.UpdateOffset(topic, partition, offset)
}

func (g *Gofka) ConsumerHandleHeartbeat(id, group_id string) {
	cg := g.GetOrCreateConsumerGroup(group_id)
	cg.ConsumerHeartbeat(id)
}

func (g *Gofka) UnregisterConsumer(id, group_id string) {
	cg := g.GetOrCreateConsumerGroup(group_id)
	cg.UnregisterConsumer(id)
}

func (g *Gofka) startSessionMonitor() {
	ticker := time.NewTicker(10 * time.Second)
	go func() {
		for range ticker.C {
			g.cleanupDeadSession()
		}
	}()
}

func (g *Gofka) cleanupDeadSession() {
	for _, con_group := range g.consumer_group {
		con_group.ClearDeadSession()
	}
}

func (g *Gofka) DeleteTopic(topic_name string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.topics[topic_name]; !ok {
		return
	}
	for _, cg := range g.consumer_group {
		for name := range cg.topics {
			if name == topic_name {
				cg.DeleteTopic(topic_name)
			}
		}
	}
	delete(g.topics, topic_name)
}

func (g *Gofka) ChangeTopic(topic_name string, partitions int) {
	t, ok := g.topics[topic_name]
	if !ok {
		fmt.Println("cant find topic")
		return
	}
	t.AddPartitions(partitions)
}

// Controller actions
func (g *Gofka) ProcessControllerLogs(logs []*pb.LogEntry) {
	for _, log := range logs {
		if log.Index > g.MetadataIndex {
			g.MetadataIndex = log.Index
		}
		if log.Command != nil {
			if log.Command.Type == pb.Command_CHANGE_PARTITION_LEADER {
				fmt.Println("Recevide new log in brokre server: ", log)
			}
			g.Metadata.DecodeLog(log, g)
		}
	}
}

func (g *Gofka) ApplyRegisterBroker(ctc *pb.Command_RegisterBroker) {
	if g.RplManager.brokerID == ctc.RegisterBroker.Id {
		return
	}
	brk := model.BrokerInfo{
		ID:       ctc.RegisterBroker.Id,
		Address:  ctc.RegisterBroker.Address,
		Alive:    ctc.RegisterBroker.Alive,
		LastSeen: ctc.RegisterBroker.LastSeen.AsTime(),
	}
	g.RplManager.client.UpdateBroker(brk)
}

func (g *Gofka) ApplyUpdateBroker(ctc *pb.Command_UpdateBroker) {
	if g.RplManager.brokerID == ctc.UpdateBroker.Id {
		return
	}
	brk := model.BrokerInfo{
		ID:       ctc.UpdateBroker.Id,
		Address:  ctc.UpdateBroker.Address,
		Alive:    ctc.UpdateBroker.Alive,
		LastSeen: ctc.UpdateBroker.LastSeen.AsTime(),
	}
	g.RplManager.client.UpdateBroker(brk)
}

func (g *Gofka) ApplyCreateTopic(cmd *pb.Command_CreateTopic) {
	fmt.Printf("Applying create topic: %+v\n", cmd.CreateTopic)
	g.createTopic(cmd.CreateTopic.Topic, int(cmd.CreateTopic.NPartitions))

	if ch, exists := g.pendingTopics[cmd.CreateTopic.Topic]; exists {
		fmt.Println("closing pending ch")
		close(ch)
	}
	fmt.Println("Creatign new topic: ", cmd)
}

func (g *Gofka) createTopic(name string, n_parts int) {
	t, err := NewTopic(name, n_parts)
	if err != nil {
		fmt.Println("error creating topic: ", err)
		return
	}
	g.topics[name] = t
	// g.ChangeTopic(cmd.CreateTopic.Topic, int(cmd.CreateTopic.NPartitions))
	for _, part := range t.partitions {
		g.RplManager.AddPartition(name, part)
	}
}

func (g *Gofka) ApplyUpdatePartitionLeader(ctc *pb.Command_ChangePartitionLeader) {
	fmt.Println("Updating partition leader")
	for _, asgn := range ctc.ChangePartitionLeader.Assignments {
		for _, replica := range asgn.NewReplicas {
			if replica == g.RplManager.brokerID {
				g.RplManager.HandleLeaderChange(asgn.TopicId, int(asgn.PartitionId), asgn.NewLeader, int64(asgn.NewEpoch))
			}
		}
	}

}
