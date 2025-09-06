package broker

import (
	"fmt"
	"hash/fnv"
	"sort"
	"time"

	"github.com/alexandrecolauto/gofka/common/proto/broker"
)

type BrokerConsumerGroups struct {
	groups          map[string]*ConsumerGroup
	joiningDuration time.Duration
}

func NewBrokerConsumerGroup(joiningDuration time.Duration) BrokerConsumerGroups {
	g := make(map[string]*ConsumerGroup)
	return BrokerConsumerGroups{groups: g, joiningDuration: joiningDuration}
}

func (g *GofkaBroker) GroupCoordinator(group_id string) (string, string, error) {
	brokers_map := g.clusterMetadata.metadata.Brokers()
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

func (g *GofkaBroker) GetOrCreateConsumerGroup(group_id string) *ConsumerGroup {
	g.mu.Lock()
	cg, ok := g.consumerGroups.groups[group_id]
	g.mu.Unlock()
	if ok {
		return cg
	}
	cg = NewConsumerGroup(group_id, g.consumerGroups.joiningDuration)
	g.mu.Lock()
	g.consumerGroups.groups[group_id] = cg
	g.mu.Unlock()
	return cg
}

func (g *GofkaBroker) SyncGroup(id, group_id string, consumers []*broker.ConsumerSession) (*broker.ConsumerSession, error) {
	cg := g.GetOrCreateConsumerGroup(group_id)
	if cg.leaderId == id {
		fmt.Println("leader assigning")
		cg.SyncGroup(consumers)
	}
	return cg.UserAssignment(id, 0)
}

func (g *GofkaBroker) ConsumerHandleHeartbeat(id, group_id string) {
	cg := g.GetOrCreateConsumerGroup(group_id)
	cg.ConsumerHeartbeat(id)
}

func (g *GofkaBroker) startSessionMonitor() {
	ticker := time.NewTicker(10 * time.Second)
	defer g.wg.Done()
	for {
		select {
		case <-ticker.C:
			if g.isShutdown {
				return
			}
			g.cleanupDeadSession()
		case <-g.shutdownCh:
			ticker.Stop()
			return
		}
	}
}

func (g *GofkaBroker) cleanupDeadSession() {
	for _, con_group := range g.consumerGroups.groups {
		con_group.ClearDeadSession()
	}
}
