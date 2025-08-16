package broker

import (
	"fmt"
	"path/filepath"
	"sync"

	"github.com/alexandrecolauto/gofka/pkg/log"
)

type Partition struct {
	id    int
	dir   string
	log   *log.Log
	mutex sync.RWMutex
}

func NewPartition(topicName string, id int) (*Partition, error) {
	partitionDir := filepath.Join(topicName, fmt.Sprintf("%d", id))
	l, err := log.NewLog(partitionDir)
	if err != nil {
		return nil, err
	}
	return &Partition{id: id, log: l, dir: partitionDir}, nil
}

func (p *Partition) ReadFrom(offset int64, opt *log.ReadOpts) ([]*log.Message, error) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.log.ReadBatch(offset, opt)
}

func (p *Partition) Append(message *log.Message) (int64, error) {
	if message == nil {
		return 0, fmt.Errorf("empty message")
	}
	p.mutex.Lock()
	defer p.mutex.Unlock()

	offset, err := p.log.Append(message)
	if err != nil {
		return 0, err
	}

	message.UpdateOffset(offset)
	return offset, nil
}
