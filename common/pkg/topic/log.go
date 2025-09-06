package topic

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alexandrecolauto/gofka/common/proto/broker"
)

type Log struct {
	dir      string
	segments []*LogSegment
	active   *LogSegment
	mu       sync.RWMutex

	segmentBytes   int64
	indexInteral   int32
	retentionBytes int64
	retentionTime  time.Duration
	stopChan       chan any
	shutdownOnce   sync.Once
	isShutdown     bool
}

type ReadOpts struct {
	MaxMessages int32
	MaxBytes    int32
	MinBytes    int32
}

func NewLog(path string, shutdownCh chan any) (*Log, error) {
	dir := "data/" + path

	if err := os.MkdirAll(dir, 0755); err != nil {
		fmt.Println(" error creatiing directory: ", err)
		return nil, err
	}

	log := &Log{
		dir:            dir,
		indexInteral:   4096,
		segmentBytes:   1024 * 1024,
		retentionBytes: 100 * 1024 * 1024,
		retentionTime:  7 * 24 * time.Hour,
		stopChan:       shutdownCh,
	}

	if err := log.loadSegments(); err != nil {
		fmt.Println(" error loading segments: ", err)
		return nil, err
	}
	if len(log.segments) == 0 {
		if err := log.newSegment(0); err != nil {
			fmt.Println(" error creating segments: ", err)
			return nil, err
		}
	}

	go log.cleanupLog()
	return log, nil
}

func (l *Log) newSegment(baseOffset int64) error {
	segment, err := NewLogSegment(l.dir, baseOffset, int(l.indexInteral))
	if err != nil {
		return err
	}
	l.segments = append(l.segments, segment)
	l.active = segment
	return nil
}

func (l *Log) loadSegments() error {
	files, err := os.ReadDir(l.dir)
	if err != nil {
		return err
	}

	var baseOffsets []int64
	offsetMap := make(map[int64]bool)

	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".log") {
			offsetStr := strings.TrimSuffix(file.Name(), ".log")
			offset, err := strconv.ParseInt(offsetStr, 10, 64)
			if err != nil {
				continue
			}
			if !offsetMap[offset] {
				baseOffsets = append(baseOffsets, offset)
				offsetMap[offset] = true
			}
		}
	}

	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	for _, offset := range baseOffsets {
		segment, err := loadLogSegments(l.dir, offset, int(l.indexInteral))
		if err != nil {
			return err
		}
		l.segments = append(l.segments, segment)
	}
	if len(l.segments) > 0 {
		l.active = l.segments[len(l.segments)-1]
	}

	return nil
}

// func (l *Log) Append(message *broker.Message) (int64, error) {
// 	l.mu.Lock()
// 	defer l.mu.Unlock()
//
// 	if l.active.size >= l.segmentBytes {
// 		nextOffset := l.active.baseOffset + l.active.size
// 		if err := l.newSegment(nextOffset); err != nil {
// 			return 0, err
// 		}
// 	}
//
// 	return l.active.append(message)
// }

func (l *Log) AppendBatch(batch []*broker.Message) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.active.size >= l.segmentBytes {
		if err := l.rollToNewSegment(); err != nil {
			return 0, err
		}
	}

	l.active.AppendBatch(batch)
	return l.active.nextOffset - 1, nil
}

func (l *Log) rollToNewSegment() error {
	var nextBaseOffset int64

	if l.active != nil {
		// Calculate next base offset correctly
		nextBaseOffset = l.active.baseOffset + l.active.Count()

		// Close current active segment
		if err := l.active.Close(); err != nil {
			return fmt.Errorf("failed to close active segment: %w", err)
		}

	} else {
		nextBaseOffset = 0
	}

	// Create new active segment
	err := l.newSegment(nextBaseOffset)
	if err != nil {
		return err
	}

	return nil
}

func (l *Log) ReadBatch(offset int64, opt *broker.ReadOptions) ([]*broker.Message, error) {
	segment := l.findSegment(offset)
	if segment == nil {
		return nil, fmt.Errorf("segment not found for offset: %d\n", offset)
	}
	currentSeg := segment
	totalBytes := int32(0)
	msgCount := int32(0)
	currentOffset := offset
	result_msgs := make([]*broker.Message, 0)
	if opt.MaxMessages == 0 {
		opt.MaxMessages = 100
	}
	if opt.MinBytes == 0 {
		opt.MinBytes = opt.MaxBytes
	}

	for currentSeg != nil && msgCount < int32(opt.MaxMessages) && totalBytes < opt.MaxBytes {
		messages, nextOffset, bytesRead, err := currentSeg.readBatch(currentOffset, opt.MaxMessages-msgCount, opt.MaxBytes-totalBytes)
		if err != nil {
			if err == io.EOF {
				break
			}
			if len(messages) > 0 {
				break
			}
			return nil, err
		}

		result_msgs = append(result_msgs, messages...)
		totalBytes += bytesRead
		msgCount += int32(len(messages))
		currentOffset = nextOffset

		if currentOffset >= currentSeg.nextOffset {
			currentSeg = l.nextSegment(currentSeg)
		}
		if totalBytes >= opt.MinBytes && msgCount > 0 {
			break
		}
	}

	return result_msgs, nil
}

func (l *Log) findSegment(offset int64) *LogSegment {
	for i := len(l.segments) - 1; i >= 0; i-- {
		if offset >= l.segments[i].baseOffset {
			return l.segments[i]
		}
	}
	return nil
}

func (l *Log) nextSegment(cur_seg *LogSegment) *LogSegment {
	if cur_seg == nil {
		return nil
	}

	for i, seg := range l.segments {
		if seg == cur_seg && i+1 < len(l.segments) {
			return l.segments[i+1]
		}
	}
	return nil
}

func (l *Log) Size() int64 {
	return l.active.nextOffset
}

func (l *Log) cleanupLog() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if l.isShutdown {
				return
			}
			l.removeOld()
			l.truncate()
		case <-l.stopChan:
			return
		}
	}
}

func (l *Log) truncate() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.isShutdown {
		return
	}
	var totalSize int64
	for _, s := range l.segments {
		totalSize += s.size
	}

	if totalSize <= l.retentionBytes {
		return
	}

	sizeToRemove := totalSize - l.retentionBytes
	fmt.Printf("Log size %d exceeded retention %d. Need to remove %d bytes.\n", totalSize, l.retentionBytes, sizeToRemove)

	newSegments := make([]*LogSegment, len(l.segments))
	copy(newSegments, l.segments)
	for len(newSegments) > 1 {
		if sizeToRemove <= 0 {
			break
		}
		segmentToRemove := newSegments[0]

		fmt.Printf("Removing the oldest segment with base offest %d and size %d \n", segmentToRemove.baseOffset, segmentToRemove.size)
		if err := segmentToRemove.Remove(); err != nil {
			fmt.Printf("Error removing  segment  %d %s \n", segmentToRemove.baseOffset, err)
			break
		}

		sizeToRemove -= segmentToRemove.size
		newSegments = newSegments[1:]
	}

	l.segments = newSegments
}

func (l *Log) removeOld() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.isShutdown {
		return
	}
	hasOld := false
	for _, s := range l.segments {
		if time.Since(s.lastModified) > l.retentionTime {
			hasOld = true
			break
		}
	}
	if !hasOld {
		return
	}
	newSegments := make([]*LogSegment, len(l.segments))
	for _, s := range l.segments {
		if time.Since(s.lastModified) > l.retentionTime {
			fmt.Printf("TOO OLD! -Removing  oldest segment with base offest %v and size %v time since %d\n", s.lastModified, l.retentionTime, time.Since(s.lastModified))
			if err := s.Remove(); err != nil {
				fmt.Printf("Error removing  segment  %d %s \n", s.baseOffset, err)
				break
			}
		} else {
			newSegments = append(newSegments, s)
		}
	}
	if len(newSegments) != len(l.segments) {
		l.segments = newSegments
	}
}

func (l *Log) Shutdown() error {
	var shutErr error
	l.shutdownOnce.Do(func() {
		if l.isShutdown {
			return
		}
		l.isShutdown = true

		for _, seg := range l.segments {
			err := seg.Close()
			if err != nil {
				if shutErr == nil {
					shutErr = err
				}
			}
		}
	})
	return shutErr
}
