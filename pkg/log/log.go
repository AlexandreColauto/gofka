package log

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Log struct {
	dir      string
	segments []*LogSegment
	active   *LogSegment
	mu       sync.RWMutex

	segmentBytes int64
	indexInteral int32
}

type ReadOpts struct {
	MaxMessages int32
	MaxBytes    int32
	MinBytes    int32
}

func NewLog(dir string) (*Log, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	log := &Log{
		dir:          dir,
		segmentBytes: 1024 * 1024 * 1024,
		indexInteral: 4096,
	}

	if err := log.loadSegments(); err != nil {
		return nil, err
	}
	if len(log.segments) == 0 {
		if err := log.newSegment(0); err != nil {
			return nil, err
		}
	}
	return log, nil
}

func (l *Log) Append(message *Message) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.active.size >= l.segmentBytes {
		nextOffset := l.active.baseOffset + l.active.size
		if err := l.newSegment(nextOffset); err != nil {
			return 0, err
		}
	}

	return l.active.append(message)
}
func (l *Log) ReadBatch(offset int64, opt *ReadOpts) ([]*Message, error) {
	segment := l.findSegment(offset)
	if segment == nil {
		return nil, fmt.Errorf("segment not found for offset: %d\n", offset)
	}
	currentSeg := segment
	totalBytes := int32(0)
	msgCount := int32(0)
	currentOffset := offset
	result_msgs := make([]*Message, 0)

	for currentSeg != nil && msgCount < int32(opt.MaxMessages) && totalBytes < opt.MaxBytes {
		messages, nextOffset, bytesRead, err := currentSeg.readBatch(currentOffset, opt.MaxMessages-msgCount, opt.MaxBytes-totalBytes)
		if err != nil {
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

func (l *Log) Read(offset int64) (*Message, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	segment := l.findSegment(offset)
	if segment == nil {
		return nil, fmt.Errorf("segment not found for offset: %d\n", offset)
	}
	return segment.read(offset)
}

func (l *Log) newSegment(baseOffset int64) error {
	segment, err := NewLogSegment(l.dir, baseOffset)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, segment)
	l.active = segment
	return nil
}

func (l *Log) findSegment(offset int64) *LogSegment {
	for i := len(l.segments) - 1; i >= 0; i-- {
		if offset >= l.segments[i].baseOffset {
			return l.segments[i]
		}
	}
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
		segment, err := loadLogSegments(l.dir, offset)
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

func (l *Log) Close() {
	l.active.Close()
}
func (l *Log) Size() int64 {
	return l.active.nextOffset
}
