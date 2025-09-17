package topic

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/alexandrecolauto/gofka/common/proto/broker"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type LogSegment struct {
	baseOffset   int64
	nextOffset   int64
	logFile      *os.File
	indexFile    *os.File
	timeIndex    *os.File
	writer       *bufio.Writer
	size         int64
	currentBatch *RecordBatch
	batchTimeout time.Duration
	maxBatchSize int
	pageInterval int

	dir string

	batchTimer    *time.Timer
	stopTimeout   chan any
	timeoutActive bool
	lastModified  time.Time

	mu sync.RWMutex
}

func NewLogSegment(dir string, baseOffset int64, pageInterval int, batchTimeout time.Duration, maxBatchMsg int) (*LogSegment, error) {
	ls := &LogSegment{
		baseOffset:    baseOffset,
		nextOffset:    baseOffset + 1,
		dir:           dir,
		batchTimeout:  batchTimeout,
		maxBatchSize:  maxBatchMsg,
		lastModified:  time.Now(),
		pageInterval:  pageInterval,
		stopTimeout:   make(chan any),
		timeoutActive: false,
	}

	ls.OpenFiles()

	info, err := ls.logFile.Stat()
	if err != nil {
		return nil, err
	}

	ls.size = info.Size()
	ls.writer = bufio.NewWriter(ls.logFile)

	return ls, nil
}

func loadLogSegments(dir string, offset int64, pageInterval int, batchTimeout time.Duration, maxBatchMsg int) (*LogSegment, error) {
	segment, err := NewLogSegment(dir, offset, pageInterval, batchTimeout, maxBatchMsg)
	if err != nil {
		return nil, err
	}

	info, err := segment.logFile.Stat()
	if err != nil {
		return nil, err
	}
	segment.size = info.Size()
	segment.nextOffset = 1

	// If file has content, scan to find the next offset
	if segment.size > 0 {
		// Find the highest offset to set nextOffset correctly
		highestOffset, err := segment.findHighestOffsetFromIndex()
		if err != nil {
			fmt.Printf("Warning: failed to find highest offset, using fallback scan: %v\n", err)
			// Fallback to full scan
			highestOffset, err = segment.findHighestOffset()
			if err != nil {
				return nil, fmt.Errorf("failed to determine highest offset: %w", err)
			}
		}

		segment.nextOffset = highestOffset + 1

	}

	return segment, nil
}

func (ls *LogSegment) findHighestOffsetFromIndex() (int64, error) {
	info, err := ls.indexFile.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to stat index file: %w", err)
	}

	if info.Size() == 0 {
		// No index entries, file might be empty or we need to scan
		return ls.findHighestOffset()
	}

	// Each index entry is 16 bytes (8 bytes offset + 8 bytes position)
	numEntries := info.Size() / 16
	if numEntries == 0 {
		return ls.baseOffset - 1, nil
	}

	// Read the last index entry
	lastEntryPos := (numEntries - 1) * 16
	entry := make([]byte, 16)

	_, err = ls.indexFile.ReadAt(entry, lastEntryPos)
	if err != nil {
		return 0, fmt.Errorf("failed to read last index entry: %w", err)
	}

	lastIndexedOffset := int64(binary.BigEndian.Uint64(entry[:8]))
	lastIndexedPosition := int64(binary.BigEndian.Uint64(entry[8:]))

	return ls.scanFromPosition(lastIndexedPosition, lastIndexedOffset)
}

func (ls *LogSegment) findHighestOffset() (int64, error) {
	if ls.size == 0 {
		return ls.baseOffset - 1, nil
	}

	if _, err := ls.logFile.Seek(0, 0); err != nil {
		return 0, err
	}

	reader := bufio.NewReader(ls.logFile)
	highestOffset := ls.baseOffset - 1

	for {
		batch, _, err := ls.deseralizeBatch(reader)
		if err != nil {
			if err == io.EOF {
				break // End of file reached
			}
			return 0, fmt.Errorf("error reading message during scan: %w", err)
		}

		if int64(batch.LastOffsetDelta)+batch.BaseOffset > highestOffset {
			highestOffset = int64(batch.LastOffsetDelta) + batch.BaseOffset
		}
	}

	return highestOffset, nil
}

func appendVarInt(buf []byte, value int64) []byte {
	// Apply zigzag encoding for signed integers
	encoded := zigzagEncode(value)
	for encoded >= 0x80 {
		buf = append(buf, byte(encoded)|0x80)
		encoded >>= 7
	}
	buf = append(buf, byte(encoded))
	return buf
}

func readVarInt(buf []byte) (int64, int, error) {
	var value uint64
	var shift uint
	var bytesRead int

	for i, b := range buf {
		bytesRead = i + 1
		val7 := uint64(b & 0x7F)
		value |= val7 << shift

		if (b & 0x80) == 0 {
			// Apply zigzag decoding to get the actual signed value
			decoded := zigzagDecode(value)
			return decoded, bytesRead, nil
		}

		shift += 7
		if shift >= 64 {
			return 0, bytesRead, fmt.Errorf("varint is too long")
		}
	}
	return 0, bytesRead, fmt.Errorf("unterminated varint")
}

// Zigzag encoding: maps signed integers to unsigned integers
// Positive numbers: 0, 2, 4, 6, ...
// Negative numbers: 1, 3, 5, 7, ...
func zigzagEncode(value int64) uint64 {
	return uint64((value << 1) ^ (value >> 63))
}

// Zigzag decoding: converts unsigned integers back to signed
func zigzagDecode(value uint64) int64 {
	return int64((value >> 1) ^ (-(value & 1)))
}

func (ls *LogSegment) scanFromPosition(startPosition, minOffset int64) (int64, error) {
	if _, err := ls.logFile.Seek(startPosition, 0); err != nil {
		return 0, fmt.Errorf("failed to seek to position %d: %w", startPosition, err)
	}

	reader := bufio.NewReader(ls.logFile)
	highestOffset := minOffset

	for {
		batch, _, err := ls.deseralizeBatch(reader)
		if err != nil {
			if err == io.EOF {
				break // End of file reached
			}
			return 0, fmt.Errorf("error reading message during scan: %w", err)
		}

		if int64(batch.LastOffsetDelta)+batch.BaseOffset > highestOffset {
			highestOffset = int64(batch.LastOffsetDelta) + batch.BaseOffset
		}
	}

	return highestOffset, nil
}

func (ls *LogSegment) Count() int64 {
	return ls.nextOffset - ls.baseOffset - 1
}

func (ls *LogSegment) stopBatchTimeout() {
	if ls.batchTimer != nil {
		ls.batchTimer.Stop()
		ls.batchTimer = nil
	}

	ls.timeoutActive = false
}

func (ls *LogSegment) AppendBatch(batch []*broker.Message) error {
	if len(batch) == 0 {
		return nil
	}
	for _, msg := range batch {
		err := ls.appendToBatch(msg)
		if err != nil {
			return err
		}
	}
	ls.lastModified = time.Now()
	return nil
}

func (ls *LogSegment) appendToBatch(msg *broker.Message) error {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	if msg.Timestamp == nil {
		msg.Timestamp = timestamppb.New(time.Now())
	}

	if ls.currentBatch == nil {
		ls.currentBatch = NewBatch(ls.nextOffset, ls.maxBatchSize, msg)
	}

	msg.Offset = ls.nextOffset

	ls.currentBatch.Records = append(ls.currentBatch.Records, msg)

	if msg.Timestamp.AsTime().UnixMilli() > ls.currentBatch.MaxTimestamp {
		ls.currentBatch.MaxTimestamp = msg.Timestamp.AsTime().UnixMilli()
	}

	ls.nextOffset++

	if len(ls.currentBatch.Records) == 1 {
		ls.startBatchTimeout()
	}

	if len(ls.currentBatch.Records) >= ls.maxBatchSize {
		return ls.flushCurrentBatch()
	}
	return nil
}

func (ls *LogSegment) startBatchTimeout() {
	if ls.timeoutActive {
		return
	}

	ls.timeoutActive = true
	ls.batchTimer = time.AfterFunc(ls.batchTimeout, func() {
		ls.handleBatchTimeout()
	})
}

func (ls *LogSegment) flushCurrentBatch() error {
	if ls.currentBatch == nil || len(ls.currentBatch.Records) == 0 {
		return nil
	}
	ls.currentBatch.LastOffsetDelta = int32(len(ls.currentBatch.Records) - 1)

	ls.stopBatchTimeout()

	data, err := ls.serializeBatch(ls.currentBatch)
	if err != nil {
		return fmt.Errorf("Failed to serialize batch: %w\n", err)
	}
	position := ls.size

	n, err := ls.logFile.Write(data)
	if err != nil {
		return fmt.Errorf("Failed to write to file: %w\n", err)
	}
	newSize := ls.size + int64(n)

	pageSize := int64(ls.pageInterval)
	if position/pageSize != newSize/pageSize {
		fmt.Printf("Flushing batch. current page %d; new page %d \n", position/pageSize, newSize/pageSize)
		fmt.Printf(" current size %d; new size %d \n", position, newSize)

		if err := ls.addIndexEntry(ls.currentBatch.BaseOffset, position); err != nil {
			return fmt.Errorf("Failed to add index entry: %w\n", err)
		}

		// Add time index entry using base timestamp
		if err := ls.addTimeIndexEntry(ls.currentBatch.BaseTimestamp, ls.currentBatch.BaseOffset); err != nil {
			return fmt.Errorf("failed to add time index entry: %w", err)
		}
	}

	ls.size = newSize

	ls.currentBatch.Records = []*broker.Message{}
	ls.currentBatch = nil

	return nil
}

func (ls *LogSegment) handleBatchTimeout() {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	if ls.currentBatch != nil && len(ls.currentBatch.Records) > 0 {
		err := ls.flushCurrentBatch()
		if err != nil {
			fmt.Printf("Error fushing batch, %s", err)
		}
	}

	ls.timeoutActive = false
}

func (ls *LogSegment) addIndexEntry(offset, position int64) error {
	entry := make([]byte, 16)

	binary.BigEndian.PutUint64(entry[:8], uint64(offset))
	binary.BigEndian.PutUint64(entry[8:], uint64(position))

	_, err := ls.indexFile.Write(entry)
	fmt.Printf("Adding index entry: %d %d\n", offset, position)
	return err
}

func (ls *LogSegment) addTimeIndexEntry(timestamp, offset int64) error {
	entry := make([]byte, 16)
	binary.BigEndian.PutUint64(entry[:8], uint64(timestamp))
	binary.BigEndian.PutUint64(entry[8:], uint64(offset))
	_, err := ls.timeIndex.Write(entry)
	fmt.Printf("Adding time index entry: %d %d\n", offset, timestamp)
	return err
}

func (ls *LogSegment) readBatch(offset int64, maxMessages, maxBytes int32) ([]*broker.Message, int64, int32, error) {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	if offset < ls.baseOffset || offset >= ls.nextOffset {
		return nil, 0, 0, fmt.Errorf("offset %d not found in segment range [%d,%d)", offset, ls.baseOffset, ls.nextOffset)
	}

	startingPos, err := ls.findPosition(offset)
	if err != nil {
		return nil, 0, 0, err
	}

	if _, err := ls.logFile.Seek(startingPos, 0); err != nil {
		return nil, 0, 0, err
	}

	reader := bufio.NewReader(ls.logFile)
	messages := make([]*broker.Message, 0)
	bytesRead := int32(0)
	currentOffset := offset

	for int32(len(messages)) < int32(maxMessages) && bytesRead < maxBytes && currentOffset < ls.nextOffset {
		batch, bytesR, err := ls.deseralizeBatch(reader)
		if err != nil {
			return nil, 0, 0, err
		}

		for _, record := range batch.Records {
			if record.Offset >= offset {
				messages = append(messages, record)
				currentOffset = record.Offset + 1
				if len(messages) >= int(maxMessages) {
					break
				}
			}
		}

		bytesRead += bytesR

		if bytesRead >= maxBytes {
			break
		}

	}
	return messages, currentOffset, bytesRead, nil
}

func (s *LogSegment) findPosition(offset int64) (int64, error) {

	info, err := s.indexFile.Stat()
	if err != nil {
		s.OpenFiles()
		info, err = s.indexFile.Stat()
		if err != nil {
			return 0, err
		}
	}
	if info.Size() == 0 {
		return 0, nil
	}
	numEntries := info.Size() / 16
	low := int64(0)
	high := numEntries - 1

	var foundPosition int64 = 0 // Default to the start of the segment.

	for low <= high {
		mid := low + (high-low)/2
		entryPos := mid * 16

		entry := make([]byte, 16)
		_, err := s.indexFile.ReadAt(entry, entryPos)
		if err != nil {
			return 0, fmt.Errorf("failed to read index entry at position %d: %w", entryPos, err)
		}

		indexedOffset := int64(binary.BigEndian.Uint64(entry[:8]))
		logPosition := int64(binary.BigEndian.Uint64(entry[8:]))

		if indexedOffset == offset {
			return logPosition, nil
		} else if indexedOffset < offset {
			foundPosition = logPosition
			low = mid + 1
		} else {
			high = mid - 1
		}

	}

	return foundPosition, nil
}

func (ls *LogSegment) Remove() error {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	if err := ls.Close(); err != nil {
		fmt.Printf("Warning: failed to close segment before removing %v\n", err)
	}
	var errors []error
	if err := os.RemoveAll(ls.logFile.Name()); err != nil {
		errors = append(errors, err)
	}
	if err := os.RemoveAll(ls.indexFile.Name()); err != nil {
		errors = append(errors, err)
	}
	if err := os.RemoveAll(ls.timeIndex.Name()); err != nil {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		return fmt.Errorf("failed to remove segment files: %v\n", errors)
	}

	return nil
}
func (ls *LogSegment) OpenFiles() error {
	filename := fmt.Sprintf("%020d", ls.baseOffset)

	logPath := filepath.Join(ls.dir, filename+".log")
	indexPath := filepath.Join(ls.dir, filename+".index")
	timePath := filepath.Join(ls.dir, filename+".timeindex")

	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	indexFile, err := os.OpenFile(indexPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		logFile.Close()
		return err
	}

	timeIndexFile, err := os.OpenFile(timePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		logFile.Close()
		indexFile.Close()
		return err
	}
	ls.logFile = logFile
	ls.indexFile = indexFile
	ls.timeIndex = timeIndexFile
	return nil

}

func (ls *LogSegment) Close() error {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	ls.stopBatchTimeout()

	if ls.writer != nil {
		ls.writer.Flush()
	}

	var errs []error
	if ls.logFile != nil {
		if err := ls.logFile.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if ls.indexFile != nil {
		if err := ls.indexFile.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if ls.timeIndex != nil {
		if err := ls.timeIndex.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errs[0] // Return first error
	}
	return nil
}

func (ls *LogSegment) FinalOffset() int64 {
	return ls.nextOffset - 1
}
