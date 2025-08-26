package log

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/alexandrecolauto/gofka/proto/broker"
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

	batchTimer    *time.Timer
	stopTimeout   chan any
	timeoutActive bool

	mu sync.RWMutex
}

func NewLogSegment(dir string, baseOffset int64) (*LogSegment, error) {
	filename := fmt.Sprintf("%020d", baseOffset)

	logPath := filepath.Join(dir, filename+".log")
	indexPath := filepath.Join(dir, filename+".index")
	timePath := filepath.Join(dir, filename+".timeindex")

	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	indexFile, err := os.OpenFile(indexPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		logFile.Close()
		return nil, err
	}

	timeIndexFile, err := os.OpenFile(timePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		logFile.Close()
		indexFile.Close()
		return nil, err
	}

	info, err := logFile.Stat()
	if err != nil {
		return nil, err
	}

	return &LogSegment{
		baseOffset:    baseOffset,
		nextOffset:    baseOffset,
		logFile:       logFile,
		indexFile:     indexFile,
		timeIndex:     timeIndexFile,
		size:          info.Size(),
		writer:        bufio.NewWriter(logFile),
		batchTimeout:  400 * time.Millisecond,
		maxBatchSize:  100,
		stopTimeout:   make(chan any),
		timeoutActive: false,
	}, nil
}

func loadLogSegments(dir string, offset int64) (*LogSegment, error) {
	segment, err := NewLogSegment(dir, offset)
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

		// Set nextOffset to be one more than the highest offset found
		segment.nextOffset = highestOffset + 1

	}

	return segment, nil
}

func (ls *LogSegment) append(message *broker.Message) (int64, error) {
	err := ls.appendToBatch(message)
	return message.Offset, err
}

func (ls *LogSegment) forceFlush() error {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	return ls.flushCurrentBatch()
}

func (ls *LogSegment) addIndexEntry(offset, position int64) error {
	entry := make([]byte, 16)

	binary.BigEndian.PutUint64(entry[:8], uint64(offset))
	binary.BigEndian.PutUint64(entry[8:], uint64(position))

	_, err := ls.indexFile.Write(entry)
	return err
}

// addTimeIndexEntry adds an entry to the time index
func (ls *LogSegment) addTimeIndexEntry(timestamp, offset int64) error {
	entry := make([]byte, 16)
	binary.BigEndian.PutUint64(entry[:8], uint64(timestamp))
	binary.BigEndian.PutUint64(entry[8:], uint64(offset))
	_, err := ls.timeIndex.Write(entry)
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

	fmt.Println("USING LOG FILE - os new file - readBatch")
	// fileReader := os.NewFile(ls.logFile.Fd(), "log")

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
	i, e := ls.logFile.Stat()
	fmt.Println("Final log stat: ", i, e)

	fmt.Println("STOP USING LOG FILE - os new file - readBatch")
	return messages, currentOffset, bytesRead, nil
}

func (ls *LogSegment) read(offset int64) (*broker.Message, error) {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	if offset < ls.baseOffset || offset >= ls.nextOffset {
		return nil, fmt.Errorf("offset %d not in segment range [%d, %d)",
			offset, ls.baseOffset, ls.nextOffset)
	}

	starting_position, err := ls.findPosition(offset)
	fmt.Println("Starting position", starting_position)
	if err != nil {
		return nil, err
	}

	fmt.Println("USING LOG FILE- os new file - read")
	// fileReader := os.NewFile(ls.logFile.Fd(), "log")

	if _, err := ls.logFile.Seek(starting_position, 0); err != nil {
		return nil, fmt.Errorf("failed to seek log file to position %d: %w", starting_position, err)
	}

	reader := bufio.NewReader(ls.logFile)

	for {
		batch, _, err := ls.deseralizeBatch(reader)

		if err != nil {
			fmt.Println("STOP USING LOG FILE- os new file - read")
			return nil, fmt.Errorf("failed to deserialize :%w", err)
		}

		for _, record := range batch.Records {
			if record.Offset == offset {
				fmt.Println("STOP USING LOG FILE- os new file - read")
				return record, nil
			}
		}

	}
}

func (s *LogSegment) findPosition(offset int64) (int64, error) {

	info, err := s.indexFile.Stat()
	if err != nil {
		return 0, err
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

func (s *LogSegment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.stopBatchTimeout()

	if s.writer != nil {
		s.writer.Flush()
	}

	var errs []error
	if s.logFile != nil {
		fmt.Println("closing file")
		if err := s.logFile.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.indexFile != nil {
		if err := s.indexFile.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.timeIndex != nil {
		if err := s.timeIndex.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errs[0] // Return first error
	}
	return nil
}

func (ls *LogSegment) findHighestOffset() (int64, error) {
	if ls.size == 0 {
		return ls.baseOffset - 1, nil
	}

	if _, err := ls.logFile.Seek(0, 0); err != nil {
		return 0, err
	}

	fmt.Println("USING LOG FILE - bufio - find HighestOffset")
	reader := bufio.NewReader(ls.logFile)
	highestOffset := ls.baseOffset - 1

	for {
		batch, _, err := ls.deseralizeBatch(reader)
		if err != nil {
			if err == io.EOF {
				break // End of file reached
			}
			fmt.Println("STOP USING LOG FILE - bufio - find HighestOffset")
			return 0, fmt.Errorf("error reading message during scan: %w", err)
		}

		if int64(batch.LastOffsetDelta)+batch.BaseOffset > highestOffset {
			highestOffset = int64(batch.LastOffsetDelta) + batch.BaseOffset
		}
	}

	fmt.Println("STOP USING LOG FILE - bufio - find HighestOffset")
	return highestOffset, nil
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

	// The last indexed offset might not be the actual last message
	// We need to scan from the last indexed position to find the true last message
	return ls.scanFromPosition(lastIndexedPosition, lastIndexedOffset)
}

// scanFromPosition scans from a specific file position to find the highest offset
func (ls *LogSegment) scanFromPosition(startPosition, minOffset int64) (int64, error) {
	// Seek to the starting position
	fmt.Println("USING LOG FILE - seek - scanFromposition")
	if _, err := ls.logFile.Seek(startPosition, 0); err != nil {
		return 0, fmt.Errorf("failed to seek to position %d: %w", startPosition, err)
	}

	fmt.Println("USING LOG FILE - bufio - scanFromPosition")
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
