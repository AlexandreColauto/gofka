package log

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"time"

	"github.com/alexandrecolauto/gofka/proto/broker"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type RecordBatch struct {
	BaseOffset           int64
	BatchLength          int32
	PartitionLeaderEpoch int32
	Magic                int8
	CRC                  int32
	Attributes           int16
	LastOffsetDelta      int32
	BaseTimestamp        int64
	MaxTimestamp         int64
	ProducerID           int64
	ProducerEpoch        int16
	BaseSequence         int32
	Records              []*broker.Message
}

func NewBatch(nextOffset int64, maxBatchSize int, message *broker.Message) *RecordBatch {
	return &RecordBatch{
		BaseOffset:    nextOffset,
		BaseTimestamp: message.Timestamp.AsTime().UnixMilli(),
		MaxTimestamp:  message.Timestamp.AsTime().UnixMilli(),
		Magic:         2,
		Records:       make([]*broker.Message, 0, maxBatchSize),
	}
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

func (ls *LogSegment) handleBatchTimeout() {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	if ls.currentBatch != nil && len(ls.currentBatch.Records) > 0 {
		err := ls.flushCurrentBatch()
		if err != nil {
			fmt.Printf("Error fushing batch, %w", err)
		}
	}

	ls.timeoutActive = false
}

func (ls *LogSegment) stopBatchTimeout() {
	if ls.batchTimer != nil {
		ls.batchTimer.Stop()
		ls.batchTimer = nil
	}

	ls.timeoutActive = false
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

	const pageSize = 4096
	if position/pageSize != newSize/pageSize {
		if err := ls.addIndexEntry(ls.currentBatch.BaseOffset, position); err != nil {
			return fmt.Errorf("Failed to add index entry: %w\n", err)
		}

		// Add time index entry using base timestamp
		if err := ls.addTimeIndexEntry(ls.currentBatch.BaseTimestamp, ls.currentBatch.BaseOffset); err != nil {
			return fmt.Errorf("failed to add time index entry: %w", err)
		}
	}

	ls.size = newSize

	ls.currentBatch = nil

	return nil
}

func (ls *LogSegment) serializeBatch(batch *RecordBatch) ([]byte, error) {
	buf := make([]byte, 0, 4096)

	buf = binary.BigEndian.AppendUint64(buf, uint64(batch.BaseOffset))

	batchLenPos := len(buf)
	buf = append(buf, 0, 0, 0, 0)

	buf = binary.BigEndian.AppendUint32(buf, uint32(batch.PartitionLeaderEpoch))

	buf = append(buf, byte(batch.Magic))

	crcPos := len(buf)
	buf = append(buf, 0, 0, 0, 0)

	buf = binary.BigEndian.AppendUint16(buf, uint16(batch.Attributes))

	buf = binary.BigEndian.AppendUint32(buf, uint32(batch.LastOffsetDelta))

	buf = binary.BigEndian.AppendUint64(buf, uint64(batch.BaseTimestamp))

	buf = binary.BigEndian.AppendUint64(buf, uint64(batch.MaxTimestamp))

	buf = binary.BigEndian.AppendUint64(buf, uint64(batch.ProducerID))

	buf = binary.BigEndian.AppendUint16(buf, uint16(batch.ProducerEpoch))

	buf = binary.BigEndian.AppendUint32(buf, uint32(batch.BaseSequence))

	buf = binary.BigEndian.AppendUint32(buf, uint32(len(batch.Records)))

	for i, message := range batch.Records {
		recordData, err := ls.serializeMessageInBatch(message, batch.BaseTimestamp, int32(i))
		if err != nil {
			return nil, fmt.Errorf("failed to serialize message")
		}
		buf = append(buf, recordData...)
	}

	batchLen := len(buf) - 12

	binary.BigEndian.PutUint32(buf[batchLenPos:], uint32(batchLen))

	crc := crc32.ChecksumIEEE(buf[crcPos+4:])
	binary.BigEndian.PutUint32(buf[crcPos:], crc)

	return buf, nil
}

func (ls *LogSegment) deseralizeBatch(r io.Reader) (*RecordBatch, int32, error) {
	var baseOffset uint64
	if err := binary.Read(r, binary.BigEndian, &baseOffset); err != nil {
		fmt.Println("error: ", err)
		return nil, 0, err
	}

	var length int32
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		fmt.Println("error: ", err)
		return nil, 0, err
	}

	data := make([]byte, length)

	if _, err := io.ReadFull(r, data); err != nil {
		return nil, 0, err
	}

	partitionLeaderE := binary.BigEndian.Uint32(data)

	offset := 4
	if offset >= len(data) {
		return nil, 0, fmt.Errorf("mesage too short: missing magic byte")
	}

	magicByte := data[offset]

	offset++

	if magicByte != 2 {
		return nil, 0, fmt.Errorf("Wrong magic byte %d", magicByte)
	}

	if offset >= len(data) {
		return nil, 0, fmt.Errorf("mesage too short: missing magic byte")
	}
	crc := binary.BigEndian.Uint32(data[offset : offset+4])
	expected := crc32.ChecksumIEEE(data[offset+4:])

	offset += 4

	if crc != expected {
		return nil, 0, fmt.Errorf("CRC mismatch")
	}

	reader := bytes.NewReader(data[offset:])

	var attrs uint16

	if err := binary.Read(reader, binary.BigEndian, &attrs); err != nil {
		return nil, 0, err
	}

	var lastOffset uint32

	if err := binary.Read(reader, binary.BigEndian, &lastOffset); err != nil {
		return nil, 0, err
	}

	var baseTimestamp uint64

	if err := binary.Read(reader, binary.BigEndian, &baseTimestamp); err != nil {
		return nil, 0, err
	}

	var maxTimestamp uint64

	if err := binary.Read(reader, binary.BigEndian, &maxTimestamp); err != nil {
		return nil, 0, err
	}

	var producerId uint64

	if err := binary.Read(reader, binary.BigEndian, &producerId); err != nil {
		return nil, 0, err
	}

	var producerEpoch uint16

	if err := binary.Read(reader, binary.BigEndian, &producerEpoch); err != nil {
		return nil, 0, err
	}

	var baseSequence uint32

	if err := binary.Read(reader, binary.BigEndian, &baseSequence); err != nil {
		return nil, 0, err
	}

	var lenRecords uint32

	if err := binary.Read(reader, binary.BigEndian, &lenRecords); err != nil {
		return nil, 0, err
	}

	batchRecords := make([]*broker.Message, lenRecords)
	for i := range lenRecords {
		record, err := ls.deserializeMessageInBatch(reader, baseTimestamp, baseOffset)
		if err != nil {
			return nil, 0, fmt.Errorf("cannot deserialize message: %w\n", err)
		}
		batchRecords[i] = record
	}

	batch := &RecordBatch{
		BaseOffset:           int64(baseOffset),
		BaseTimestamp:        int64(baseTimestamp),
		MaxTimestamp:         int64(maxTimestamp),
		PartitionLeaderEpoch: int32(partitionLeaderE),
		Magic:                int8(magicByte),
		CRC:                  int32(crc),
		Attributes:           int16(attrs),
		LastOffsetDelta:      int32(lastOffset),
		ProducerID:           int64(producerId),
		ProducerEpoch:        int16(producerEpoch),
		BaseSequence:         int32(baseSequence),
		BatchLength:          int32(len(batchRecords)),
		Records:              batchRecords,
	}
	return batch, length, nil
}

func (ls *LogSegment) deserializeMessageInBatch(r io.Reader, baseTimestamp uint64, baseOffset uint64) (*broker.Message, error) {
	var length uint32

	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return nil, err
	}

	data := make([]byte, length)

	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}

	offset := 0

	timestampDelta, bytesRead, err := readVarInt(data[offset:])
	if err != nil {
		return nil, fmt.Errorf("Error reading msg timestamp: %w\n", err)
	}

	offset += bytesRead

	offsetDelta, bytesRead, err := readVarInt(data[offset:])
	if err != nil {
		return nil, fmt.Errorf("Error reading msg timestamp: %w\n", err)
	}

	offset += bytesRead

	msg := &broker.Message{
		Headers:   make(map[string][]byte),
		Offset:    int64(baseOffset) + offsetDelta,
		Timestamp: timestamppb.New(time.Unix(0, (int64(baseTimestamp)+timestampDelta)*int64(time.Millisecond))),
	}

	keyLength, bytesRead, err := readVarInt(data[offset:])
	if err != nil {
		return nil, fmt.Errorf("failed to read key length: %w", err)
	}

	offset += bytesRead

	if keyLength == -1 {
		msg.Key = ""
	} else if keyLength == 0 {
		msg.Key = ""
	} else {
		if offset+int(keyLength) > len(data) {
			return nil, fmt.Errorf("message too short: key extends beyond message")
		}

		key := make([]byte, keyLength)
		copy(key, data[offset:offset+int(keyLength)])
		msg.Key = string(key)
		offset += int(keyLength)
	}

	valueLength, bytesRead, err := readVarInt(data[offset:])
	if err != nil {
		return nil, fmt.Errorf("failed to read value length: %w", err)
	}

	offset += bytesRead

	if valueLength == -1 {
		msg.Value = ""
	} else if valueLength == 0 {
		msg.Value = ""
	} else {
		if offset+int(valueLength) > len(data) {
			return nil, fmt.Errorf("message too short: value extends beyond message")
		}

		value := make([]byte, valueLength)
		copy(value, data[offset:offset+int(valueLength)])
		msg.Value = string(value)
		offset += int(valueLength)
	}

	headersCount, bytesRead, err := readVarInt(data[offset:])

	if err != nil {
		return nil, fmt.Errorf("failed to read headers count: %w", err)
	}
	offset += bytesRead

	for range headersCount {
		keyLen, bytesRead, err := readVarInt(data[offset:])
		if err != nil {
			return nil, fmt.Errorf("failed to read header key: %w", err)
		}
		offset += bytesRead

		if offset+int(keyLen) > len(data) {
			return nil, fmt.Errorf("message too short: header key extends beyond message")
		}
		headerKey := string(data[offset : offset+int(keyLen)])
		offset += int(keyLen)

		valueLength, bytesRead, err := readVarInt(data[offset:])
		if err != nil {
			return nil, fmt.Errorf("failed to read header value: %w", err)
		}
		offset += bytesRead
		var headerValue []byte
		if valueLength == -1 {
			headerValue = nil
		} else {
			if offset+int(valueLength) > len(data) {
				return nil, fmt.Errorf("message too short: header key extends beyond message")
			}
			headerValue = make([]byte, valueLength)

			copy(headerValue, data[offset:offset+int(valueLength)])
			offset += int(valueLength)
		}

		msg.Headers[headerKey] = headerValue
	}

	return msg, nil
}

func (ls *LogSegment) serializeMessageInBatch(message *broker.Message, baseTimestamp int64, offsetDelta int32) ([]byte, error) {
	buf := make([]byte, 0, 512)

	lenPos := len(buf)
	buf = append(buf, 0, 0, 0, 0)

	timeStampDelta := message.Timestamp.AsTime().UnixMilli() - baseTimestamp

	buf = appendVarInt(buf, timeStampDelta)

	buf = appendVarInt(buf, int64(offsetDelta))

	if message.Key == "" {
		buf = appendVarInt(buf, -1)
	} else {
		buf = appendVarInt(buf, int64(len(message.Key)))
		buf = append(buf, message.Key...)
	}

	if message.Value == "" {
		buf = appendVarInt(buf, -1)
	} else {
		buf = appendVarInt(buf, int64(len(message.Value)))
		buf = append(buf, message.Value...)
	}

	buf = appendVarInt(buf, int64(len(message.Headers)))
	for key, value := range message.Headers {
		buf = appendVarInt(buf, int64(len(key)))
		buf = append(buf, key...)

		buf = appendVarInt(buf, int64(len(value)))
		buf = append(buf, value...)
	}

	recordLen := len(buf) - 4
	binary.BigEndian.PutUint32(buf[lenPos:], uint32(recordLen))
	return buf, nil
}
