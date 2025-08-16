package main

import (
	"fmt"
	"time"

	"github.com/alexandrecolauto/gofka/pkg/broker"
	"github.com/alexandrecolauto/gofka/pkg/log"
)

func main() {
	log, err := log.NewLog("data/topic-0")
	defer log.Close()
	if err != nil {
		panic(err)
	}
	headers := map[string][]byte{
		"header-key": []byte("header-val"),
	}
	msg := &broker.Message{
		Key:       "user-121",
		Value:     "order-data",
		Headers:   headers,
		Topic:     "msg-topic",
		Partition: 2,
	}

	offset, err := log.Append(msg)
	if err != nil {
		panic(err)
	}
	offset, err = log.Append(msg)
	if err != nil {
		panic(err)
	}
	offset, err = log.Append(msg)
	if err != nil {
		panic(err)
	}
	fmt.Println("new offset", offset)
	time.Sleep(1 * time.Second)

	res, err := log.Read(offset)
	if err != nil {
		panic(err)
	}
	fmt.Printf("message %+v\n", res)
}

//
// import (
// 	"encoding/binary"
// 	"fmt"
// 	"io"
// 	"os"
// 	"path/filepath"
// )
//
// type LogSegment struct {
// 	path       string
// 	logFile    *os.File
// 	indexFile  *os.File
// 	baseOffset uint64
// 	nextOffset uint64
// }
//
// const indexEntryWidth = 16
//
// const recordHeaderSize = 4
//
// func NewSegment(dir string, baseOffset uint64) (*LogSegment, error) {
// 	s := LogSegment{
// 		path:       dir,
// 		baseOffset: baseOffset,
// 		nextOffset: baseOffset,
// 	}
// 	if err := os.MkdirAll(dir, 0755); err != nil {
// 		return nil, err
// 	}
//
// 	logPath := filepath.Join(dir, fmt.Sprintf("%d.log", baseOffset))
// 	logFile, err := os.OpenFile(logPath, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
// 	if err != nil {
// 		return nil, err
// 	}
// 	s.logFile = logFile
//
// 	indexPath := filepath.Join(dir, fmt.Sprintf("%d.index", baseOffset))
// 	indexFile, err := os.OpenFile(indexPath, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
// 	if err != nil {
// 		return nil, err
// 	}
// 	s.indexFile = indexFile
//
// 	stat, _ := s.indexFile.Stat()
//
// 	if stat.Size() > 0 {
// 		lastEntry := make([]byte, indexEntryWidth)
// 		_, err := s.indexFile.ReadAt(lastEntry, stat.Size()-indexEntryWidth)
// 		if err != nil && err != io.EOF {
// 			return nil, err
// 		}
// 		lastOffset := binary.BigEndian.Uint64(lastEntry[:8])
// 		s.nextOffset = lastOffset
// 	}
//
// 	return &s, nil
//
// }
//
// func (s *LogSegment) Close() error {
// 	if err := s.logFile.Close(); err != nil {
// 		return err
// 	}
// 	if err := s.indexFile.Close(); err != nil {
// 		return err
// 	}
// 	return nil
// }
//
// func (s *LogSegment) Append(message []byte) (uint64, error) {
// 	pos, err := s.logFile.Seek(0, io.SeekEnd)
// 	if err != nil {
// 		return 0, err
// 	}
// 	header := make([]byte, recordHeaderSize)
// 	binary.BigEndian.PutUint32(header, uint32(len(message)))
//
// 	if _, err := s.logFile.Write(header); err != nil {
// 		return 0, err
// 	}
//
// 	if _, err := s.logFile.Write(message); err != nil {
// 		return 0, err
// 	}
//
// 	offset := s.nextOffset
// 	indexEntry := make([]byte, indexEntryWidth)
// 	binary.BigEndian.PutUint64(indexEntry[0:8], offset)
// 	binary.BigEndian.PutUint64(indexEntry[8:16], uint64(pos))
//
// 	if _, err := s.indexFile.Write(indexEntry); err != nil {
// 		return 0, err
// 	}
//
// 	s.nextOffset++
//
// 	return offset, nil
// }
//
// func (s *LogSegment) Read(offset uint64) ([]byte, error) {
// 	var pos int64 = -1
// 	indexScanner := make([]byte, indexEntryWidth)
//
// 	s.indexFile.Seek(0, io.SeekStart)
//
// 	for {
// 		_, err := s.indexFile.Read(indexScanner)
// 		if err != nil {
// 			return nil, err
// 		}
// 		entyOffset := binary.BigEndian.Uint64(indexScanner[:8])
// 		if entyOffset == offset {
// 			pos = int64(binary.BigEndian.Uint64(indexScanner[8:16]))
// 			break
// 		}
// 	}
//
// 	if pos == -1 {
// 		return nil, fmt.Errorf("coulnt find offset: %d", offset)
// 	}
//
// 	if _, err := s.logFile.Seek(pos, io.SeekStart); err != nil {
// 		return nil, err
// 	}
// 	header := make([]byte, recordHeaderSize)
//
// 	if _, err := s.logFile.Read(header); err != nil {
// 		return nil, err
// 	}
//
// 	messageSize := binary.BigEndian.Uint32(header)
//
// 	message := make([]byte, messageSize)
// 	if _, err := s.logFile.Read(message); err != nil {
// 		return nil, err
// 	}
// 	return message, nil
// }
//
// func main() {
// 	dir, err := os.MkdirTemp("", "kafka-go-segment")
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer os.RemoveAll(dir)
// 	fmt.Printf("using segment dir: %s\n", dir)
//
// 	seg, err := NewSegment(dir, 0)
// 	if err != nil {
// 		panic(err)
// 	}
//
// 	msg1 := []byte("hello world")
// 	off1, err := seg.Append(msg1)
// 	if err != nil {
// 		panic(err)
// 	}
//
// 	fmt.Printf("Appended '%s' at offset %d\n", msg1, off1)
//
// 	msg2 := []byte("this is kafka")
// 	off2, err := seg.Append(msg2)
// 	if err != nil {
// 		panic(err)
// 	}
// 	fmt.Printf("Appended '%s' at offset %d\n", msg2, off2)
//
// 	msg3 := []byte("in go")
// 	off3, err := seg.Append(msg3)
// 	if err != nil {
// 		panic(err)
// 	}
// 	fmt.Printf("Appended '%s' at offset %d\n", msg3, off3)
//
// 	seg.Close()
//
// 	fmt.Println("Reading phase")
// 	r_seg, err := NewSegment(dir, 0)
//
// 	if err != nil {
// 		panic(err)
// 	}
//
// 	defer r_seg.Close()
// 	fmt.Println("Reading msg at offset 2")
//
// 	readMsg, err := r_seg.Read(off2)
// 	if err != nil {
// 		panic(err)
// 	}
//
// 	fmt.Printf("Read message: '%s'\n", readMsg)
// 	fmt.Printf("Next offset will be: %d\n", r_seg.nextOffset)
//
// }

// import (
// 	"fmt"
// 	"log"
// 	"time"
//
// 	broker "github.com/alexandrecolauto/gofka/pkg/broker"
// 	"github.com/alexandrecolauto/gofka/pkg/consumer"
// 	"github.com/alexandrecolauto/gofka/pkg/producer"
// )
//
// func consume(n int, cons *consumer.Consumer) {
// 	for i := range n {
// 		log.Println("consumer - pooling", i)
// 		msgs := cons.Poll(time.Second * 1)
//
// 		for _, msg := range msgs {
// 			log.Println("consumer - Msg: ", msg.Value)
// 		}
// 	}
// }
//
// func send(n int, prod *producer.Producer) {
// 	for i := range n {
// 		log.Println("producer - Sent message: ", i)
// 		prod.SendMessage("key", fmt.Sprintf("message_%d", i))
// 		time.Sleep(time.Millisecond * 1500)
// 	}
// }
//
// func main() {
// 	topic := "foo_topic"
// 	gfk := broker.NewGofka()
// 	prod := producer.NewProducer(topic, gfk)
// 	cons := consumer.NewConsumer("bar_group", gfk)
// 	cons.Subscribe(topic)
// 	go consume(2, cons)
// 	go send(4, prod)
// 	time.Sleep(time.Second * 2)
// 	log.Println("Creating new consumer")
// 	cons = consumer.NewConsumer("bar_group", gfk)
// 	cons.Subscribe(topic)
// 	go consume(4, cons)
// 	time.Sleep(time.Second * 10)
//
// }
