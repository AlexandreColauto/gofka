package log

import (
	"bytes"
	"fmt"
	"log"
	"testing"
	"time"
)

func TestBatch(t *testing.T) {
	msg1 := NewMessage("foo", "bar")
	msg2 := NewMessage("foo", "bar")

	ls := &LogSegment{nextOffset: 5, maxBatchSize: 4}
	ls.appendToBatch(msg1)
	time.Sleep(155 * time.Millisecond)
	ls.appendToBatch(msg2)
	ls.currentBatch.LastOffsetDelta = int32(len(ls.currentBatch.Records) - 1)
	data, err := ls.serializeBatch(ls.currentBatch)
	if err != nil {
		log.Fatal(err)
	}
	r := bytes.NewReader(data)
	b, err := ls.deseralizeBatch(r)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("msg: %+v\n", b)
}
