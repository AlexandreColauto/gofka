package gofka

import "time"

type Message struct {
	Key       string
	Value     string
	Timestamp time.Time
}

func NewMessage(key, value string) *Message {
	return &Message{Key: key, Value: value, Timestamp: time.Now()}
}
