package log

type Message struct {
	Key       string
	Value     string
	Timestamp int64
	Topic     string
	Partition int
	Offset    int64
	Headers   map[string][]byte
}

func NewMessage(key, value string) *Message {
	hd := make(map[string][]byte)
	return &Message{Key: key, Value: value, Headers: hd}
}

func NewMessageWithMetadata(key, value, topic string, partition int, offset int64) *Message {
	return &Message{Key: key, Value: value, Topic: topic, Partition: partition, Offset: offset}
}

func (m *Message) UpdateMetadata(topic string, partition int) {
	m.Topic = topic
	m.Partition = partition
}

func (m *Message) UpdateOffset(offset int64) {
	m.Offset = offset
}
