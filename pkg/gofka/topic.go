package gofka

type Topic struct {
	name  string
	items []*Message
	size  int
}

func NewTopic(name string) *Topic {
	items := make([]*Message, 1024)
	return &Topic{name: name, items: items, size: 0}
}

func (t *Topic) Append(message *Message) {
	if message == nil {
		return
	}
	t.items[t.size] = message
	t.size++
}

func (t *Topic) Head() *Message {
	if t.size == 0 {
		return nil
	}
	return t.items[t.size-1]
}
func (t *Topic) ByIndex(index int) *Message {
	if t.size == 0 {
		return nil
	}
	return t.items[index]
}
func (t *Topic) ReadFrom(offset int, ch chan []*Message) {
	if offset >= t.size {
		return
	}
	items := t.items[offset:t.size]
	ch <- items
}
