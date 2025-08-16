package util

import "fmt"

func TopicPartitionToOffsetKey(topic string, partition int) string {
	return fmt.Sprintf("%s_%d", topic, partition)
}
