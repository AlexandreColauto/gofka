package config

import "time"

type BrokerConfig struct {
	ControllerAddress string        `yaml:"controller_address" mapstructure:"controller_address"`
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval" mapstructure:"heartbeat_interval"`
	MetadataInterval  time.Duration `yaml:"metadata_interval" mapstructure:"metadata_interval"`
	MaxLagTimeout     time.Duration `yaml:"max_lag_timeout" mapstructure:"max_lag_timeout"`
	Replica           Replica       `yaml:"replica" mapstructure:"replica"`
	Log               Log           `yaml:"log" mapstructure:"log"`
	ConsumerGroup     ConsumerGroup `yaml:"consumer_group" mapstructure:"consumer_group"`
}

type Log struct {
	BatchTimeout   time.Duration `yaml:"batch_timeout" mapstructure:"batch_timeout"`
	MaxBatchMsg    int           `yaml:"max_batch_msg" mapstructure:"max_batch_msg"`
	SegmentBytes   int64         `yaml:"segment_bytes" mapstructure:"segment_bytes"`
	IndexInterval  int32         `yaml:"index_interval" mapstructure:"index_interval"`
	RetentionBytes int64         `yaml:"retention_bytes" mapstructure:"retention_bytes"`
	RetentionTime  time.Duration `yaml:"retention_time" mapstructure:"retention_time"`
}
type Replica struct {
	FetchInterval time.Duration `yaml:"fetch_interval" mapstructure:"fetch_interval"`
}

type ConsumerGroup struct {
	JoiningDuration time.Duration `yaml:"joining_duration" mapstructure:"joining_duration"`
}
