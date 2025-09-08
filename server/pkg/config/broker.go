package config

import "time"

type BrokerConfig struct {
	ControllerAddress string        `yaml:"controller_address" mapstructure:"controller_address"`
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval" mapstructure:"heartbeat_interval"`
	MetadataInterval  time.Duration `yaml:"metadata_interval" mapstructure:"metadata_interval"`
	MaxLagTimeout     time.Duration `yaml:"max_lag_timeout" mapstructure:"max_lag_timeout"`
	Replica           Replica       `yaml:"replica" mapstructure:"replica"`
	ConsumerGroup     ConsumerGroup `yaml:"consumer_group" mapstructure:"consumer_group"`
}

type Replica struct {
	FetchInterval time.Duration `yaml:"fetch_interval" mapstructure:"fetch_interval"`
}

type ConsumerGroup struct {
	JoiningDuration time.Duration `yaml:"joining_duration" mapstructure:"joining_duration"`
}
