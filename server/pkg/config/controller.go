package config

import "time"

type ServerConfig struct {
	NodeID         string        `yaml:"node_id" mapstructure:"node_id"`
	Address        string        `yaml:"address" mapstructure:"address"`
	Port           int           `yaml:"port" mapstructure:"port"`
	MaxRetries     int           `yaml:"max_retries" mapstructure:"max_retries"`
	InitialBackoff time.Duration `yaml:"inital_backoff" mapstructure:"inital_backoff"`
	Cluster        ClusterConfig `yaml:"cluster" mapstructure:"cluster"`
}

type ClusterConfig struct {
	Peers map[string]string `yaml:"peers" mapstructure:"peers"`
}

type KraftConfig struct {
	Timeout     time.Duration `yaml:"timeout" mapstructure:"timeout"`
	GracePeriod time.Duration `yaml:"grace_period" mapstructure:"grace_period"`
}
