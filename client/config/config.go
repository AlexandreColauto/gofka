package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	Producer ProducerConfig `yaml:"producer" mapstructure:"producer"`
	Consumer ConsumerConfig `yaml:"consumer" mapstructure:"consumer"`
}

type ProducerConfig struct {
	Enabled          bool             `yaml:"enabled" mapstructure:"enabled"`
	BootstrapAddress string           `yaml:"bootstrap_address" mapstructure:"bootstrap_address"`
	Topic            string           `yaml:"topic" mapstructure:"topic"`
	ACKS             string           `yaml:"acks" mapstructure:"acks"`
	Visualizer       VisualizerConfig `yaml:"visualizer" mapstructure:"visualizer"`
}

type ConsumerConfig struct {
	Enabled          bool             `yaml:"enabled" mapstructure:"enabled"`
	GroupID          string           `yaml:"group_id" mapstructure:"group_id"`
	BootstrapAddress string           `yaml:"bootstrap_address" mapstructure:"bootstrap_address"`
	Topics           []string         `yaml:"topics" mapstructure:"topics"`
	Visualizer       VisualizerConfig `yaml:"visualizer" mapstructure:"visualizer"`
}

type VisualizerConfig struct {
	Enabled bool   `yaml:"enabled" mapstructure:"enabled"`
	Address string `yaml:"address" mapstructure:"address"`
}

func LoadConfig(configPath string) (*Config, error) {
	env := os.Getenv("SERVER_NODE_ID")
	fmt.Println("NODE ID FROM ENV: ", env)
	viper.SetConfigType("yaml")
	viper.SetConfigFile(configPath)
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AllowEmptyEnv(true)

	viper.SetDefault("kraft.timeout", 5*time.Second)
	viper.SetDefault("kraft.graceperiod", 10*time.Second)

	viper.SetDefault("server.address", "localhost")
	viper.SetDefault("server.port", 9092)
	viper.SetDefault("server.max_retries", 5)
	viper.SetDefault("server.initial_backoff", 250*time.Millisecond)

	viper.SetDefault("broker.heartbeat_interval", 250*time.Millisecond)
	viper.SetDefault("broker.metadata_interval", 250*time.Millisecond)
	viper.SetDefault("broker.replica.fetch_interval", 750*time.Millisecond)
	viper.SetDefault("broker.consumer_group.joining_duration", 750*time.Millisecond)

	viper.SetDefault("kraft.timeout", 5*time.Second)
	viper.SetDefault("kraft.grace_period", 10*time.Second)

	viper.SetDefault("visualizer.enabled", false)

	// Read the config file. Errors here are okay if we don't need a file.
	if err := viper.ReadInConfig(); err != nil {
		fmt.Println("found error: ", err)
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}
	}

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unable to decode into struct: %w", err)
	}

	if err := validate(&cfg); err != nil {
		return nil, fmt.Errorf("configuartion validation failed: %v", err)
	}

	return &cfg, nil
}

// validate validates the configuration
func validate(config *Config) error {
	fmt.Println("validating: ", config)

	return nil
}
