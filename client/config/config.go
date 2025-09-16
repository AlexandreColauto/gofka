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
	AutoCreateTopics bool             `yaml:"auto_create_topics" mapstructure:"auto_create_topics"`
	ACKS             string           `yaml:"acks" mapstructure:"acks"`
	MaxMsg           int              `yaml:"max_msg" mapstructure:"max_msg"`
	BatchTimeout     time.Duration    `yaml:"batch_timeout" mapstructure:"batch_timeout"`
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

func NewProducerConfig() *ProducerConfig {
	return &ProducerConfig{
		Enabled:    true,
		ACKS:       "1",
		Visualizer: VisualizerConfig{Enabled: false},
	}
}
func NewConsumerConfig() *ConsumerConfig {
	return &ConsumerConfig{
		Enabled:    true,
		Visualizer: VisualizerConfig{Enabled: false},
	}
}
func LoadConfig(configPath string) (*Config, error) {
	env := os.Getenv("SERVER_NODE_ID")
	fmt.Println("NODE ID FROM ENV: ", env)
	viper.SetConfigType("yaml")
	viper.SetConfigFile(configPath)
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AllowEmptyEnv(true)

	viper.SetDefault("producer.bootstrap_address", "localhost:42169")
	viper.SetDefault("producer.acks", "1")
	viper.SetDefault("producer.max_msg", "10")
	viper.SetDefault("producer.batch_timeout", "250ms")
	viper.SetDefault("consumer.bootstrap_address", "localhost:42169")

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
