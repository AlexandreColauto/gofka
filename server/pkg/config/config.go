package config

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	Server     ServerConfig     `yaml:"server" mapstructure:"server"`
	Kraft      KraftConfig      `yaml:"kraft" mapstructure:"kraft"`
	Broker     BrokerConfig     `yaml:"broker" mapstructure:"broker"`
	Visualizer VisualizerConfig `yaml:"visualizer" mapstructure:"visualizer"`
}

type VisualizerConfig struct {
	Enabled bool   `yaml:"enabled" mapstructure:"enabled"`
	Address string `yaml:"address" mapstructure:"address"`
}

func LoadConfig(configPath string) (*Config, error) {
	viper.SetConfigType("yaml")
	viper.SetConfigFile(configPath)
	viper.AutomaticEnv()

	viper.SetDefault("kraft.timeout", 5*time.Second)
	viper.SetDefault("kraft.graceperiod", 10*time.Second)

	viper.SetDefault("server.address", "localhost")
	viper.SetDefault("server.port", 9092)
	viper.SetDefault("server.max_retries", 5)
	viper.SetDefault("server.initial_backoff", 250*time.Millisecond)

	viper.SetDefault("broker.heartbeat_interval", 250*time.Millisecond)
	viper.SetDefault("broker.metadata_interval", 250*time.Millisecond)
	viper.SetDefault("broker.port", 9093)
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
	if config.Server.NodeID == "" {
		return fmt.Errorf("server.node_id is required")
	}

	if config.Server.Address == "" {
		return fmt.Errorf("server.address is required")
	}

	if config.Server.Port <= 0 || config.Server.Port > 65535 {
		return fmt.Errorf("server.port must be between 1 and 65535")
	}

	if config.Kraft.Timeout <= 0 {
		return fmt.Errorf("kraft.timeout must be positive")
	}

	if config.Kraft.GracePeriod <= 0 {
		return fmt.Errorf("kraft.grace_period must be positive")
	}

	return nil
}
