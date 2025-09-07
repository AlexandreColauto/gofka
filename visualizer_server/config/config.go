package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	Server     ServerConfig     `yaml:"server" mapstructure:"server"`
	Visualizer VisualizerConfig `yaml:"visualizer" mapstructure:"visualizer"`
}

type ServerConfig struct {
	Address  string `yaml:"address" mapstructure:"address"`
	GRPCPort int    `yaml:"grpc_port" mapstructure:"grpc_port"`
	WebPort  int    `yaml:"web_port" mapstructure:"web_port"`
}

type VisualizerConfig struct {
	Enabled bool `yaml:"enabled" mapstructure:"enabled"`
}

func LoadConfig(configPath string) (*Config, error) {
	env := os.Getenv("SERVER_NODE_ID")
	fmt.Println("NODE ID FROM ENV: ", env)
	viper.SetConfigType("yaml")
	viper.SetConfigFile(configPath)
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AllowEmptyEnv(true)

	viper.SetDefault("server.address", "localhost")
	viper.SetDefault("server.web_port", 42069)
	viper.SetDefault("server.grpc_port", 42169)

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
	if config.Server.GRPCPort == 0 {
		return fmt.Errorf("server.grpc_port is required")
	}
	if config.Server.WebPort == 0 {
		return fmt.Errorf("server.web_port is required")
	}

	if config.Server.Address == "" {
		return fmt.Errorf("server.address is required")
	}

	if config.Server.GRPCPort <= 0 || config.Server.GRPCPort > 65535 {
		return fmt.Errorf("server.grpc_port must be between 1 and 65535")
	}
	if config.Server.WebPort <= 0 || config.Server.WebPort > 65535 {
		return fmt.Errorf("server.web_port must be between 1 and 65535")
	}
	if config.Server.WebPort == config.Server.GRPCPort {
		return fmt.Errorf("server.grpc_port must be different than server.web_port")
	}

	return nil
}
