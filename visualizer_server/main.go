package main

import (
	"fmt"
	"log"
	"os"

	"github.com/alexandrecolauto/gofka/visualizer_server/config"
	"github.com/alexandrecolauto/gofka/visualizer_server/visualizer"
)

func main() {
	if err := run(); err != nil {
		log.Printf("Application failed: %v", err)
		os.Exit(1)
	}
}
func run() error {
	path := os.Getenv("GOFKA_CONFIG_PATH")
	if path == "" {
		path = "gofka.yaml"
	}
	config, err := config.LoadConfig(path)
	if err != nil {
		fmt.Println("error loadign config: ", err)
		return err
	}
	vis := visualizer.NewVisualizer()
	errCh := make(chan error, 2)
	go func() {
		errCh <- vis.Start(config.Server.GRPCPort, config.Server.WebPort)
	}()

	return <-errCh
}
