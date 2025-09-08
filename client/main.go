package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/alexandrecolauto/gofka/client/config"
	"github.com/alexandrecolauto/gofka/client/pkg/consumer"
	"github.com/alexandrecolauto/gofka/client/pkg/producer"
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
		path = "gofka-client.yaml"
	}
	config, err := config.LoadConfig(path)
	if err != nil {
		fmt.Println("error loadign config: ", err)
		return err
	}
	if config.Producer.Enabled {
		p := producer.NewProducer(&config.Producer)
		p.ConnectToBroker()
	}

	if config.Consumer.Enabled {
		c := consumer.NewConsumer(&config.Consumer)
		ticker := time.NewTicker(2 * time.Second)
		for range ticker.C {
			c.CommitOffsets()
		}
	}
	waitCh := make(chan any)
	<-waitCh
	return nil
}
