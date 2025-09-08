package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"slices"

	"github.com/alexandrecolauto/gofka/server/pkg/broker"
	"github.com/alexandrecolauto/gofka/server/pkg/config"
	"github.com/alexandrecolauto/gofka/server/pkg/controller/kraft"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip"
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
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", config.Server.Port))
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer()
	var controllerServer *kraft.KraftServer
	var brokerServer *broker.BrokerServer
	isController := slices.Contains(config.Server.Roles, "controller")
	isBroker := slices.Contains(config.Server.Roles, "broker")
	if isController {
		log.Println("Starting cotroller", config.Server.NodeID)
		controllerServer, err = kraft.NewControllerServer(config)
		if err != nil {
			return err
		}
		defer controllerServer.Shutdown()
		controllerServer.Register(grpcServer)
	}
	if isBroker {
		log.Println("Starting broker", config.Server.NodeID)
		brokerServer, err = broker.NewBrokerServer(config)
		if err != nil {
			return err
		}
		defer brokerServer.Shutdown()
		brokerServer.Register(grpcServer)
	}
	errCh := make(chan error, 2)
	go func() {
		log.Println("Serving cotroller", config.Server.NodeID)
		errCh <- grpcServer.Serve(listener)
	}()

	if isController {
		controllerServer.ConnectGRPC()
	}
	if isBroker {
		brokerServer.Connect()
	}

	return <-errCh
}
