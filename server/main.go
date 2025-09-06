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
		controllerServer, err = kraft.NewControllerServer(config)
		if err != nil {
			return err
		}
		defer controllerServer.Shutdown()
		controllerServer.Register(grpcServer)
	}
	if isBroker {
		brokerServer, err = broker.NewBrokerServer(config)
		if err != nil {
			return err
		}
		defer brokerServer.Shutdown()
		brokerServer.Register(grpcServer)
	}
	errCh := make(chan error, 2)
	go func() {
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
