package visualizer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"

	"github.com/alexandrecolauto/gofka/proto/broker"
	pv "github.com/alexandrecolauto/gofka/proto/visualizer"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type VisualizerGRPCServer struct {
	pv.UnimplementedVisualizerServiceServer
	msgCh       chan Message
	commandsFor func(target string) ([]Message, error)
}

func NewVisualizerGRPCServer(msgCh chan Message, commandFunc func(string) ([]Message, error)) *VisualizerGRPCServer {
	return &VisualizerGRPCServer{msgCh: msgCh, commandsFor: commandFunc}
}

func (v *VisualizerGRPCServer) Start(port string) error {
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer()
	pv.RegisterVisualizerServiceServer(grpcServer, v)
	log.Printf("Visualizer grpc starting on port: %s\n", port)
	return grpcServer.Serve(listener)
}

func (v *VisualizerGRPCServer) Update(ctx context.Context, req *pv.VisualizerRequest) (*pv.VisualizerResponse, error) {
	// fmt.Println("New message from", req.Target, req.Action)
	if req.Action == "commands" {
		commands := v.PendingCommandsFor(req.Target)
		res := &pv.VisualizerResponse{
			Success:  true,
			Commands: commands,
		}
		return res, nil
	}
	data := req.Data
	if req.Action == "metadata" {
		data = v.parseMetadata(data)
	}
	if req.Action == "assigns" {
		data = v.parseAssignments(data)
	}
	msg := Message{
		NodeType: req.NodeType,
		Action:   req.Action,
		Target:   req.Target,
		Data:     data,
	}
	v.msgCh <- msg
	// commands := v.PendingCommandsFor(req.Target)
	res := &pv.VisualizerResponse{
		Success: true,
	}
	return res, nil
}

func (v *VisualizerGRPCServer) parseMetadata(data []byte) []byte {
	var metadata broker.ClusterMetadata
	err := proto.Unmarshal(data, &metadata)
	if err != nil {
		return nil
	}
	str, err := json.Marshal(&metadata)
	if err != nil {
		return nil
	}
	return str
}

func (v *VisualizerGRPCServer) parseAssignments(data []byte) []byte {
	var metadata broker.ConsumerSession
	err := proto.Unmarshal(data, &metadata)
	if err != nil {
		return nil
	}
	str, err := json.Marshal(&metadata)
	if err != nil {
		return nil
	}
	return str
}
func (v *VisualizerGRPCServer) PendingCommandsFor(target string) []*pv.Command {
	msgs, err := v.commandsFor(target)
	if err != nil {
		return nil
	}
	res := []*pv.Command{}
	for _, msg := range msgs {
		data, ok := msg.Data.(string)
		if !ok {
			fmt.Printf("failed parsing data: %T\n", msg.Data)
		}
		c := &pv.Command{
			Action: msg.Action,
			Target: msg.Target,
			Data:   []byte(data),
		}
		res = append(res, c)
	}
	return res
}
