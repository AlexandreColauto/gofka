package visualizer

import (
	"context"
	"encoding/json"
	"log"
	"net"

	"github.com/alexandrecolauto/gofka/proto/broker"
	pv "github.com/alexandrecolauto/gofka/proto/visualizer"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type VisualizerGRPCServer struct {
	pv.UnimplementedVisualizerServiceServer
	msgCh chan Message
}

func NewVisualizerGRPCServer(msgCh chan Message) *VisualizerGRPCServer {
	return &VisualizerGRPCServer{msgCh: msgCh}
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
