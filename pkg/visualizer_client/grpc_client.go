package visualizerclient

import (
	"context"
	"fmt"
	"time"

	pv "github.com/alexandrecolauto/gofka/proto/visualizer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type VisualizerClient struct {
	nodeType  string
	vAddress  string
	target    string
	vConn     *grpc.ClientConn
	vCli      pv.VisualizerServiceClient
	Processor *CommandProcessor
}

func NewVisualizerClient(nType, address string) *VisualizerClient {
	p := NewCommandProcessor()
	v := &VisualizerClient{nodeType: nType, vAddress: address, Processor: p}
	p.sendError = v.SendError
	v.Connect()
	go v.FetchCommands()
	return v
}

func (v *VisualizerClient) Connect() error {
	conn, err := grpc.NewClient(v.vAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	v.vConn = conn
	v.vCli = pv.NewVisualizerServiceClient(conn)
	return nil
}

func (v *VisualizerClient) SendMessage(action, target string, data []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := &pv.VisualizerRequest{
		NodeType: v.nodeType,
		Target:   target,
		Action:   action,
		Data:     data,
	}
	res, err := v.vCli.Update(ctx, req)
	if err != nil {
		fmt.Println("error sending msg: ", err)
		return err
	}
	if !res.Success {
		return fmt.Errorf("error sending request with data %s", data)
	}
	if len(res.Commands) > 0 {
		v.Processor.Process(res.Commands)
	}

	return nil
}
func (v *VisualizerClient) FetchCommands() {
	ticker := time.NewTicker(250 * time.Millisecond)
	for range ticker.C {
		v.fetchCommand()
	}
}

func (v *VisualizerClient) fetchCommand() {
	for id := range v.Processor.clients {
		v.SendMessage("commands", id, nil)
	}
}
func (v *VisualizerClient) SendError(target, errorMsg string) {
	v.SendMessage("error", target, []byte(errorMsg))

}
