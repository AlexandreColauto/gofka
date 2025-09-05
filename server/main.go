package main

import (
	"fmt"

	visualizerclient "github.com/alexandrecolauto/gofka/common/pkg/visualizer_client"
	"github.com/alexandrecolauto/gofka/server/pkg/controller/kraft"
)

func main() {
	fmt.Println("hello")
}
func setupRaftController() {
	nodeAddresses := map[string]string{
		"node1": "localhost:42069",
		"node2": "localhost:42070",
		"node3": "localhost:42071",
		"node4": "localhost:42072",
		"node5": "localhost:42073",
	}

	controllers := make([]*kraft.KraftServer, 0, 5)
	for nodeID, address := range nodeAddresses {
		peers := make(map[string]string)
		for nID, addr := range nodeAddresses {
			if nID != nodeID {
				peers[nID] = addr
			}
		}
		nodeType := "controller"
		viCli := visualizerclient.NewVisualizerClient(nodeType, "localhost:42042")

		ctrl, err := kraft.NewControllerServer(nodeID, address, peers, viCli)
		if err != nil {
			panic(err)
		}
		controllers = append(controllers, ctrl)
	}
}
