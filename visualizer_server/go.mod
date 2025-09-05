module github.com/alexandrecolauto/gofka/visualizer_server

go 1.23.3

require (
	github.com/alexandrecolauto/gofka/common v0.0.0
	github.com/gorilla/websocket v1.5.3
	google.golang.org/grpc v1.75.0
	google.golang.org/protobuf v1.36.8
)

require (
	golang.org/x/net v0.43.0 // indirect
	golang.org/x/sys v0.35.0 // indirect
	golang.org/x/text v0.28.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250818200422-3122310a409c // indirect
)

replace github.com/alexandrecolauto/gofka/common => ../common
