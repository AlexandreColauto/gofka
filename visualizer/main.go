package visualizer

import (
	"log"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for simplicity, but you should restrict this in production
	},
}

type VisualizerServer struct {
	clients    map[*Client]bool
	register   chan *Client
	unregister chan *Client
	mutex      sync.RWMutex
	grpc       *VisualizerGRPCServer
	msgCh      chan Message
	msgBuffer  []Message
}

func NewVisualizer() *VisualizerServer {
	grpcCh := make(chan Message)
	msgB := make([]Message, 0)
	grpcS := NewVisualizerGRPCServer(grpcCh)
	vs := &VisualizerServer{
		grpc:       grpcS,
		clients:    make(map[*Client]bool),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		msgCh:      grpcCh,
		msgBuffer:  msgB,
	}
	go vs.grpcMessages()
	return vs
}

func (v *VisualizerServer) Start() {
	go v.run()
	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("./static/"))))

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, filepath.Join("static", "index.html"))
	})

	http.HandleFunc("/ws", v.HandleWebSocket)

	log.Println("Starting server at :41042")
	go v.grpc.Start("42042")
	http.ListenAndServe(":41042", nil)
}

func (v *VisualizerServer) grpcMessages() {
	for msg := range v.msgCh {
		if msg.Action == "metadata" || msg.Action == "log_append" {
			msg.LogIndex = -420
			for cli := range v.clients {
				cli.SendMessage([]Message{msg})
			}
		} else {
			msg.LogIndex = len(v.msgBuffer)
			v.msgBuffer = append(v.msgBuffer, msg)
			v.SendToClients()
		}
	}
}
func (v *VisualizerServer) run() {
	for {
		select {
		case client := <-v.register:
			v.mutex.Lock()
			v.clients[client] = true
			v.mutex.Unlock()
			log.Printf("Client %s registered. Total clients: %d", client.id, len(v.clients))

			// Send welcome message
			welcomeMsg := []Message{Message{
				Type:      "welcome",
				Data:      map[string]string{"message": "Connected to visualizer"},
				Timestamp: time.Now().Unix(),
				LogIndex:  -1,
			}}
			select {
			case client.send <- welcomeMsg:
			default:
				close(client.send)
				delete(v.clients, client)
			}

		case client := <-v.unregister:
			v.mutex.Lock()
			if _, ok := v.clients[client]; ok {
				delete(v.clients, client)
				close(client.send)
				log.Printf("Client %s unregistered. Total clients: %d", client.id, len(v.clients))
			}
			v.mutex.Unlock()

		}
	}
}

func (v *VisualizerServer) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Upgrade failed:", err)
		return
	}

	// Create new client
	client := &Client{
		conn:   conn,
		send:   make(chan []Message, 256),
		server: v,
		id:     generateClientID(),
	}
	v.register <- client

	go client.writePump()
	go client.readPump()
}

func (v *VisualizerServer) SendToClients() {
	for cli := range v.clients {
		msgs := v.msgBuffer[cli.lastOffset:]
		cli.SendMessage(msgs)
	}
}
