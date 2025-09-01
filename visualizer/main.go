package visualizer

import (
	"fmt"
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

type Message struct {
	Type      string `json:"type"`
	Data      any    `json:"data"`
	Timestamp int64  `json:"timestamp"`
}

type Client struct {
	conn   *websocket.Conn
	send   chan Message
	server *VisualizerServer
	id     string
}

type VisualizerServer struct {
	clients    map[*Client]bool
	register   chan *Client
	unregister chan *Client
	mutex      sync.RWMutex
}

func NewVisualizer() *VisualizerServer {
	return &VisualizerServer{
		clients:    make(map[*Client]bool),
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}
}

func (v *VisualizerServer) Start() {
	go v.run()
	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("./static/"))))

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, filepath.Join("static", "index.html"))
	})

	http.HandleFunc("/ws", v.HandleWebSocket)

	log.Println("Starting server at :42069")
	http.ListenAndServe(":42069", nil)
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
			welcomeMsg := Message{
				Type:      "welcome",
				Data:      map[string]string{"message": "Connected to visualizer"},
				Timestamp: time.Now().Unix(),
			}
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
		send:   make(chan Message, 256),
		server: v,
		id:     generateClientID(),
	}
	v.register <- client

	go client.writePump()
	go client.readPump()
}

func (c *Client) writePump() {
	ticker := time.NewTicker(54 * time.Second)
	defer func() {
		fmt.Println("write pump closing")
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			if err := c.conn.WriteJSON(message); err != nil {
				log.Printf("Write error for client %s: %v", c.id, err)
				return
			}

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (c *Client) readPump() {
	defer func() {
		fmt.Println("reading pump closing")
		c.server.unregister <- c
		c.conn.Close()
	}()

	// Set read deadline and pong handler for keepalive
	c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		var msg Message
		err := c.conn.ReadJSON(&msg)
		if err != nil {
			fmt.Println("err reading from connection", err)
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("WebSocket error: %v", err)
			}
			break
		}

		msg.Timestamp = time.Now().Unix()
		log.Printf("Received from client %s: %+v", c.id, msg)

		// Handle different message types
		c.handleMessage(msg)
	}
}

func (c *Client) handleMessage(msg Message) {
	switch msg.Type {
	case "ping":
		// Respond with pong
		response := Message{
			Type:      "pong",
			Data:      msg.Data,
			Timestamp: time.Now().Unix(),
		}
		c.SendMessage(response)

	case "visualize":
		// Handle visualization requests
		log.Printf("Processing visualization request from client %s", c.id)
		// Add your visualization logic here

		// Example response
		response := Message{
			Type: "visualization_data",
			Data: map[string]interface{}{
				"nodes": []map[string]interface{}{
					{"id": "1", "label": "Package A", "x": 100, "y": 100},
					{"id": "2", "label": "Package B", "x": 200, "y": 200},
				},
				"edges": []map[string]interface{}{
					{"from": "1", "to": "2", "label": "depends on"},
				},
			},
			Timestamp: time.Now().Unix(),
		}
		c.SendMessage(response)

	default:
		log.Printf("Unknown message type: %s", msg.Type)
	}
}

func (c *Client) SendMessage(msg Message) {
	select {
	case c.send <- msg:
		log.Printf("Message sent to client %s: %s", c.id, msg.Type)
	default:
		log.Printf("Failed to send message to client %s: channel full", c.id)
	}
}

func generateClientID() string {
	return fmt.Sprintf("client_%d", time.Now().UnixNano())
}

func (v *VisualizerServer) SendToClient(clientID string, msg Message) {
	v.mutex.RLock()
	defer v.mutex.RUnlock()

	for client := range v.clients {
		if client.id == clientID {
			client.SendMessage(msg)
			return
		}
	}
	log.Printf("Client %s not found", clientID)
}
