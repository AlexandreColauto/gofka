package visualizer

import (
	"fmt"
	"log"
	"time"

	"github.com/gorilla/websocket"
)

type Message struct {
	Type      string `json:"type"`
	LogIndex  int    `json:"log_index"`
	NodeType  string `json:"node_type"`
	Action    string `json:"action"`
	Target    string `json:"target"`
	Data      any    `json:"data"`
	Timestamp int64  `json:"timestamp"`
}

type Client struct {
	conn              *websocket.Conn
	send              chan []Message
	server            *VisualizerServer
	id                string
	lastOffset        int
	appendCommandFunc func(command Message) error
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

		// Handle different message types
		c.handleMessage(msg)
	}
}

func (c *Client) handleMessage(msg Message) {
	switch msg.Type {
	case "ping":
		// Respond with pong
		response := []Message{Message{
			Type:      "pong",
			Data:      msg.Data,
			Timestamp: time.Now().Unix(),
			LogIndex:  -1,
		}}
		c.SendMessage(response)
	case "offset":
		dt, ok := msg.Data.(float64)
		if !ok {
			fmt.Println("Cannot convert data: ", msg.Data)
			return
		}
		c.lastOffset = int(dt)

	case "create-topic":
		log.Printf("appending create topic for client %s", c.id)
		c.appendCommandFunc(msg)
	case "update-topic":
		log.Printf("updating topic for client %s", c.id)
		c.appendCommandFunc(msg)
	case "add-topic":
		log.Printf("updating topic for client %s", c.id)
		c.appendCommandFunc(msg)
	case "remove-topic":
		log.Printf("updating topic for client %s", c.id)
		c.appendCommandFunc(msg)
	case "send-message":
		log.Printf("sending msg for client %s", c.id)
		c.appendCommandFunc(msg)
	case "consume-message":
		log.Printf("sending msg for client %s", c.id)
		c.appendCommandFunc(msg)
	case "fence":
		log.Printf("fencing client %s", c.id)
		c.appendCommandFunc(msg)

	default:
		log.Printf("Unknown message type: %s", msg.Type)
	}
}

func (c *Client) SendMessage(msg []Message) {
	select {
	case c.send <- msg:
		// log.Printf("Message sent to client %s: %s", c.id, msg[0].Type)
	default:
		log.Printf("Failed to send message to client %s: channel full", c.id)
	}
}

func generateClientID() string {
	return fmt.Sprintf("client_%d", time.Now().UnixNano())
}
func (c *Client) SetAppendCommandFunc(aFunc func(command Message) error) {
	c.appendCommandFunc = aFunc
}
