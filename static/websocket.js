class Websocket {
    connectWebSocket() {
        this.socket = new WebSocket("ws://" + window.location.host + "/ws");

        this.socket.onopen = () => {
            this.sendMessage("ping", "hello wrold")
        };

        this.socket.onmessage = (event) => {
            console.log("Received data from server:", event.data);
            try {
                const data = JSON.parse(event.data);
                console.log("Parsed data:", data);
            } catch (e) {
                console.error("Failed to parse message as JSON:", e);
            }
        };

        this.socket.onclose = (event) => {
            console.log("WebSocket connection closed:", event);
            setTimeout(() => this.connectWebSocket(), 1000);
        };

        this.socket.onerror = (error) => {
            console.error("WebSocket error:", error);
        };
    }

    sendMessage(type, data) {
        if (this.socket && this.socket.readyState === WebSocket.OPEN) {
            const message = { type, data }
            if (!message.timestamp) {
                message.timestamp = Date.now();
            }
            const jsonMessage = JSON.stringify(message);
            this.socket.send(jsonMessage);
        } else {
            console.warn("WebSocket is not connected. Cannot send message:", message);
        }
    }
}
export default Websocket
