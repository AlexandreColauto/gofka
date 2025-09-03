class Websocket {
    retries = 0
    lastOffset = -1
    connectWebSocket(onMsgReceived) {
        this.onMsgReceived = onMsgReceived
        this.socket = new WebSocket("ws://" + window.location.host + "/ws");

        this.socket.onopen = () => {
            this.sendMessage("ping", "hello wrold")
        };

        this.socket.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);
                const prevOffset = this.lastOffset;
                const applying = []
                for (const msg of data) {
                    if (msg.log_index === -420) {
                        //metadata and log append
                        applying.push(msg)
                    }
                    if (msg.log_index > this.lastOffset) {
                        this.lastOffset = msg.log_index
                        applying.push(msg)
                    }
                }
                if (this.lastOffset > prevOffset) {
                    this.sendMessage("offset", this.lastOffset)
                }

                for (const msg of applying) {
                    this.onMsgReceived(msg)
                }
            } catch (e) {
                console.error("Failed to parse message as JSON:", e);
            }
        };

        this.socket.onclose = (event) => {
            console.log("WebSocket connection closed:", event);
            this.retries++
            if (this.retries < 5) {
                setTimeout(() => this.connectWebSocket(this.onMsgReceived), 1000);
            }
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
