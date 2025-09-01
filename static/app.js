import Controller from './controller.js';
import Websocket from './websocket.js';

class KafkaVisualizer {
    constructor() {
        this.app = new PIXI.Application({
            width: window.innerWidth,
            height: window.innerHeight,
            backgroundColor: 0x1099bb
        });

        document.body.appendChild(this.app.view)

        this.app.view.id = 'gameCanvas'
        this.init()
    }

    async init() {
        this.brokersContainer = new PIXI.Container();
        const ctr = new Controller(this.app, this.brokersContainer)
        ctr.addBroker()
        ctr.addBroker()
        // Add a click event to the broker
        this.app.stage.addChild(this.brokersContainer);
        this.socket = new Websocket()
        this.socket.connectWebSocket()
    }

}

window.addEventListener('load', () => {
    new KafkaVisualizer();
});
