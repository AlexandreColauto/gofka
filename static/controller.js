export default class Controller {

    constructor(app, container) {
        this.app = app
        this.container = container
    }

    addBroker() {
        const broker = new PIXI.Graphics();
        broker.beginFill(0xffffff);
        broker.drawCircle(0, 0, 50);
        broker.endFill();
        broker.x = (this.app.screen.width + (250 * this.container.children.length)) / 2;
        broker.y = this.app.screen.height / 2;
        broker.interactive = true;
        broker.buttonMode = true;
        broker.on('pointerdown', () => {
            alert('Broker clicked!');
            // Here you would send a request to your Go server to "fence" the broker
        });
        this.container.addChild(broker)
    }
}
