export default class Producer {
    constructor(app, container) {
        this.app = app
        this.container = container
        this.n_producers = 0
        this.producers = {}
    }

    handleMessage(message) {
        switch (message.action) {
            case "alive":
                this.addProducer(message.target)
                break;
            //case "metadata":
            //    this.handleMetadata(message)
            //    break;

        }
    }

    addProducer(id) {
        console.log("creating new producer")
        if (this.producers[id]) {
            console.log("producer already exists")
            return
        }
        const container = new PIXI.Container()
        // Load the SVG texture and create a sprite
        const texture = PIXI.Texture.from('./static/producer.svg');
        const producer = new PIXI.Sprite(texture);

        // Set the anchor to center the sprite
        producer.anchor.set(0.5);

        // Scale the sprite to match the original circle size (100px diameter)
        producer.width = 100;
        producer.height = 100;

        container.nodeId = id
        container.x = (250 + (250 * this.n_producers)) / 2;
        container.y = 200
        container.interactive = true;
        container.buttonMode = true;
        container.on('pointerdown', () => {
            alert(`Broker ${container.nodeId} clicked!`);
            // Here you would send a request to your Go server to "fence" the controller
        });

        const idLabel = createID(id)

        container.addChild(producer)
        container.addChild(idLabel)
        this.producers[id] = container
        this.container.addChild(container)
        this.n_producers++
    }




}


function createID(id) {
    const idStyle = new PIXI.TextStyle({
        fontFamily: 'Arial',
        fontSize: 10,
        fontWeight: 'bold',
        fill: 0x333333,
        align: 'center'
    })
    const idLabel = new PIXI.Text(`${id.slice(0, 10)}...${id.slice(-5)}`, idStyle);
    idLabel.anchor.set(0.5);
    idLabel.x = 0;
    idLabel.y = -50; // Above the broker icon
    return idLabel
}

