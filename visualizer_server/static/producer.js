export default class Producer {
    constructor(app, container) {
        this.app = app
        this.container = container
        this.n_producers = 0
        this.producers = {}
        this.routerProducer = ""
        this.applyStartProducingParent = () => { }
        this.applyStopProducingParent = () => { }
    }

    handleMessage(message) {
        switch (message.action) {
            case "alive":
                this.addProducer(message.target)
                break;
            case "start-producing":
                this.applyStartProducing(message.target)
                break;
            case "stop-producing":
                this.applyStopProducing(message.target)
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
        if (this.routerProducer == "") {
            this.routerProducer = id
        }
        const container = new PIXI.Container()
        // Load the SVG texture and create a sprite
        const texture = PIXI.Texture.from('./static/producer.webp');
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

    applyStartProducing(id) {
        this.applyStartProducingParent()
        const producerContainer = this.producers[id];
        // Exit if producer doesn't exist or is already animating
        if (!producerContainer || producerContainer.isProducing) {
            return;
        }
        producerContainer.isProducing = true;

        // The actual sprite is the first child we added
        const producerSprite = producerContainer.children[0];


        // Store the original position for the shake effect
        const originalX = producerSprite.x;
        const originalY = producerSprite.y;
        const originalRotation = producerSprite.rotation; // Store original rotation
        const originalState = { originalX, originalY, originalRotation }

        // Define the animation function that will run on every frame
        const animationTicker = (ticker) => {
            // ticker.deltaMS can be used for frame-rate independent animation
            const delta = ticker;


            // Track elapsed time for the wobble effect
            if (!producerContainer.animationTime) {
                producerContainer.animationTime = 0;
            }
            producerContainer.animationTime += delta;

            // 1. Back-and-forth rotation using sine wave
            const wobbleSpeed = 0.6; // How fast it wobbles
            const wobbleAmount = 0.1; // Maximum rotation in radians (about 11.5 degrees)

            //this line is making my sprite disappear. without this line works fine. why?
            const newRotation = originalRotation + Math.sin(producerContainer.animationTime * wobbleSpeed) * wobbleAmount
            producerSprite.rotation = newRotation


            // 2. Shake Effect
            const shakeAmount = 2.0; // How many pixels to shake
            producerSprite.x = originalX + (Math.random() * shakeAmount) - (shakeAmount / 2);
            producerSprite.y = originalY + (Math.random() * shakeAmount) - (shakeAmount / 2);
        };

        // Store the ticker function on the container itself.
        // This makes it easy to find and remove later.
        producerContainer.animationTicker = animationTicker;
        producerContainer.originalState = originalState;

        PIXI.Ticker.shared.add(animationTicker);
    }

    applyStopProducing(id) {
        this.applyStopProducingParent()
        const producerContainer = this.producers[id];
        // Exit if producer doesn't exist or is not animating
        if (!producerContainer || !producerContainer.isProducing) {
            return;
        }
        console.log("Stopping producing for:", id);
        producerContainer.isProducing = false;

        // If an animation ticker is stored on the container, remove it.
        if (producerContainer.animationTicker) {
            PIXI.Ticker.shared.remove(producerContainer.animationTicker);
            producerContainer.animationTicker = null; // Clean up the reference
        }
        // Reset the sprite to its original, non-animated state.
        const producerSprite = producerContainer.children[0];
        const originalState = producerContainer.animationState;
        producerSprite.rotation = originalState ? originalState.originalRotation : 0;
        producerSprite.x = originalState ? originalState.originalX : 0;
        producerSprite.y = originalState ? originalState.originalY : 0;

    }

    startProducing() {
        Object.keys(this.producers || {}).forEach(producer => {
            this.produceMessage(producer)
        })
    }

    stopProducing() {
        Object.keys(this.producers || {}).forEach(producer => {
            this.stopProduceMessage(producer)
        })
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

