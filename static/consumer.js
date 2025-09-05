export default class Consumer {
    constructor(app, container) {
        this.app = app
        this.container = container
        this.n_consumers = 0
        this.consumers = {}
        this.applyStartConsumingParent = () => { }
        this.applyStopConsumingParent = () => { }
    }

    handleMessage(message) {
        switch (message.action) {
            case "alive":
                this.addConsumer(message)
                break;
            case "assigns":
                this.consumerAssignments(message)
                break;
            case "consumed-offset":
                this.consumerOffset(message)
                break;
            case "start-consuming":
                this.applyStartConsuming(message.target)
                break;
            case "stop-consuming":
                this.applyStopConsuming(message.target)
                break;
            default:
                console.log("cannot find handler for:", message.action)

        }
    }

    addConsumer(message) {
        const id = message.target
        const consumerGroup = atob(message.data);
        console.log("creating new consumer", id, message, consumerGroup)
        if (this.consumers[id]) {
            console.log("consumer already exists", id)
            return
        }
        const container = new PIXI.Container()
        // Load the SVG texture and create a sprite
        const texture = PIXI.Texture.from('./static/consumer.svg');
        const consumer = new PIXI.Sprite(texture);

        // Set the anchor to center the sprite
        consumer.anchor.set(0.5);

        // Scale the sprite to match the original circle size (100px diameter)
        consumer.width = 100;
        consumer.height = 100;

        container.nodeId = id
        container.x = (250 + (250 * this.n_consumers)) / 2;
        container.y = 200
        container.interactive = true;
        container.buttonMode = true;
        container.on('pointerdown', () => {
            alert(`Broker ${container.nodeId} clicked!`);
            // Here you would send a request to your Go server to "fence" the controller
        });

        const idLabel = createID(id)

        const cg = createStatConsumerGroup(consumerGroup)
        container.consumerGroup = consumerGroup
        container.cgText = cg

        const assigns = createStatAssignments()
        container.assignsText = assigns
        assigns.visible = false

        container.addChild(consumer)
        container.addChild(idLabel)
        container.addChild(cg)
        container.addChild(assigns)
        container.assignments = {}

        this.consumers[id] = container
        console.log(`New consumer ${id} addeded`)
        this.container.addChild(container)
        this.n_consumers++
    }

    consumerAssignments(message) {
        const assigns = decodeBase64ToJSON(message.data)
        console.log("new assignmens: ", assigns)
        this.udpateAssignments(assigns)
    }
    consumerOffset(message) {
        const offsets = decodeBase64ToJSON(message.data)
        console.log("updating offset:", offsets)
        this.updateOffsets(offsets, message.target)
    }

    udpateAssignments(assignments) {
        if (!this.consumers[assignments.id]) {
            return
        }
        const container = this.consumers[assignments.id]
        for (const topic of Object.values(container.assignments)) {
            for (const partition of Object.values(topic)) {
                container.removeChild(partition)
            }
        }
        for (let i = 0; i < assignments.assignments.length; i++) {
            const ass = assignments.assignments[i]
            const topic = ass.topic_name
            const partition = ass.id || 0
            console.log(ass)
            const assignText = createAssignmentText(topic, partition, i)
            container.addChild(assignText)
            if (!container.assignments[topic]) {
                container.assignments[topic] = {}
            }
            container.assignments[topic][partition] = assignText
            console.log("new container assignemnt", container.assignments)
        }
        const text = container.assignsText
        //text.text = `Assignments ${assignList.map(x => "\n" + x)}`
        text.visible = true

        const cgText = container.cgText
        cgText.text = `CG: ${container.consumerGroup}${assignments.leader ? " (L)" : ""}`
        cgText.visible = true

    }
    updateOffsets(offsets, id) {
        console.log('updating offsets: ', offsets)
        if (!this.consumers[id]) {
            return
        }
        const container = this.consumers[id]
        for (const topic of Object.keys(offsets)) {
            for (const partition of Object.keys(offsets[topic])) {
                const offset = offsets[topic][partition]
                const text = container.assignments[topic][partition]
                text.text = `${topic} - ${partition} (H: ${offset}) `
            }
        }
    }
    applyStartConsuming(id) {
        this.applyStartConsumingParent()
        const producerContainer = this.consumers[id];
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

    applyStopConsuming(id) {
        this.applyStopConsumingParent()
        const producerContainer = this.consumers[id];
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

}


function decodeBase64ToJSON(base64String) {
    try {
        // Decode base64 to string
        const jsonString = atob(base64String);

        // Parse JSON string to object
        const jsonObject = JSON.parse(jsonString);

        return jsonObject;
    } catch (error) {
        console.error('Error decoding base64 or parsing JSON:', error);
        return null;
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

function createStatConsumerGroup(consumerGroup) {
    const idStyle = new PIXI.TextStyle({
        fontFamily: 'Arial',
        fontSize: 10,
        fontWeight: 'bold',
        fill: 0x333333,
        align: 'center'
    })
    const idLabel = new PIXI.Text(`CG: ${consumerGroup}`, idStyle);
    idLabel.anchor.set(0.5);
    idLabel.x = 0;
    idLabel.y = 50;
    return idLabel
}

function createStatAssignments() {
    const n = 0
    const idStyle = new PIXI.TextStyle({
        fontFamily: 'Arial',
        fontSize: 12,
        fontWeight: 'bold',
        fill: 0x333333,
        align: 'center',
        breakWords: true,
        wordWrap: true
    })
    const idLabel = new PIXI.Text(`Assignments `, idStyle);
    idLabel.anchor.set(0.5);
    idLabel.x = 0;
    idLabel.y = 75 + (n * 25);
    return idLabel
}
function createAssignmentText(topic, partition, n) {
    const idStyle = new PIXI.TextStyle({
        fontFamily: 'Arial',
        fontSize: 12,
        fontWeight: 'bold',
        fill: 0x333333,
        align: 'center',
        breakWords: true,
        wordWrap: true
    })
    const idLabel = new PIXI.Text(`${topic} - ${partition} `, idStyle);
    idLabel.anchor.set(0.5);
    idLabel.x = 0;
    idLabel.y = 100 + (n * 25);
    return idLabel
}
