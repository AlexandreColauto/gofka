export default class Consumer {
    constructor(app, container) {
        this.app = app
        this.container = container
        this.n_consumers = 0
        this.consumers = {}
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
