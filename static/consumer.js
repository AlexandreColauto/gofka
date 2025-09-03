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
            alert(`Broker ${controller.nodeId} clicked!`);
            // Here you would send a request to your Go server to "fence" the controller
        });

        const idLabel = createID(id)

        const cg = createStatConsumerGroup(consumerGroup)
        container.consumerGroup = consumerGroup
        container.cgText = cg

        const assigns = createStatAssignments()
        container.assignsText = assigns

        container.addChild(consumer)
        container.addChild(idLabel)
        container.addChild(cg)
        container.addChild(assigns)

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

    udpateAssignments(assignments) {
        const assignList = []
        for (const ass of assignments.assignments) {
            console.log(ass)
            assignList.push(`${ass.topic_name}-${ass.id || 0}`)
        }
        const container = this.consumers[assignments.id]
        const text = container.assignsText
        text.text = `Assignments ${assignList.map(x => "\n" + x)}`

        const cgText = container.cgText
        cgText.text = `CG: ${container.consumerGroup}${assignments.leader ? " (L)" : ""}`

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
    idLabel.y = 75;
    return idLabel
}

function createStatAssignments() {
    const n = 2
    const idStyle = new PIXI.TextStyle({
        fontFamily: 'Arial',
        fontSize: 12,
        fontWeight: 'bold',
        fill: 0x333333,
        align: 'center',
        breakWords: true,
        wordWrap: true
    })
    const ass = [1, 2, 3]
    const idLabel = new PIXI.Text(`Assignments ${ass.map(x => "\nfoo-topic-" + x + "")}`, idStyle);
    idLabel.anchor.set(0.5);
    idLabel.x = 0;
    idLabel.y = 75 + (n * 25);
    return idLabel
}
