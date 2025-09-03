export default class Controller {
    constructor(app, container) {
        this.app = app
        this.container = container
        this.n_controllers = 0
        this.controllers = {}
    }

    handleMessage(message) {
        switch (message.action) {
            case "alive":
                this.addController(message.target)
                break;
            case "leader":
                this.controllerLeader(message.target)
                break;
            case "log_append":
                this.logAppend(message)
                break;
        }
    }

    addController(id) {
        console.log("creating new controller")
        if (this.controllers[id]) {
            console.log("controller already exists")
            return
        }
        const container = new PIXI.Container()
        // Load the SVG texture and create a sprite
        const texture = PIXI.Texture.from('./static/server.svg');
        const controller = new PIXI.Sprite(texture);

        // Set the anchor to center the sprite
        controller.anchor.set(0.5);

        // Scale the sprite to match the original circle size (100px diameter)
        controller.width = 100;
        controller.height = 100;

        container.nodeId = id
        container.x = (250 + (250 * this.n_controllers)) / 2;
        container.y = 150
        container.interactive = true;
        container.buttonMode = true;
        container.on('pointerdown', () => {
            alert(`Broker ${controller.nodeId} clicked!`);
            // Here you would send a request to your Go server to "fence" the controller
        });

        const idLabel = createID(id)

        const leo = createStatLEO()
        container.leoText = leo

        const term = createStatTerm()
        container.termText = term

        const lastApplied = createStatLastApplied()
        container.lastAppliedText = lastApplied

        container.addChild(controller)
        container.addChild(idLabel)
        container.addChild(leo)
        container.addChild(term)
        container.addChild(lastApplied)
        this.controllers[id] = container
        this.container.addChild(container)
        this.n_controllers++
    }

    controllerLeader(id) {
        if (!this.controllers[id]) {
            console.log("cannot find controller", id)
            return
        }

        // Remove crown from all controllers first
        Object.values(this.controllers).forEach(controller => {
            if (controller.crown) {
                controller.removeChild(controller.crown);
                controller.crown = null;
            }
        });

        // Add crown to the leader controller
        const controller = this.controllers[id]; // Added 'const' here
        const crownTexture = PIXI.Texture.from('./static/crown.svg');
        const crown = new PIXI.Sprite(crownTexture);

        // Position crown at the top of the controller
        crown.anchor.set(0.5);
        crown.width = 60;  // Adjust size as needed
        crown.height = 60;
        crown.x = 0;  // Relative to controller center
        crown.y = -60; // Above the controller (negative y goes up)

        // Store crown reference and add to controller
        controller.crown = crown;
        controller.addChild(crown);
    }

    logAppend(message) {
        if (!this.controllers[message.target]) {
            console.log("cannot find controller", id)
            return
        }
        const data = decodeBase64ToJSON(message.data)

        const container = this.controllers[message.target]

        const { leoText, termText, lastAppliedText } = container
        const { term, leo, last_applied } = data
        leoText.text = `LEO: ${leo}`
        termText.text = `Term: ${term}`
        lastAppliedText.text = `LastApplied: ${last_applied || 0}`


    }
}

function createID(id) {
    const idStyle = new PIXI.TextStyle({
        fontFamily: 'Arial',
        fontSize: 12,
        fontWeight: 'bold',
        fill: 0x333333,
        align: 'center'
    })
    const idLabel = new PIXI.Text(`${id}`, idStyle);
    idLabel.anchor.set(0.5);
    idLabel.x = 0;
    idLabel.y = -50; // Above the broker icon
    return idLabel
}

function createStatLEO() {
    const idStyle = new PIXI.TextStyle({
        fontFamily: 'Arial',
        fontSize: 12,
        fontWeight: 'bold',
        fill: 0x333333,
        align: 'center'
    })
    const idLabel = new PIXI.Text(`LEO: ${150}`, idStyle);
    idLabel.anchor.set(0.5);
    idLabel.x = 0;
    idLabel.y = 75;
    return idLabel
}

function createStatTerm() {
    const n = 1
    const idStyle = new PIXI.TextStyle({
        fontFamily: 'Arial',
        fontSize: 12,
        fontWeight: 'bold',
        fill: 0x333333,
        align: 'center'
    })
    const idLabel = new PIXI.Text(`Term: ${2}`, idStyle);
    idLabel.anchor.set(0.5);
    idLabel.x = 0;
    idLabel.y = 75 + (n * 25);
    return idLabel
}

function createStatLastApplied() {
    const n = 2
    const idStyle = new PIXI.TextStyle({
        fontFamily: 'Arial',
        fontSize: 12,
        fontWeight: 'bold',
        fill: 0x333333,
        align: 'center'
    })
    const idLabel = new PIXI.Text(`Last Applied: ${2}`, idStyle);
    idLabel.anchor.set(0.5);
    idLabel.x = 0;
    idLabel.y = 75 + (n * 25);
    return idLabel
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
