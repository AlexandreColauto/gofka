import Controller from './controller.js';
import Broker from './broker.js';
import Websocket from './websocket.js';
import Producer from './producer.js';
import Consumer from './consumer.js';

class KafkaVisualizer {
    constructor() {
        this.app = new PIXI.Application({
            width: window.innerWidth,
            height: window.innerHeight,
            backgroundColor: 0xAAAAAA
        });

        document.body.appendChild(this.app.view)

        this.app.view.id = 'gameCanvas'
        this.init()
        this.availableTopics = []
    }

    async init() {
        this.initController()
        this.initBroker()
        this.initProducer()
        this.initConsumer()
        this.socket = new Websocket()
        this.socket.connectWebSocket((message) => this.onMsgReceived(message))
    }

    initController() {
        this.controllerContainer = new PIXI.Container();
        this.createContainerController()
        const ctr = new Controller(this.app, this.controllerContainer)
        ctr.fenceFunc = (target) => this.sendCommand({ type: "fence", action: "fence", data: "", target: target })
        this.controller = ctr
        this.app.stage.addChild(this.controllerContainer);
    }

    createContainerController() {
        // Create bounding box
        const boundingBox = new PIXI.Graphics();
        boundingBox.lineStyle(2, 0x333333); // 2px thick, dark gray border
        boundingBox.beginFill(0xf8f8f8, 0.1); // Light gray fill with low opacity
        boundingBox.drawRoundedRect(0, 0, 800, 400, 10); // Adjust size as needed
        boundingBox.endFill();

        // Create title text
        const titleStyle = new PIXI.TextStyle({
            fontFamily: 'Arial',
            fontSize: 24,
            fontWeight: 'bold',
            fill: 0x333333,
            align: 'center'
        });

        const title = new PIXI.Text('Kraft Controller', titleStyle);
        title.anchor.set(0.5, 0);
        title.x = boundingBox.width / 2;
        title.y = 10; // 10px from top of bounding box

        // Create inner container for brokers (with padding from title)
        const brokersInnerContainer = new PIXI.Container();
        brokersInnerContainer.y = 50; // Space for title + padding

        // Add everything to the main container
        this.controllerContainer.addChild(boundingBox);
        this.controllerContainer.addChild(title);
        this.controllerContainer.addChild(brokersInnerContainer);

        // Position the entire container
        this.controllerContainer.x = 50; // Adjust position as needed
        this.controllerContainer.y = 50;
    }

    initBroker() {
        this.brokerContainer = new PIXI.Container();
        this.createContainerBroker()
        const ctr = new Broker(this.app, this.brokerContainer)
        ctr.addAvailableTopic = (topic) => this.addTopic(topic)
        ctr.fenceFunc = (target) => this.sendCommand({ type: "fence", action: "fence", data: "", target: target })
        this.broker = ctr
        this.app.stage.addChild(this.brokerContainer);
    }
    createContainerBroker() {
        // Create bounding box with dynamic sizing
        const boundingBox = new PIXI.Graphics();
        boundingBox.lineStyle(2, 0x333333);
        boundingBox.beginFill(0xf8f8f8, 0.1);
        boundingBox.drawRoundedRect(0, 0, 800, 400, 10); // Larger container for detailed info
        boundingBox.endFill();

        // Create title text
        const titleStyle = new PIXI.TextStyle({
            fontFamily: 'Arial',
            fontSize: 24,
            fontWeight: 'bold',
            fill: 0x333333,
            align: 'center'
        });
        const title = new PIXI.Text('Kafka Brokers', titleStyle);
        title.anchor.set(0.5, 0);
        title.x = boundingBox.width / 2;
        title.y = 15;


        // Create inner container for brokers
        const brokersInnerContainer = new PIXI.Container();
        brokersInnerContainer.y = 90;

        // Add everything to the main container
        this.brokerContainer.addChild(boundingBox);
        this.brokerContainer.addChild(title);
        this.brokerContainer.addChild(brokersInnerContainer);

        // Position the entire container
        this.brokerContainer.x = this.app.screen.width / 2 + 20;
        this.brokerContainer.y = 50;
    }

    initProducer() {
        this.producerContainer = new PIXI.Container();
        this.createContainerProducer()
        const ctr = new Producer(this.app, this.producerContainer)
        ctr.produceMessage = (target) => this.sendCommand({ type: "send-message", action: "send-message", data: "", target: target })
        ctr.stopProduceMessage = (target) => this.sendCommand({ type: "stop-send-message", action: "stop-send-message", data: "", target: target })
        ctr.applyStartProducingParent = () => this.applyStartProducing()
        ctr.applyStopProducingParent = () => this.applyStopProducing()
        this.producer = ctr
        this.app.stage.addChild(this.producerContainer);
    }

    createContainerProducer() {
        // Create bounding box
        const boundingBox = new PIXI.Graphics();
        boundingBox.lineStyle(2, 0x333333); // 2px thick, dark gray border
        boundingBox.beginFill(0xf8f8f8, 0.1); // Light gray fill with low opacity
        boundingBox.drawRoundedRect(0, 0, 800, 400, 10); // Adjust size as needed
        boundingBox.endFill();

        // Create title text
        const titleStyle = new PIXI.TextStyle({
            fontFamily: 'Arial',
            fontSize: 24,
            fontWeight: 'bold',
            fill: 0x333333,
            align: 'center'
        });
        const title = new PIXI.Text('Gofka Producers', titleStyle);
        title.anchor.set(0.5, 0);
        title.x = boundingBox.width / 2;
        title.y = 10; // 10px from top of bounding box

        // Create topic controls container
        const topicControlsContainer = new PIXI.Container();
        topicControlsContainer.y = 45; // Below title

        const dropdownBg = createDropDownBg()
        const dropdownText = createDropDownText()
        const arrow = createDropDownArrow()

        // Make dropdown interactive
        dropdownBg.interactive = true;
        dropdownBg.buttonMode = true;
        dropdownBg.on('click', this.onTopicDropdownClick.bind(this));

        // Create "Add Topic" button
        const { addTopicButton, buttonText } = addButton("Add Topic")
        addTopicButton.on('click', this.onAddTopicClick.bind(this));

        const { addTopicButton: startButton, buttonText: startMsgText } = addButton("Send Msg")
        startButton.x = 350
        startMsgText.x = startButton.x + 50
        startButton.on('click', this.onSendMsg.bind(this));

        const { addTopicButton: stopButton, buttonText: stopMsgText } = addButton("Stop Send Msg", 0x750000)
        stopButton.x = 350
        stopMsgText.x = stopButton.x + 50
        stopButton.on('click', this.onStopSendMsg.bind(this));
        startButton.visible = false
        stopMsgText.visible = false

        // Add all dropdown elements to the controls container
        topicControlsContainer.addChild(dropdownBg);
        topicControlsContainer.addChild(dropdownText);
        topicControlsContainer.addChild(arrow);

        topicControlsContainer.addChild(addTopicButton);
        topicControlsContainer.addChild(buttonText);

        topicControlsContainer.addChild(startButton);
        topicControlsContainer.addChild(startMsgText);

        topicControlsContainer.addChild(stopButton);
        topicControlsContainer.addChild(stopMsgText);
        // Create inner container for brokers (with padding from title and controls)
        const brokersInnerContainer = new PIXI.Container();
        brokersInnerContainer.y = 90; // Space for title + controls + padding

        // Add everything to the main container
        this.producerContainer.addChild(boundingBox);
        this.producerContainer.addChild(title);
        this.producerContainer.addChild(topicControlsContainer);
        this.producerContainer.addChild(brokersInnerContainer);
        this.producerContainer.msgButton = { button: startButton, text: startMsgText }
        this.producerContainer.stopMsgButton = { button: stopButton, text: stopMsgText }

        // Position the entire container
        this.producerContainer.x = 50; // Adjust position as needed
        this.producerContainer.y = 550;

        // Store references for later use
        this.topicDropdownText = dropdownText;
        this.topicDropdownBg = dropdownBg;
        this.selectedTopic = null;
        this.availableTopics = []
    }

    // Handler for dropdown click
    onTopicDropdownClick() {
        this.showTopicDropdown();
    }

    // Handler for add topic button click
    onAddTopicClick() {
        this.showAddTopicDialog();
    }

    onSendMsg() {
        this.producerContainer.msgButton.button.interactive = false
        this.producer.startProducing()
    }

    onStopSendMsg() {
        this.producerContainer.stopMsgButton.button.interactive = false
        this.producer.stopProducing()
    }

    applyStartProducing() {
        const { button, text } = this.producerContainer.msgButton
        button.visible = false
        text.visible = false
        const { button: stop, text: stopText } = this.producerContainer.stopMsgButton
        stop.visible = true
        stop.interactive = true
        stopText.visible = true
    }

    applyStopProducing() {
        const { button, text } = this.producerContainer.msgButton
        button.visible = true
        button.interactive = true
        text.visible = true
        const { button: stop, text: stopText } = this.producerContainer.stopMsgButton
        stop.visible = false
        stopText.visible = false
    }

    // Method to show dropdown options
    showTopicDropdown() {
        // Remove existing dropdown if it exists
        if (this.topicDropdownMenu) {
            this.producerContainer.removeChild(this.topicDropdownMenu);
            this.topicDropdownMenu = null;
            return;
        }

        // Create dropdown menu
        this.topicDropdownMenu = new PIXI.Container();
        this.topicDropdownMenu.x = this.topicDropdownBg.x;
        this.topicDropdownMenu.y = this.topicDropdownBg.y + 35;

        // Create dropdown items
        this.availableTopics.forEach((topic, index) => {
            const itemBg = new PIXI.Graphics();
            itemBg.beginFill(0xffffff);
            itemBg.lineStyle(1, 0xcccccc);
            itemBg.drawRect(0, index * 25, 200, 25);
            itemBg.endFill();

            const itemText = new PIXI.Text(topic, new PIXI.TextStyle({
                fontFamily: 'Arial',
                fontSize: 12,
                fill: 0x333333
            }));
            itemText.x = 10;
            itemText.y = index * 25 + 6;

            // Make item interactive
            itemBg.interactive = true;
            itemBg.buttonMode = true;
            itemBg.on('click', () => this.selectTopic(topic));
            itemBg.on('pointerover', () => {
                itemBg.clear();
                itemBg.beginFill(0xf0f0f0);
                itemBg.lineStyle(1, 0xcccccc);
                itemBg.drawRect(0, index * 25, 200, 25);
                itemBg.endFill();
            });
            itemBg.on('pointerout', () => {
                itemBg.clear();
                itemBg.beginFill(0xffffff);
                itemBg.lineStyle(1, 0xcccccc);
                itemBg.drawRect(0, index * 25, 200, 25);
                itemBg.endFill();
            });

            this.topicDropdownMenu.addChild(itemBg);
            this.topicDropdownMenu.addChild(itemText);
        });

        this.producerContainer.addChild(this.topicDropdownMenu);
    }

    // Method to select a topic
    selectTopic(topic) {
        this.selectedTopic = topic;
        this.topicDropdownText.text = topic;

        // Close dropdown
        if (this.topicDropdownMenu) {
            this.producerContainer.removeChild(this.topicDropdownMenu);
            this.topicDropdownMenu = null;
        }

        // Trigger any topic selection logic here
        this.onTopicSelected(topic);
    }

    // Method to show add topic dialog
    showAddTopicDialog() {
        if (!this.producer.routerProducer) {
            return
        }
        const router = this.producer.routerProducer

        // Create a simple prompt dialog
        const newTopic = prompt('Enter new topic name:');
        const n_parts = +prompt('Enter number of partitions:');
        const replication = +prompt('Enter replication factor:');
        const json = { topic: newTopic, n_parts, replication }
        const str = JSON.stringify(json)
        this.sendCommand({ type: "create-topic", action: "create-topic", data: str, target: router })
    }

    addTopic(newTopic) {
        if (newTopic && newTopic.trim() && !this.availableTopics.includes(newTopic.trim())) {
            this.availableTopics.push(newTopic.trim());
            this.selectTopic(newTopic.trim());
        }
    }

    // Method called when a topic is selected
    onTopicSelected(topic) {
        console.log('Topic selected:', topic);
        Object.keys(this.producer.producers || {}).forEach(producer => {
            this.sendCommand({ type: "update-topic", action: "update-topic", data: topic, target: producer })
        })
    }

    sendCommand(command) {
        this.socket.sendMessage(command.type, command.data, command.target, command.action)
    }



    initConsumer() {
        this.consumerContainer = new PIXI.Container();
        this.createContainerConsumer()
        const ctr = new Consumer(this.app, this.consumerContainer)
        this.consumer = ctr
        this.app.stage.addChild(this.consumerContainer);
    }

    createContainerConsumer() {
        // Create bounding box
        const boundingBox = new PIXI.Graphics();
        boundingBox.lineStyle(2, 0x333333); // 2px thick, dark gray border
        boundingBox.beginFill(0xf8f8f8, 0.1); // Light gray fill with low opacity
        boundingBox.drawRoundedRect(0, 0, 800, 400, 10); // Adjust size as needed
        boundingBox.endFill();

        // Create title text
        const titleStyle = new PIXI.TextStyle({
            fontFamily: 'Arial',
            fontSize: 24,
            fontWeight: 'bold',
            fill: 0x333333,
            align: 'center'
        });

        const title = new PIXI.Text('Gofka Consumers', titleStyle);
        title.anchor.set(0.5, 0);
        title.x = boundingBox.width / 2;
        title.y = 10; // 10px from top of bounding box

        // Create topic controls container
        const topicControlsContainer = new PIXI.Container();
        topicControlsContainer.y = 45; // Below title

        const dropdownBg = createDropDownBg()
        const dropdownText = createDropDownText()
        const arrow = createDropDownArrow()


        // Make dropdown interactive
        dropdownBg.interactive = true;
        dropdownBg.buttonMode = true;
        dropdownBg.on('click', this.onTopicDropdownClickConsumer.bind(this));

        const { addTopicButton: addTopic, buttonText: addTopicText } = addButton("Add topic")
        addTopic.x = 350
        addTopicText.x = addTopic.x + 50
        addTopic.on('click', this.onAddTopicConsumer.bind(this));

        const { addTopicButton: removeTopic, buttonText: removeTopicText } = addButton("Remove topic")
        removeTopic.x = 500
        removeTopicText.x = removeTopic.x + 50
        removeTopic.on('click', this.onRemoveTopicConsumer.bind(this));

        const { addTopicButton: startMsg, buttonText: startMsgText } = addButton("Consume Msg")
        startMsg.x = 650
        startMsgText.x = startMsg.x + 50
        startMsg.on('click', this.onConsumeMsg.bind(this));

        const { addTopicButton: stopButton, buttonText: stopMsgText } = addButton("Stop Send Msg", 0x750000)
        stopButton.x = 350
        stopMsgText.x = stopButton.x + 50
        stopButton.on('click', this.onStopSendMsg.bind(this));
        startButton.visible = false
        stopMsgText.visible = false

        // Add all dropdown elements to the controls container
        topicControlsContainer.addChild(dropdownBg);
        topicControlsContainer.addChild(dropdownText);
        topicControlsContainer.addChild(arrow);

        topicControlsContainer.addChild(addTopic);
        topicControlsContainer.addChild(addTopicText);

        topicControlsContainer.addChild(removeTopic);
        topicControlsContainer.addChild(removeTopicText);

        topicControlsContainer.addChild(startMsg);
        topicControlsContainer.addChild(startMsgText);

        topicControlsContainer.addChild(stopButton);
        topicControlsContainer.addChild(stopMsgText);
        // Create inner container for brokers (with padding from title)
        const brokersInnerContainer = new PIXI.Container();
        brokersInnerContainer.y = 50; // Space for title + padding

        // Add everything to the main container
        this.consumerContainer.addChild(boundingBox);
        this.consumerContainer.addChild(title);
        this.consumerContainer.addChild(topicControlsContainer);
        this.consumerContainer.addChild(brokersInnerContainer);
        this.consumerContainer.msgButton = { button: startButton, text: startMsgText }
        this.consumerContainer.stopMsgButton = { button: stopButton, text: stopMsgText }

        // Position the entire container
        //this.consumerContainer.x = 50; // Adjust position as needed
        this.consumerContainer.x = this.app.screen.width / 2 + 20;
        this.consumerContainer.y = 550;

        // Store references for later use
        this.topicDropdownTextConsumer = dropdownText;
        this.topicDropdownBgConsumer = dropdownBg;
    }

    onTopicDropdownClickConsumer() {
        this.showTopicDropdownConsumer();
    }

    showTopicDropdownConsumer() {
        // Remove existing dropdown if it exists
        if (this.topicDropdownMenuConsumer) {
            this.consumerContainer.removeChild(this.topicDropdownMenuConsumer);
            this.topicDropdownMenuConsumer = null;
            return;
        }

        // Create dropdown menu
        this.topicDropdownMenuConsumer = new PIXI.Container();
        this.topicDropdownMenuConsumer.x = this.topicDropdownBgConsumer.x;
        this.topicDropdownMenuConsumer.y = this.topicDropdownBgConsumer.y + 35;

        // Create dropdown items
        this.availableTopics.forEach((topic, index) => {
            const itemBg = new PIXI.Graphics();
            itemBg.beginFill(0xffffff);
            itemBg.lineStyle(1, 0xcccccc);
            itemBg.drawRect(0, index * 25, 200, 25);
            itemBg.endFill();

            const itemText = new PIXI.Text(topic, new PIXI.TextStyle({
                fontFamily: 'Arial',
                fontSize: 12,
                fill: 0x333333
            }));
            itemText.x = 10;
            itemText.y = index * 25 + 6;

            // Make item interactive
            itemBg.interactive = true;
            itemBg.buttonMode = true;
            itemBg.on('click', () => this.selectTopicConsumer(topic));
            itemBg.on('pointerover', () => {
                itemBg.clear();
                itemBg.beginFill(0xf0f0f0);
                itemBg.lineStyle(1, 0xcccccc);
                itemBg.drawRect(0, index * 25, 200, 25);
                itemBg.endFill();
            });
            itemBg.on('pointerout', () => {
                itemBg.clear();
                itemBg.beginFill(0xffffff);
                itemBg.lineStyle(1, 0xcccccc);
                itemBg.drawRect(0, index * 25, 200, 25);
                itemBg.endFill();
            });

            this.topicDropdownMenuConsumer.addChild(itemBg);
            this.topicDropdownMenuConsumer.addChild(itemText);
        });

        this.consumerContainer.addChild(this.topicDropdownMenuConsumer)
    }

    selectTopicConsumer(topic) {
        this.selectedTopicConsumer = topic;
        this.topicDropdownTextConsumer.text = topic;

        // Close dropdown
        if (this.topicDropdownMenuConsumer) {
            this.consumerContainer.removeChild(this.topicDropdownMenuConsumer);
            this.topicDropdownMenuConsumer = null;
        } else {
            console.log("cannot find consumer topic dropdown", this.topicDropdownMenuConsumer)
        }

        // Trigger any topic selection logic here
        this.onTopicSelectedConsumer(topic);
    }

    onTopicSelectedConsumer(topic) {
        console.log('Topic selected:', topic);
        this.consumerSelectedTopic = topic
    }

    onConsumeMsg() {
        Object.keys(this.consumer.consumers || {}).forEach(consumer => {
            this.sendCommand({ type: "consume-message", action: "consume-message", data: "", target: consumer })
        })
    }

    onAddTopicConsumer() {
        const topic = this.consumerSelectedTopic
        if (topic) {
            Object.keys(this.consumer.consumers || {}).forEach(consumer => {
                this.sendCommand({ type: "add-topic", action: "add-topic", data: topic, target: consumer })
            })
        }
    }

    onRemoveTopicConsumer() {
        const topic = this.consumerSelectedTopic
        if (topic) {
            Object.keys(this.consumer.consumers || {}).forEach(consumer => {
                this.sendCommand({ type: "remove-topic", action: "remove-topic", data: topic, target: consumer })
            })
        }
    }

    onMsgReceived(message) {
        if (message.action == "error") {
            this.handleError(message)
            return
        }
        switch (message.node_type) {
            case "controller":
                this.controller.handleMessage(message)
                break;
            case "broker":
                this.broker.handleMessage(message)
                break;
            case "consumer":
                this.consumer.handleMessage(message)
                break;
            case "producer":
                this.producer.handleMessage(message)
                break;
            default:
                console.log("cannot find router for node: ", message)
        }
    }

    handleError(message) {
        console.log("found error: ", message)
        const msg = atob(message.data)
        console.log("err message: ", msg)
        alert(msg)
    }
}

function addButton(text, color = 0x4CAF50) {
    const addTopicButton = new PIXI.Graphics();
    addTopicButton.beginFill(color); // Green background
    addTopicButton.lineStyle(1, color);
    addTopicButton.drawRoundedRect(0, 0, 100, 30, 5);
    addTopicButton.endFill();
    addTopicButton.x = 240; // Next to dropdown

    // Add button text
    const buttonTextStyle = new PIXI.TextStyle({
        fontFamily: 'Arial',
        fontSize: 14,
        fill: 0xffffff,
        align: 'center'
    });
    const buttonText = new PIXI.Text(text, buttonTextStyle);
    buttonText.anchor.set(0.5, 0.5);
    buttonText.x = addTopicButton.x + 50;
    buttonText.y = 15;

    // Make button interactive
    addTopicButton.interactive = true;
    addTopicButton.buttonMode = true;
    //addTopicButton.on('click', this.onAddTopicClick.bind(this));
    addTopicButton.on('pointerover', () => {
        addTopicButton.clear();
        addTopicButton.beginFill(0x5cbf60); // Lighter green on hover
        addTopicButton.lineStyle(1, 0x45a049);
        addTopicButton.drawRoundedRect(0, 0, 100, 30, 5);
        addTopicButton.endFill();
    });
    addTopicButton.on('pointerout', () => {
        addTopicButton.clear();
        addTopicButton.beginFill(color); // Original green
        addTopicButton.lineStyle(1, color);
        addTopicButton.drawRoundedRect(0, 0, 100, 30, 5);
        addTopicButton.endFill();
    });
    return { addTopicButton, buttonText }
}

function createControls() {
    // Create topic controls container
    const topicControlsContainer = new PIXI.Container();
    topicControlsContainer.y = 45; // Below title

    const dropdownBg = createDropDownBg()
    const dropdownText = createDropDownText()
    const dorpdownArrow = createDropDownArrow()
}

function createDropDownBg() {
    // Create topic dropdown background
    const dropdownBg = new PIXI.Graphics();
    dropdownBg.beginFill(0xffffff);
    dropdownBg.lineStyle(1, 0xcccccc);
    dropdownBg.drawRoundedRect(0, 0, 200, 30, 5);
    dropdownBg.endFill();
    dropdownBg.x = 20;
    return dropdownBg
}

function createDropDownText() {
    // Create dropdown text
    const dropdownStyle = new PIXI.TextStyle({
        fontFamily: 'Arial',
        fontSize: 14,
        fill: 0x333333,
        align: 'left'
    });
    const dropdownText = new PIXI.Text('Select Topic...', dropdownStyle);
    dropdownText.x = 30;
    dropdownText.y = 8;
    return dropdownText
}

function createDropDownArrow() {
    // Create dropdown arrow
    const arrow = new PIXI.Graphics();
    arrow.beginFill(0x666666);
    arrow.moveTo(0, 0);
    arrow.lineTo(8, 0);
    arrow.lineTo(4, 6);
    arrow.lineTo(0, 0);
    arrow.endFill();
    arrow.x = 195;
    arrow.y = 12;
    return arrow
}

window.addEventListener('load', () => {
    new KafkaVisualizer();
});
