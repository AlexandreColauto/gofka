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

        // Create topic dropdown background
        const dropdownBg = new PIXI.Graphics();
        dropdownBg.beginFill(0xffffff);
        dropdownBg.lineStyle(1, 0xcccccc);
        dropdownBg.drawRoundedRect(0, 0, 200, 30, 5);
        dropdownBg.endFill();
        dropdownBg.x = 20;

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

        // Make dropdown interactive
        dropdownBg.interactive = true;
        dropdownBg.buttonMode = true;
        dropdownBg.on('click', this.onTopicDropdownClick.bind(this));

        // Create "Add Topic" button
        const addTopicButton = new PIXI.Graphics();
        addTopicButton.beginFill(0x4CAF50); // Green background
        addTopicButton.lineStyle(1, 0x45a049);
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
        const buttonText = new PIXI.Text('Add Topic', buttonTextStyle);
        buttonText.anchor.set(0.5, 0.5);
        buttonText.x = addTopicButton.x + 50;
        buttonText.y = 15;

        // Make button interactive
        addTopicButton.interactive = true;
        addTopicButton.buttonMode = true;
        addTopicButton.on('click', this.onAddTopicClick.bind(this));
        addTopicButton.on('pointerover', () => {
            addTopicButton.clear();
            addTopicButton.beginFill(0x5cbf60); // Lighter green on hover
            addTopicButton.lineStyle(1, 0x45a049);
            addTopicButton.drawRoundedRect(0, 0, 100, 30, 5);
            addTopicButton.endFill();
        });
        addTopicButton.on('pointerout', () => {
            addTopicButton.clear();
            addTopicButton.beginFill(0x4CAF50); // Original green
            addTopicButton.lineStyle(1, 0x45a049);
            addTopicButton.drawRoundedRect(0, 0, 100, 30, 5);
            addTopicButton.endFill();
        });

        // Add all dropdown elements to the controls container
        topicControlsContainer.addChild(dropdownBg);
        topicControlsContainer.addChild(dropdownText);
        topicControlsContainer.addChild(arrow);
        topicControlsContainer.addChild(addTopicButton);
        topicControlsContainer.addChild(buttonText);

        // Create inner container for brokers (with padding from title and controls)
        const brokersInnerContainer = new PIXI.Container();
        brokersInnerContainer.y = 90; // Space for title + controls + padding

        // Add everything to the main container
        this.producerContainer.addChild(boundingBox);
        this.producerContainer.addChild(title);
        this.producerContainer.addChild(topicControlsContainer);
        this.producerContainer.addChild(brokersInnerContainer);

        // Position the entire container
        this.producerContainer.x = 50; // Adjust position as needed
        this.producerContainer.y = 550;

        // Store references for later use
        this.topicDropdownText = dropdownText;
        this.topicDropdownBg = dropdownBg;
        this.selectedTopic = null;
        this.availableTopics = ['user-events', 'order-updates', 'notifications']; // Example topics
    }

    // Handler for dropdown click
    onTopicDropdownClick() {
        this.showTopicDropdown();
    }

    // Handler for add topic button click
    onAddTopicClick() {
        this.showAddTopicDialog();
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
        // Create a simple prompt dialog
        const newTopic = prompt('Enter new topic name:');
        this.sendCommand({ type: "create-topic", data: newTopic })
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
        // Add your topic selection logic here
        // This could update the producers, filter data, etc.
    }

    sendCommand(command) {
        this.socket.sendMessage(command.type, command.data)
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

        // Create inner container for brokers (with padding from title)
        const brokersInnerContainer = new PIXI.Container();
        brokersInnerContainer.y = 50; // Space for title + padding

        // Add everything to the main container
        this.consumerContainer.addChild(boundingBox);
        this.consumerContainer.addChild(title);
        this.consumerContainer.addChild(brokersInnerContainer);

        // Position the entire container
        //this.consumerContainer.x = 50; // Adjust position as needed
        this.consumerContainer.x = this.app.screen.width / 2 + 20;
        this.consumerContainer.y = 550;
    }

    onMsgReceived(message) {
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
}

window.addEventListener('load', () => {
    new KafkaVisualizer();
});
