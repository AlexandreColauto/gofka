export default class Broker {
    constructor(app, container) {
        this.app = app
        this.container = container
        this.n_brokers = 0
        this.brokers = {}
    }

    handleMessage(message) {
        switch (message.action) {
            case "alive":
                this.addBroker(message.target)
                break;
            case "metadata":
                this.handleMetadata(message)
                break;
            case "fenced":
                this.fenced(message)
                break;
            case "fenced-removed":
                this.removeFenced(message)
                break;

        }
    }

    addBroker(id) {
        console.log("creating new broker")
        if (this.brokers[id]) {
            console.log("broker already exists")
            return
        }
        //FIELDS TO ADD, THIS WILL COME FROM METADATA LATER
        const leaderOf = []
        const followerOf = []

        const brokerContainer = new PIXI.Container()

        // Load the SVG texture and create a sprite
        const texture = PIXI.Texture.from('./static/broker.webp');
        const brokerSprite = new PIXI.Sprite(texture);

        brokerContainer.serverImg = brokerSprite

        // Set the anchor to center the sprite
        brokerSprite.anchor.set(0.5);
        brokerSprite.width = 80;
        brokerSprite.height = 80;
        brokerSprite.x = 0; // Center in container
        brokerSprite.y = 0;

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

        // Create expandable info container below the broker
        const infoContainer = new PIXI.Container();
        infoContainer.y = 60; // Below the broker icon

        // Create collapsible sections for leader/follower info
        const leaderSection = this.createPartitionSection("LEADER", leaderOf, 0x27ae60, 0);
        const followerSection = this.createPartitionSection("FOLLOWER", followerOf, 0x3498db, 120);

        infoContainer.addChild(leaderSection.container);
        infoContainer.addChild(followerSection.container);

        // Status indicator (circle)
        const statusIndicator = new PIXI.Graphics();
        statusIndicator.beginFill(0x27ae60); // Green for healthy
        statusIndicator.drawCircle(35, -35, 8); // Top right corner
        statusIndicator.endFill();

        // Add all elements to the broker container
        brokerContainer.addChild(brokerSprite);
        brokerContainer.addChild(idLabel);
        brokerContainer.addChild(infoContainer);
        brokerContainer.addChild(statusIndicator);

        // Calculate position in grid layout
        const brokersPerRow = 3; // Reduced to 2 per row due to wider containers
        const brokerSpacingX = 250;
        const brokerSpacingY = 250;
        const startX = 125;
        const startY = 50;

        const row = Math.floor(this.n_brokers / brokersPerRow);
        const col = this.n_brokers % brokersPerRow;

        brokerContainer.x = startX + (col * brokerSpacingX);
        brokerContainer.y = startY + (row * brokerSpacingY);

        // Make interactive
        brokerContainer.interactive = true;
        brokerContainer.buttonMode = true;
        brokerContainer.on('pointerdown', () => {
            this.fenceFunc(brokerContainer.nodeId)
        });

        // Store references for later updates
        brokerContainer.nodeId = id;
        brokerContainer.leaderSection = leaderSection;
        brokerContainer.followerSection = followerSection;
        brokerContainer.statusIndicator = statusIndicator;
        brokerContainer.leaderOf = leaderOf;
        brokerContainer.followerOf = followerOf;
        brokerContainer.expanded = true;

        this.brokers[id] = brokerContainer;

        // Add to the brokers inner container
        const brokersInnerContainer = this.container.children[2]; // Fourth child is the inner container
        brokersInnerContainer.addChild(brokerContainer);

        this.n_brokers++;
    }

    // Helper method to create partition sections
    createPartitionSection(title, partitions, color, yOffset) {
        const container = new PIXI.Container();
        container.x = yOffset;

        // Section header
        const headerStyle = new PIXI.TextStyle({
            fontFamily: 'Arial',
            fontSize: 11,
            fontWeight: 'bold',
            fill: color,
            align: 'left'
        });

        const header = new PIXI.Text(`${title} (${partitions.length})`, headerStyle);
        header.anchor.set(0, 0.5);
        header.x = -100;
        header.y = 0;

        // Partition details container (initially empty, will be populated on expand)
        const detailsContainer = new PIXI.Container();
        detailsContainer.y = 15;
        detailsContainer.visible = true;

        container.addChild(header);
        container.addChild(detailsContainer);

        return {
            container: container,
            header: header,
            detailsContainer: detailsContainer,
            partitions: partitions,
            color: color,
            title: title
        };
    }

    populatePartitionDetails(section) {
        // Clear existing details
        section.detailsContainer.removeChildren();

        const detailStyle = new PIXI.TextStyle({
            fontFamily: 'Arial',
            fontSize: 9,
            fill: 0x555555,
            align: 'left'
        });

        const headerStyle = new PIXI.TextStyle({
            fontFamily: 'Arial',
            fontSize: 8,
            fill: 0x777777,
            fontStyle: 'italic'
        });

        section.partitions.forEach((partition, index) => {
            const yPos = index * 24; // Increased spacing for more info

            // Main partition name
            const partitionName = `• ${partition.displayName}`;
            const detail = new PIXI.Text(partitionName, detailStyle);
            detail.anchor.set(0, 0);
            detail.x = -95;
            detail.y = yPos;

            // Additional info (replicas, ISR, epoch)
            const replicaInfo = `  Height: ${partition.offset || "N/A"}`;
            //const isrInfo = `  ISR: [${partition.isr.join(', ')}] E:${partition.epoch}`;

            const replicaDetail = new PIXI.Text(replicaInfo, headerStyle);
            replicaDetail.anchor.set(0, 0);
            replicaDetail.x = -95;
            replicaDetail.y = yPos + 10;

            //const isrDetail = new PIXI.Text(isrInfo, headerStyle);
            //isrDetail.anchor.set(0, 0);
            //isrDetail.x = -95;
            //isrDetail.y = yPos + 18;

            section.detailsContainer.addChild(detail);
            section.detailsContainer.addChild(replicaDetail);
            //section.detailsContainer.addChild(isrDetail);
        });
    }
    toggleBrokerDetails(brokerContainer) {
        const isExpanded = brokerContainer.expanded;

        if (!isExpanded) {
            // Expand: show partition details
            this.populatePartitionDetails(brokerContainer.leaderSection);
            this.populatePartitionDetails(brokerContainer.followerSection);

            brokerContainer.leaderSection.detailsContainer.visible = true;
            brokerContainer.followerSection.detailsContainer.visible = true;
            brokerContainer.expanded = true;

            console.log(`Expanded Broker ${brokerContainer.nodeId}`);
        } else {
            // Collapse: hide partition details
            brokerContainer.leaderSection.detailsContainer.visible = false;
            brokerContainer.followerSection.detailsContainer.visible = false;
            brokerContainer.expanded = false;

            console.log(`Collapsed Broker ${brokerContainer.nodeId}`);
        }
    }

    updateBrokerMetadata(id, metadata) {
        const broker = this.brokers[id];
        if (!broker) {
            console.log("Broker not found");
            return;
        }

        // Update the stored data
        broker.leaderOf = metadata.leaderOf || [];
        broker.followerOf = metadata.followerOf || [];

        // Update sections with detailed partition info
        broker.leaderSection.partitions = broker.leaderOf;
        broker.followerSection.partitions = broker.followerOf;

        // Update headers with counts and additional info
        broker.leaderSection.header.text = `LEADER (${broker.leaderOf.length})`;
        broker.followerSection.header.text = `FOLLOWER (${broker.followerOf.length})`;

        // Update broker ID label with additional info
        const idLabel = broker.children.find(child => child.text);
        if (idLabel) {
            idLabel.text = `${id} - (${metadata.clusterLastIndex})`;
        }

        // If expanded, refresh the details with enhanced information
        if (broker.expanded) {
            this.populatePartitionDetails(broker.leaderSection);
            this.populatePartitionDetails(broker.followerSection);
        }

        // Update status indicator based on health and additional factors
        broker.statusIndicator.clear();
        let statusColor = 0xe74c3c; // Red (unhealthy)

        if (metadata.isHealthy) {
            // Green for healthy, yellow for some issues
            if (metadata.stats.inSyncReplicas.percentage >= 75 || metadata.stats.inSyncReplicas.total === 0) {
                statusColor = 0x27ae60; // Green - healthy
            } else if (metadata.stats.inSyncReplicas.percentage >= 50) {
                statusColor = 0xf39c12; // Orange - some issues
            } else {
                //console.log("ISSUES WITH broker: ", metadata)
                statusColor = 0xe67e22; // Red-orange - major issues
            }
        } else {
            console.log("NOT ALIVE -ISSUES WITH broker: ", metadata)
        }

        broker.statusIndicator.beginFill(statusColor);
        broker.statusIndicator.drawCircle(35, -35, 8);
        broker.statusIndicator.endFill();

    }


    handleMetadata(message) {

        const id = message.target
        if (this.brokers[id]) {
            const metadata = decodeBase64ToJSON(message.data)
            this.brokers[id].metadata = metadata
            const payload = this.convertMetadata(metadata, id)

            this.updateBrokerMetadata(id, payload);
        } else {
            console.log("cannot find broker with id", id)
        }
    }

    convertMetadata(metadata, brokerId) {
        //Convert
        const leaderOf = [];
        const followerOf = [];
        const topics = new Set();

        Object.values(metadata.topics || {}).forEach(topic => {
            const topicName = topic.name;
            this.addAvailableTopic(topicName)

            // Process each partition in the topic
            Object.entries(topic.partitions || {}).forEach(([partitionId, partition]) => {
                const partitionInfo = {
                    topic: topicName,
                    partition: parseInt(partitionId),
                    displayName: `${topicName}-${partitionId}`,
                    replicas: partition.replicas || [],
                    isr: partition.isr || [],
                    epoch: partition.epoch || 0,
                    offset: partition.offset || 0,
                    leaderAddress: partition.leader_address
                };

                // Check if this broker is the leader
                if (partition.leader === brokerId) {
                    leaderOf.push(partitionInfo);
                    topics.add(topicName);
                }

                // Check if this broker is a follower (in replicas but not leader)
                if (partition.replicas && partition.replicas.includes(brokerId) && partition.leader !== brokerId) {
                    partitionInfo.offset = partition.offset
                    followerOf.push(partitionInfo);
                    topics.add(topicName);
                }
            });
        });
        const brokerInfo = metadata.brokers?.[brokerId] || {};

        // Calculate last seen time in a human-readable format
        const lastSeenMs = brokerInfo.last_seen ?
            (brokerInfo.last_seen.seconds * 1000) + Math.floor(brokerInfo.last_seen.nanos / 1000000) :
            Date.now();
        const lastSeenDate = new Date(lastSeenMs);
        //const timeSinceLastSeen = Date.now() - lastSeenMs;

        // Determine if broker is healthy (alive and recently seen)
        const isHealthy = brokerInfo.alive === true

        return {
            // Partition leadership info
            leaderOf: leaderOf,
            followerOf: followerOf,

            // Topic involvement
            n_topics: topics.size,
            topics: Array.from(topics),

            // Broker health and status
            isHealthy: isHealthy,
            alive: brokerInfo.alive || false,
            address: brokerInfo.address || 'unknown',
            lastSeen: lastSeenDate.toLocaleTimeString(),
            lastSeenMs: lastSeenMs,

            // Cluster information
            totalBrokers: Object.keys(metadata.brokers || {}).length,
            totalTopics: Object.keys(metadata.topics || {}).length,
            clusterLastIndex: metadata.last_index || 0,

            // Detailed partition statistics
            stats: {
                totalPartitionsAsLeader: leaderOf.length,
                totalPartitionsAsFollower: followerOf.length,
                totalPartitionsInvolved: leaderOf.length + followerOf.length,
                inSyncReplicas: this.calculateInSyncStats(leaderOf)
            }
        };

    }

    calculateInSyncStats(leaderOf) {
        if (leaderOf.length === 0) return { total: 0, inSync: 0, percentage: 0 };

        let totalReplicas = 0;
        let inSyncReplicas = 0;

        leaderOf.forEach(partition => {
            const replicaCount = partition.replicas ? partition.replicas.length : 0;
            const isrCount = partition.isr ? partition.isr.length : 0;

            totalReplicas += replicaCount;
            inSyncReplicas += isrCount;
        });

        return {
            total: totalReplicas,
            inSync: inSyncReplicas,
            percentage: totalReplicas > 0 ? Math.round((inSyncReplicas / totalReplicas) * 100) : 0
        };
    }

    fenced(message) {
        console.log("fencing ", message)
        if (!this.brokers[message.target]) {
            console.log("cannot find controller", message.target)
            return
        }
        const container = this.brokers[message.target]
        const colorMatrix = new PIXI.filters.ColorMatrixFilter();
        colorMatrix.desaturate(); // Removes all color saturation
        container.serverImg.filters = [colorMatrix];
        container.serverImg.alpha = 0.5


    }

    removeFenced(message) {
        console.log("remove fencing ", message)
        if (!this.brokers[message.target]) {
            console.log("cannot find controller", message.target)
            return
        }
        const container = this.brokers[message.target]
        container.serverImg.filters = [];
        container.serverImg.alpha = 1.0
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
