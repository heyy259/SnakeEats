class SubscriptionManager {
    constructor(wsClient) {
        this.wsClient = wsClient;
        this.subscriptions = new Map(); // subscriptionId -> { streamName, fromPosition }
        this.notificationCallbacks = [];

        // Register notification handler
        this.wsClient.onNotification((notification) => {
            if (notification.method === 'stream.on_change') {
                this.handleNotification(notification.params);
            }
        });
    }

    // Subscribe to a stream
    async subscribe(streamName, fromPosition = null) {
        const params = { name: streamName };
        if (fromPosition !== null) {
            params.from_position = fromPosition;
        }

        const result = await this.wsClient.send('stream.subscribe', params);
        const subscriptionId = result.subscription_id;

        this.subscriptions.set(subscriptionId, {
            streamName,
            fromPosition,
            text: result.text,
            version: result.version
        });

        return subscriptionId;
    }

    // Unsubscribe
    async unsubscribe(subscriptionId) {
        await this.wsClient.send('stream.unsubscribe', { subscription_id: subscriptionId });
        this.subscriptions.delete(subscriptionId);
    }

    // Handle notification
    handleNotification(params) {
        const { subscription_id, name, change, new_size } = params;

        // Update subscription info
        if (this.subscriptions.has(subscription_id)) {
            const sub = this.subscriptions.get(subscription_id);

            // If it is a rename event, update the local streamName
            if (change && change.type === 'rename') {
                sub.streamName = change.new_name;
            }

            sub.lastNotification = {
                change,
                newSize: new_size,
                timestamp: new Date()
            };
        }

        // Trigger callbacks
        this.notificationCallbacks.forEach(cb => cb({
            subscriptionId: subscription_id,
            streamName: name,
            change,
            newSize: new_size,
            timestamp: new Date()
        }));
    }

    // Get active subscriptions
    getActiveSubscriptions() {
        return Array.from(this.subscriptions.entries()).map(([id, sub]) => ({
            id,
            ...sub
        }));
    }

    // Check whether a stream is subscribed
    isSubscribed(streamName) {
        for (const sub of this.subscriptions.values()) {
            if (sub.streamName === streamName) return true;
        }
        return false;
    }

    // Get the subscription ID of a stream
    getSubscriptionId(streamName) {
        for (const [id, sub] of this.subscriptions.entries()) {
            if (sub.streamName === streamName) return id;
        }
        return null;
    }

    // Clear all subscriptions
    async clearAll() {
        const promises = [];
        this.subscriptions.forEach((sub, id) => {
            promises.push(this.unsubscribe(id));
        });
        await Promise.all(promises);
    }

    // Register notification callback
    onNotification(callback) {
        this.notificationCallbacks.push(callback);
    }
}

// Export (global)
window.SubscriptionManager = SubscriptionManager;
