class SubscriptionManager {
    constructor(wsClient) {
        this.wsClient = wsClient;
        this.subscriptions = new Map(); // subscriptionId -> { streamName, fromPosition }
        this.notificationCallbacks = [];

        // 注册通知处理
        this.wsClient.onNotification((notification) => {
            if (notification.method === 'stream.on_change') {
                this.handleNotification(notification.params);
            }
        });
    }

    // 订阅流
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

    // 取消订阅
    async unsubscribe(subscriptionId) {
        await this.wsClient.send('stream.unsubscribe', { subscription_id: subscriptionId });
        this.subscriptions.delete(subscriptionId);
    }

    // 处理通知
    handleNotification(params) {
        const { subscription_id, name, change, new_size } = params;

        // 更新订阅信息
        if (this.subscriptions.has(subscription_id)) {
            const sub = this.subscriptions.get(subscription_id);

            // 如果是 rename 事件，更新本地 streamName
            if (change && change.type === 'rename') {
                sub.streamName = change.new_name;
            }

            sub.lastNotification = {
                change,
                newSize: new_size,
                timestamp: new Date()
            };
        }

        // 触发回调
        this.notificationCallbacks.forEach(cb => cb({
            subscriptionId: subscription_id,
            streamName: name,
            change,
            newSize: new_size,
            timestamp: new Date()
        }));
    }

    // 获取活跃订阅
    getActiveSubscriptions() {
        return Array.from(this.subscriptions.entries()).map(([id, sub]) => ({
            id,
            ...sub
        }));
    }

    // 检查某流是否已订阅
    isSubscribed(streamName) {
        for (const sub of this.subscriptions.values()) {
            if (sub.streamName === streamName) return true;
        }
        return false;
    }

    // 获取某流的订阅ID
    getSubscriptionId(streamName) {
        for (const [id, sub] of this.subscriptions.entries()) {
            if (sub.streamName === streamName) return id;
        }
        return null;
    }

    // 清空所有订阅
    async clearAll() {
        const promises = [];
        this.subscriptions.forEach((sub, id) => {
            promises.push(this.unsubscribe(id));
        });
        await Promise.all(promises);
    }

    // 注册通知回调
    onNotification(callback) {
        this.notificationCallbacks.push(callback);
    }
}

// 导出（全局）
window.SubscriptionManager = SubscriptionManager;
