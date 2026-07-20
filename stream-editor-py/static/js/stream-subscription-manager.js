/**
 * 流订阅管理器
 * 
 * 管理到 Flask 后端的 WebSocket 连接，处理流订阅和通知。
 */
class StreamSubscriptionManager {
    constructor() {
        this.ws = null;
        this.subscriptions = new Map(); // streamName -> subscriptionId
        this.callbacks = new Map(); // streamName -> callback
        this.reconnectAttempts = 0;
        this.maxReconnectAttempts = 5;
        this.reconnectDelay = 1000;
        this.systemId = null;
        this.port = null;
        this._connected = false;
    }

    /**
     * 连接到 Flask 后端的 WebSocket 端点
     * @param {number} port - Flask 服务器端口
     * @param {string} systemId - 系统 ID
     */
    connect(port, systemId) {
        this.port = port;
        this.systemId = systemId;

        if (this.ws) {
            try { this.ws.close(); } catch (e) { /* ignore */ }
            this.ws = null;
        }

        const wsPort = window.WEBSOCKET_PORT || (port + 1000);
        const url = `ws://localhost:${wsPort}`;
        
        console.log(`连接到 WebSocket 服务器: ${url}`);
        
        try {
            this.ws = new WebSocket(url);

            this.ws.onopen = () => {
                console.log('WebSocket 连接已建立');
                this._connected = true;
                this.reconnectAttempts = 0;
                this.resubscribeAll();
                state.emit('wsReconnected');
            };

            this.ws.onmessage = (event) => {
                try {
                    const data = JSON.parse(event.data);
                    this.handleMessage(data);
                } catch (e) {
                    console.error('解析消息失败:', e);
                }
            };

            this.ws.onclose = (event) => {
                console.log('WebSocket 连接关闭:', event.code, event.reason);
                this._connected = false;
                this.scheduleReconnect();
            };

            this.ws.onerror = (error) => {
                console.error('WebSocket 错误:', error);
            };
        } catch (e) {
            console.error('创建 WebSocket 连接失败:', e);
            this.scheduleReconnect();
        }
    }

    /**
     * 处理接收到的消息
     * @param {Object} data - 消息数据
     */
    handleMessage(data) {
        switch (data.type) {
            case 'connected':
                console.log('WebSocket 连接成功，客户端 ID:', data.client_id);
                break;
            case 'stream.on_change':
                this.handleStreamChange(data);
                break;
            case 'subscribed':
                console.log(`订阅成功: ${data.stream_name} -> ${data.subscription_id}`);
                this.subscriptions.set(data.stream_name, data.subscription_id);
                break;
            case 'unsubscribed':
                console.log(`取消订阅成功: ${data.stream_name}`);
                this.subscriptions.delete(data.stream_name);
                break;
            case 'subscriptions':
                console.log('当前订阅列表:', data.subscriptions);
                break;
            case 'error':
                console.error('订阅错误:', data.message);
                break;
            case 'tool.on_registry_change':
                // 工具注册/注销 → 自动刷新工具列表
                console.log('[Notification] Tool registry changed:', data.change);
                state.emit('toolRegistryChanged', data.change);
                break;
            case 'tool.on_process':
                // 后台进程生命周期（启动/退出）→ 独立通道
                console.log('[Notification] Process event:', data.change?.type, data.change?.process_name);
                this._handleProcessEvent(data);
                break;
            case 'scan.on_change':
                // 扫描事件 → 分发到具体处理器
                console.log('[Notification] Scan event:', data.change?.type, data.stream_name);
                this._handleScanEvent(data);
                break;
            default:
                console.warn('未知消息类型:', data.type);
        }
    }

    /**
     * 订阅流
     * @param {string} systemId - 系统 ID
     * @param {string} streamName - 流名称
     * @param {Function} callback - 变化回调函数
     */
    subscribe(systemId, streamName, callback) {
        this.callbacks.set(streamName, callback);
        this.systemId = systemId;

        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(JSON.stringify({
                action: 'subscribe',
                system_id: systemId,
                stream_name: streamName
            }));
        } else {
            console.warn('WebSocket 未连接，将在连接后订阅');
        }
    }

    /**
     * 取消订阅流
     * @param {string} systemId - 系统 ID
     * @param {string} streamName - 流名称
     */
    unsubscribe(systemId, streamName) {
        this.callbacks.delete(streamName);

        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(JSON.stringify({
                action: 'unsubscribe',
                system_id: systemId,
                stream_name: streamName
            }));
        }
    }

    /**
     * 处理流变化通知
     * @param {Object} data - 变化数据
     */
    handleStreamChange(data) {
        const callback = this.callbacks.get(data.stream_name);
        if (callback) {
            try {
                callback(data);
            } catch (e) {
                console.error('执行回调失败:', e);
            }
        }
    }

    /**
     * 处理扫描事件通知
     * @param {Object} data - { type, stream_name, change }
     */
    _handleScanEvent(data) {
        const change = data.change;
        if (!change) return;

        switch (change.type) {
            case 'state':
                // 扫描状态变更 (idle/scanning/paused)
                // 注意：使用 'scanWSStateChange' 而非 'scanStateChanged'，
                // 避免与 state.setScanState() 的事件冲突导致无限递归
                state.emit('scanWSStateChange', {
                    status: change.new_state,
                    oldStatus: change.old_state,
                });
                break;

            case 'position':
                // 位置更新 (300ms 节流，替代轮询)
                state.emit('scanPositionUpdated', {
                    position: change.position,
                });
                break;

            case 'marker_found':
                state.emit('scanMarkerFound', change);
                break;

            case 'marker_executed':
                state.emit('scanMarkerExecuted', change);
                break;

            case 'scan_complete':
                // 扫描完成
                state.emit('scanCompleted', change);
                break;

            case 'error':
                // 扫描错误
                state.emit('scanError', change);
                break;

            default:
                console.warn('[Notification] Unknown scan event type:', change.type);
        }
    }

    _handleProcessEvent(data) {
        const change = data.change;
        if (!change) return;

        switch (change.type) {
            case 'process_started':
                state.emit('processStarted', change);
                break;
            case 'process_exited':
                state.emit('processExited', change);
                break;
            default:
                console.warn('[Notification] Unknown process event type:', change.type);
        }
    }

    /**
     * 重新订阅所有流（用于重连后）
     */
    resubscribeAll() {
        if (!this.systemId) {
            return;
        }

        console.log(`重新订阅 ${this.callbacks.size} 个流`);
        
        for (const [streamName, callback] of this.callbacks) {
            this.subscribe(this.systemId, streamName, callback);
        }
    }

    /**
     * 计划重连
     */
    scheduleReconnect() {
        if (this.reconnectAttempts < this.maxReconnectAttempts) {
            this.reconnectAttempts++;
            const delay = this.reconnectDelay * Math.pow(2, this.reconnectAttempts - 1);
            
            console.log(`${delay}ms 后重连 (尝试 ${this.reconnectAttempts}/${this.maxReconnectAttempts})`);
            
            setTimeout(() => {
                if (this.port && this.systemId) {
                    this.connect(this.port, this.systemId);
                }
            }, delay);
        } else {
            console.error('达到最大重连次数，停止重连');
        }
    }

    /**
     * 断开连接
     */
    disconnect() {
        // Prevent pending reconnect timers from firing after disconnect
        this.port = null;
        this.systemId = null;
        this.reconnectAttempts = this.maxReconnectAttempts;

        if (this.ws) {
            this.ws.close();
            this.ws = null;
        }
        this._connected = false;
        this.subscriptions.clear();
        this.callbacks.clear();
    }

    /**
     * 列出当前订阅
     */
    listSubscriptions() {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(JSON.stringify({
                action: 'list_subscriptions'
            }));
        }
    }

    /**
     * 是否已连接
     * @returns {boolean}
     */
    isConnected() {
        return this._connected && this.ws && this.ws.readyState === WebSocket.OPEN;
    }

    /**
     * 获取订阅数量
     * @returns {number}
     */
    getSubscriptionCount() {
        return this.subscriptions.size;
    }

    /**
     * 获取所有订阅的流名称
     * @returns {string[]}
     */
    getSubscribedStreams() {
        return Array.from(this.subscriptions.keys());
    }
}

// 导出（全局）
window.StreamSubscriptionManager = StreamSubscriptionManager;
