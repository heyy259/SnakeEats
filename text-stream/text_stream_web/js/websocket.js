class WebSocketClient {
    constructor(port, token, options = {}) {
        this.port = port;
        this.token = token;
        this.ws = null;
        this.requestId = 0;
        this.pendingRequests = new Map();
        this.connectionChangeCallbacks = [];
        this.errorCallbacks = [];
        this.notificationCallbacks = [];
        this.reconnectAttempts = 0;
        this.isConnecting = false;
        this.shouldReconnect = true;
        this.pendingSubscriptions = [];
        
        // 配置选项
        this.autoReconnect = options.autoReconnect !== false;
        this.maxReconnectAttempts = options.maxReconnectAttempts || 5;
        this.reconnectBaseDelay = options.reconnectBaseDelay || 1000;
        this.host = options.host || 'localhost';
        this.protocol = options.protocol || (window.location.protocol === 'https:' ? 'wss' : 'ws');
    }

    // 连接到WebSocket服务
    connect() {
        if (this.isConnecting || (this.ws && this.ws.readyState === WebSocket.OPEN)) {
            console.log('已连接或正在连接，跳过重复连接');
            return;
        }

        this.isConnecting = true;
        this.shouldReconnect = true;
        this.updateConnectionStatus('connecting');

        const url = `${this.protocol}://${this.host}:${this.port}`;
        console.log('连接到:', url);

        this.ws = new WebSocket(url);

        this.ws.onopen = () => {
            console.log('WebSocket连接已建立');
            this.reconnectAttempts = 0;
            this.isConnecting = false;
            
            // 如果设置了token，发送认证请求；否则直接标记已连接
            if (this.token) {
                this.authenticate();
            } else {
                this.onAuthenticated();
            }
        };

        this.ws.onmessage = (event) => {
            this.handleMessage(event.data);
        };

        this.ws.onclose = (event) => {
            console.log('WebSocket连接关闭:', event.code, event.reason);
            this.isConnecting = false;
            this.updateConnectionStatus('disconnected');
            
            // 清理待处理的请求
            this.pendingRequests.forEach((pending, id) => {
                pending.reject(new Error('连接已关闭'));
            });
            this.pendingRequests.clear();

            // 自动重连
            if (this.shouldReconnect && this.autoReconnect && this.reconnectAttempts < this.maxReconnectAttempts) {
                this.scheduleReconnect();
            }
        };

        this.ws.onerror = (error) => {
            console.error('WebSocket错误:', error);
            this.errorCallbacks.forEach(cb => cb(error));
        };
    }

    // 断开连接
    disconnect() {
        this.shouldReconnect = false;
        if (this.ws) {
            this.ws.close();
            this.ws = null;
        }
        this.reconnectAttempts = 0;
        this.updateConnectionStatus('disconnected');
    }

    // 认证
    authenticate() {
        this.send('auth', { token: this.token })
            .then(result => {
                console.log('认证成功:', result);
                this.onAuthenticated();
            })
            .catch(error => {
                console.error('认证失败:', error);
                this.updateConnectionStatus('auth_failed');
                this.errorCallbacks.forEach(cb => cb(error));
            });
    }

    // 认证成功后的处理（标记已连接 + 重新订阅）
    onAuthenticated() {
        this.updateConnectionStatus('connected');
        // 重新订阅之前的订阅
        for (const sub of this.pendingSubscriptions) {
            this.send('stream.subscribe', sub).catch(err => {
                console.error('重新订阅失败:', err);
            });
        }
    }

    // 保存订阅以便重连后恢复
    addPendingSubscription(params) {
        if (!this.pendingSubscriptions.some(s => s.stream_id === params.stream_id)) {
            this.pendingSubscriptions.push(params);
        }
    }

    removePendingSubscription(streamId) {
        this.pendingSubscriptions = this.pendingSubscriptions.filter(s => s.stream_id !== streamId);
    }

    // 发送JSON-RPC请求
    send(method, params = {}) {
        return new Promise((resolve, reject) => {
            if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
                reject(new Error('WebSocket未连接'));
                return;
            }

            const id = ++this.requestId;
            const request = {
                jsonrpc: '2.0',
                method: method,
                params: params,
                id: id
            };

            // 保存待处理的请求
            this.pendingRequests.set(id, { resolve, reject, method });

            // 发送请求
            this.ws.send(JSON.stringify(request));

            // 设置超时
            setTimeout(() => {
                if (this.pendingRequests.has(id)) {
                    this.pendingRequests.delete(id);
                    reject(new Error(`请求超时: ${method}`));
                }
            }, 30000); // 30秒超时
        });
    }

    // 处理接收到的消息
    handleMessage(data) {
        try {
            const message = JSON.parse(data);
            
            // 处理响应
            if (message.id && this.pendingRequests.has(message.id)) {
                const pending = this.pendingRequests.get(message.id);
                this.pendingRequests.delete(message.id);

                if (message.error) {
                    pending.reject(message.error);
                } else {
                    pending.resolve(message.result);
                }
            }
            // 处理通知（无id）
            else if (message.method && !message.id) {
                this.notificationCallbacks.forEach(cb => cb(message));
            }
            // 未知消息
            else {
                console.warn('未知消息格式:', message);
            }
        } catch (error) {
            console.error('解析消息失败:', error, data);
        }
    }

    // 计划重连
    scheduleReconnect() {
        this.reconnectAttempts++;
        const delay = this.reconnectBaseDelay * Math.pow(2, this.reconnectAttempts - 1);
        
        console.log(`${delay}ms 后重连 (尝试 ${this.reconnectAttempts}/${this.maxReconnectAttempts})`);
        this.updateConnectionStatus('reconnecting');

        setTimeout(() => {
            this.connect();
        }, delay);
    }

    // 更新连接状态
    updateConnectionStatus(status) {
        this.connectionChangeCallbacks.forEach(cb => cb(status));
    }

    // 注册回调
    onConnectionChange(callback) {
        this.connectionChangeCallbacks.push(callback);
    }

    onError(callback) {
        this.errorCallbacks.push(callback);
    }

    onNotification(callback) {
        this.notificationCallbacks.push(callback);
    }
}

// 导出（全局）
window.WebSocketClient = WebSocketClient;