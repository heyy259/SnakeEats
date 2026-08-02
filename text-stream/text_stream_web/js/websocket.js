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
        
        // Config options
        this.autoReconnect = options.autoReconnect !== false;
        this.maxReconnectAttempts = options.maxReconnectAttempts || 5;
        this.reconnectBaseDelay = options.reconnectBaseDelay || 1000;
        this.host = options.host || 'localhost';
        this.protocol = options.protocol || (window.location.protocol === 'https:' ? 'wss' : 'ws');
    }

    // Connect to the WebSocket service
    connect() {
        if (this.isConnecting || (this.ws && this.ws.readyState === WebSocket.OPEN)) {
            console.log('Already connected or connecting, skipping duplicate connection');
            return;
        }

        this.isConnecting = true;
        this.shouldReconnect = true;
        this.updateConnectionStatus('connecting');

        const url = `${this.protocol}://${this.host}:${this.port}`;
        console.log('Connecting to:', url);

        this.ws = new WebSocket(url);

        this.ws.onopen = () => {
            console.log('WebSocket connection established');
            this.reconnectAttempts = 0;
            this.isConnecting = false;
            
            // If a token is set, send an authentication request; otherwise mark as connected
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
            console.log('WebSocket connection closed:', event.code, event.reason);
            this.isConnecting = false;
            this.updateConnectionStatus('disconnected');
            
            // Clean up pending requests
            this.pendingRequests.forEach((pending, id) => {
                pending.reject(new Error('Connection closed'));
            });
            this.pendingRequests.clear();

            // Auto reconnect
            if (this.shouldReconnect && this.autoReconnect && this.reconnectAttempts < this.maxReconnectAttempts) {
                this.scheduleReconnect();
            }
        };

        this.ws.onerror = (error) => {
            console.error('WebSocket error:', error);
            this.errorCallbacks.forEach(cb => cb(error));
        };
    }

    // Disconnect
    disconnect() {
        this.shouldReconnect = false;
        if (this.ws) {
            this.ws.close();
            this.ws = null;
        }
        this.reconnectAttempts = 0;
        this.updateConnectionStatus('disconnected');
    }

    // Authentication
    authenticate() {
        this.send('auth', { token: this.token })
            .then(result => {
                console.log('Authentication successful:', result);
                this.onAuthenticated();
            })
            .catch(error => {
                console.error('Authentication failed:', error);
                this.updateConnectionStatus('auth_failed');
                this.errorCallbacks.forEach(cb => cb(error));
            });
    }

    // Handle after successful authentication (mark connected + resubscribe)
    onAuthenticated() {
        this.updateConnectionStatus('connected');
        // Resubscribe to previous subscriptions
        for (const sub of this.pendingSubscriptions) {
            this.send('stream.subscribe', sub).catch(err => {
                console.error('Resubscribe failed:', err);
            });
        }
    }

    // Save subscriptions to restore after reconnect
    addPendingSubscription(params) {
        if (!this.pendingSubscriptions.some(s => s.stream_id === params.stream_id)) {
            this.pendingSubscriptions.push(params);
        }
    }

    removePendingSubscription(streamId) {
        this.pendingSubscriptions = this.pendingSubscriptions.filter(s => s.stream_id !== streamId);
    }

    // Send a JSON-RPC request
    send(method, params = {}) {
        return new Promise((resolve, reject) => {
            if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
                reject(new Error('WebSocket not connected'));
                return;
            }

            const id = ++this.requestId;
            const request = {
                jsonrpc: '2.0',
                method: method,
                params: params,
                id: id
            };

            // Save the pending request
            this.pendingRequests.set(id, { resolve, reject, method });

            // Send the request
            this.ws.send(JSON.stringify(request));

            // Set a timeout
            setTimeout(() => {
                if (this.pendingRequests.has(id)) {
                    this.pendingRequests.delete(id);
                    reject(new Error(`Request timeout: ${method}`));
                }
            }, 30000); // 30-second timeout
        });
    }

    // Handle incoming messages
    handleMessage(data) {
        try {
            const message = JSON.parse(data);
            
            // Handle response
            if (message.id && this.pendingRequests.has(message.id)) {
                const pending = this.pendingRequests.get(message.id);
                this.pendingRequests.delete(message.id);

                if (message.error) {
                    pending.reject(message.error);
                } else {
                    pending.resolve(message.result);
                }
            }
            // Handle notification (no id)
            else if (message.method && !message.id) {
                this.notificationCallbacks.forEach(cb => cb(message));
            }
            // Unknown message
            else {
                console.warn('Unknown message format:', message);
            }
        } catch (error) {
            console.error('Failed to parse message:', error, data);
        }
    }

    // Schedule reconnect
    scheduleReconnect() {
        this.reconnectAttempts++;
        const delay = this.reconnectBaseDelay * Math.pow(2, this.reconnectAttempts - 1);
        
        console.log(`Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts}/${this.maxReconnectAttempts})`);
        this.updateConnectionStatus('reconnecting');

        setTimeout(() => {
            this.connect();
        }, delay);
    }

    // Update connection status
    updateConnectionStatus(status) {
        this.connectionChangeCallbacks.forEach(cb => cb(status));
    }

    // Register callbacks
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

// Export (global)
window.WebSocketClient = WebSocketClient;