/**
 * Stream subscription manager
 * 
 * Manages the WebSocket connection to the Flask backend, handling stream subscriptions and notifications.
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
     * Connect to the WebSocket endpoint of the Flask backend
     * @param {number} port - Flask server port
     * @param {string} systemId - system ID
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
        
        console.log(`Connecting to WebSocket server: ${url}`);
        
        try {
            this.ws = new WebSocket(url);

            this.ws.onopen = () => {
                console.log('WebSocket connection established');
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
                    console.error('Failed to parse message:', e);
                }
            };

            this.ws.onclose = (event) => {
                console.log('WebSocket connection closed:', event.code, event.reason);
                this._connected = false;
                this.scheduleReconnect();
            };

            this.ws.onerror = (error) => {
                console.error('WebSocket error:', error);
            };
        } catch (e) {
            console.error('Failed to create WebSocket connection:', e);
            this.scheduleReconnect();
        }
    }

    /**
     * Handle an incoming message
     * @param {Object} data - message data
     */
    handleMessage(data) {
        switch (data.type) {
            case 'connected':
                console.log('WebSocket connected, client ID:', data.client_id);
                break;
            case 'stream.on_change':
                this.handleStreamChange(data);
                break;
            case 'subscribed':
                console.log(`Subscribed: ${data.stream_name} -> ${data.subscription_id}`);
                this.subscriptions.set(data.stream_name, data.subscription_id);
                break;
            case 'unsubscribed':
                console.log(`Unsubscribed: ${data.stream_name}`);
                this.subscriptions.delete(data.stream_name);
                break;
            case 'subscriptions':
                console.log('Current subscription list:', data.subscriptions);
                break;
            case 'error':
                console.error('Subscription error:', data.message);
                break;
            case 'tool.on_registry_change':
                // Tool registered/unregistered → automatically refresh the tool list
                console.log('[Notification] Tool registry changed:', data.change);
                state.emit('toolRegistryChanged', data.change);
                break;
            case 'tool.on_process':
                // Background process lifecycle (started/exited) → dedicated channel
                console.log('[Notification] Process event:', data.change?.type, data.change?.process_name);
                this._handleProcessEvent(data);
                break;
            case 'scan.on_change':
                // Scan events → dispatch to the specific handler
                console.log('[Notification] Scan event:', data.change?.type, data.stream_name);
                this._handleScanEvent(data);
                break;
            default:
                console.warn('Unknown message type:', data.type);
        }
    }

    /**
     * Subscribe to a stream
     * @param {string} systemId - system ID
     * @param {string} streamName - stream name
     * @param {Function} callback - change callback function
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
            console.warn('WebSocket not connected, will subscribe after connecting');
        }
    }

    /**
     * Unsubscribe from a stream
     * @param {string} systemId - system ID
     * @param {string} streamName - stream name
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
     * Handle a stream change notification
     * @param {Object} data - change data
     */
    handleStreamChange(data) {
        const callback = this.callbacks.get(data.stream_name);
        if (callback) {
            try {
                callback(data);
            } catch (e) {
                console.error('Callback execution failed:', e);
            }
        }
    }

    /**
     * Handle a scan event notification
     * @param {Object} data - { type, stream_name, change }
     */
    _handleScanEvent(data) {
        const change = data.change;
        if (!change) return;

        switch (change.type) {
            case 'state':
                // Scan state change (idle/scanning/paused)
                // Note: use 'scanWSStateChange' instead of 'scanStateChanged',
                // to avoid conflicting with state.setScanState() events and causing infinite recursion
                state.emit('scanWSStateChange', {
                    status: change.new_state,
                    oldStatus: change.old_state,
                });
                break;

            case 'position':
                // Position update (300ms throttled, replaces polling)
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
                // Scan complete
                state.emit('scanCompleted', change);
                break;

            case 'error':
                // Scan error
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
     * Re-subscribe to all streams (used after reconnecting)
     */
    resubscribeAll() {
        if (!this.systemId) {
            return;
        }

        console.log(`Re-subscribing to ${this.callbacks.size} stream(s)`);
        
        for (const [streamName, callback] of this.callbacks) {
            this.subscribe(this.systemId, streamName, callback);
        }
    }

    /**
     * Schedule a reconnect
     */
    scheduleReconnect() {
        if (this.reconnectAttempts < this.maxReconnectAttempts) {
            this.reconnectAttempts++;
            const delay = this.reconnectDelay * Math.pow(2, this.reconnectAttempts - 1);
            
            console.log(`Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts}/${this.maxReconnectAttempts})`);
            
            setTimeout(() => {
                if (this.port && this.systemId) {
                    this.connect(this.port, this.systemId);
                }
            }, delay);
        } else {
            console.error('Maximum reconnect attempts reached, stopping reconnection');
        }
    }

    /**
     * Disconnect
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
     * List the current subscriptions
     */
    listSubscriptions() {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(JSON.stringify({
                action: 'list_subscriptions'
            }));
        }
    }

    /**
     * Whether the connection is established
     * @returns {boolean}
     */
    isConnected() {
        return this._connected && this.ws && this.ws.readyState === WebSocket.OPEN;
    }

    /**
     * Get the subscription count
     * @returns {number}
     */
    getSubscriptionCount() {
        return this.subscriptions.size;
    }

    /**
     * Get the names of all subscribed streams
     * @returns {string[]}
     */
    getSubscribedStreams() {
        return Array.from(this.subscriptions.keys());
    }
}

// Export (global)
window.StreamSubscriptionManager = StreamSubscriptionManager;
