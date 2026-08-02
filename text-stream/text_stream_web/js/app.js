console.log('text-stream Web UI started');

// Config management
const config = {
    port: 9876,
    token: null,
    autoReconnect: true,
    maxReconnectAttempts: 5,
    reconnectBaseDelay: 1000
};

let wsClient = null;
let streamManager = null;
let subscriptionManager = null;

// Load config from URL parameters
function loadConfig() {
    const params = new URLSearchParams(window.location.search);
    if (params.has('port')) {
        config.port = parseInt(params.get('port'));
    }

    // Read token from sessionStorage (avoid leaking it in the URL)
    const savedToken = sessionStorage.getItem('text_stream_token');
    if (savedToken) {
        config.token = savedToken;
    } else if (params.has('token')) {
        config.token = params.get('token');
        sessionStorage.setItem('text_stream_token', config.token);
    }

    // Load from local storage
    const saved = localStorage.getItem('text_stream_config');
    if (saved) {
        try {
            const parsed = JSON.parse(saved);
            if (parsed.token) delete parsed.token; // Don't restore the token from local storage
            Object.assign(config, parsed);
        } catch (e) {
            console.warn('Failed to parse saved config:', e);
        }
    }
}

// Save config to local storage
function saveConfig() {
    localStorage.setItem('text_stream_config', JSON.stringify(config));
}

// Initialize the app
function initApp() {
    loadConfig();
    console.log('Config:', config);
    
    // Initialize WebSocket client
    wsClient = new WebSocketClient(config.port, config.token, {
        autoReconnect: config.autoReconnect,
        maxReconnectAttempts: config.maxReconnectAttempts,
        reconnectBaseDelay: config.reconnectBaseDelay
    });
    
    // Initialize stream manager
    streamManager = new StreamManager(wsClient);
    
    // Initialize subscription manager
    subscriptionManager = new SubscriptionManager(wsClient);
    
    // Register subscription notification callback
    subscriptionManager.onNotification((notification) => {
        addNotification(notification);
        updateStatusBar();
    });
    
    // Register connection status callback
    wsClient.onConnectionChange((status) => {
        updateConnectionUI(status);
    });
    
    // Register error callback
    wsClient.onError((error) => {
        const msg = error.message || 'Connection error';
        showError('WebSocket error: ' + msg);
    });
    
    // Bind connection button events
    document.getElementById('btn-connect').addEventListener('click', () => {
        wsClient.connect();
    });
    
    document.getElementById('btn-disconnect').addEventListener('click', () => {
        wsClient.disconnect();
    });
    
    // Bind stream list button event
    document.getElementById('btn-create-stream').addEventListener('click', showCreateStreamDialog);
    
    // Bind search event
    document.getElementById('stream-search').addEventListener('input', (e) => {
        filterStreams(e.target.value);
    });
    
    // Bind tab switching
    document.querySelectorAll('.tab').forEach(tab => {
        tab.addEventListener('click', (e) => switchTab(e.target.dataset.tab));
    });

    // Bind editor toolbar buttons
    document.getElementById('btn-read').addEventListener('click', readStreamContent);
    document.getElementById('btn-write').addEventListener('click', writeStreamContent);
    document.getElementById('btn-append').addEventListener('click', appendStreamContent);
    document.getElementById('btn-undo').addEventListener('click', undoStreamContent);

    // Bind search event
    document.getElementById('btn-search').addEventListener('click', searchStreamContent);

    // Bind metadata action buttons
    document.getElementById('btn-add-tag').addEventListener('click', addTag);
    document.getElementById('btn-update-desc').addEventListener('click', updateDescription);
    document.getElementById('btn-rename').addEventListener('click', renameStream);

    // Bind subscribe/unsubscribe buttons
    document.getElementById('btn-subscribe').addEventListener('click', subscribeToStream);
    document.getElementById('btn-unsubscribe').addEventListener('click', unsubscribeFromStream);

    // Bind delete stream button
    document.getElementById('btn-delete-stream').addEventListener('click', deleteCurrentStream);

    // Bind append modal dialog events
    document.getElementById('append-modal-close').addEventListener('click', closeAppendModal);
    document.getElementById('append-modal-cancel').addEventListener('click', closeAppendModal);
    document.getElementById('append-modal-confirm').addEventListener('click', confirmAppendContent);
    
    // Close when clicking outside the modal
    document.getElementById('append-modal').addEventListener('click', (e) => {
        if (e.target === document.getElementById('append-modal')) {
            closeAppendModal();
        }
    });

    // Keyboard shortcut support
    document.getElementById('append-textarea').addEventListener('keydown', (e) => {
        // Ctrl+Enter or Cmd+Enter to confirm append
        if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
            e.preventDefault();
            confirmAppendContent();
        }
        // Escape closes the modal
        if (e.key === 'Escape') {
            closeAppendModal();
        }
    });

    // Auto connect
    wsClient.connect();
}

// Update connection status UI
function updateConnectionUI(status) {
    const statusEl = document.getElementById('connection-status');
    const connectBtn = document.getElementById('btn-connect');
    const disconnectBtn = document.getElementById('btn-disconnect');
    
    switch (status) {
        case 'connecting':
            statusEl.textContent = 'Connecting...';
            connectBtn.disabled = true;
            disconnectBtn.disabled = true;
            break;
        case 'connected':
            statusEl.textContent = 'Connected';
            connectBtn.disabled = true;
            disconnectBtn.disabled = false;
            // Load system info after connection succeeds (only when the connection is actually usable)
            if (wsClient && wsClient.ws && wsClient.ws.readyState === WebSocket.OPEN) {
                loadSystemInfo();
            }
            break;
        case 'disconnected':
            statusEl.textContent = 'Disconnected';
            connectBtn.disabled = false;
            disconnectBtn.disabled = true;
            break;
        case 'reconnecting':
            statusEl.textContent = 'Reconnecting...';
            connectBtn.disabled = true;
            disconnectBtn.disabled = true;
            break;
        case 'auth_failed':
            statusEl.textContent = 'Authentication failed';
            connectBtn.disabled = false;
            disconnectBtn.disabled = true;
            break;
    }
    
    // Update the bottom status bar
    document.getElementById('status-connection').textContent = statusEl.textContent;
}

// Load system info
async function loadSystemInfo() {
    try {
        const info = await wsClient.send('system.info');
        document.getElementById('system-id').textContent = `ID: ${info.system_id}`;
        document.getElementById('port').textContent = `Port: ${info.port}`;
        // Load stream list
        await loadStreamList();
    } catch (error) {
        console.error('Failed to fetch system info:', error);
    }
}

// Add a notification to the list
function addNotification(params) {
    const notificationsList = document.getElementById('notifications-list');
    if (!notificationsList) return;
    
    const notificationEl = document.createElement('div');
    notificationEl.className = 'notification-item';

    const timeEl = document.createElement('span');
    timeEl.className = 'notification-time';
    timeEl.textContent = new Date().toLocaleTimeString();

    const contentEl = document.createElement('span');
    contentEl.className = 'notification-content';
    contentEl.textContent = JSON.stringify(params);

    notificationEl.appendChild(timeEl);
    notificationEl.appendChild(contentEl);
    
    notificationsList.insertBefore(notificationEl, notificationsList.firstChild);
    
    // Limit the number of notifications
    while (notificationsList.children.length > 100) {
        notificationsList.removeChild(notificationsList.lastChild);
    }
}

// Ensure the toast container exists
function ensureToastContainer() {
    let container = document.getElementById('toast-container');
    if (!container) {
        container = document.createElement('div');
        container.id = 'toast-container';
        container.style.cssText = 'position:fixed;top:16px;right:16px;z-index:9999;display:flex;flex-direction:column;gap:8px;';
        document.body.appendChild(container);
    }
    return container;
}

// Show error
function showError(message) {
    console.error(message);
    const container = ensureToastContainer();
    const toast = document.createElement('div');
    toast.className = 'toast-error';
    toast.style.cssText = 'background:#e74c3c;color:#fff;padding:10px 16px;border-radius:4px;font-size:14px;box-shadow:0 2px 8px rgba(0,0,0,0.2);max-width:400px;word-break:break-word;';
    toast.textContent = message;
    container.appendChild(toast);
    setTimeout(() => {
        toast.classList.add('removing');
        setTimeout(() => toast.remove(), 300);
    }, 5000);
}

// Initialize after the page has loaded
document.addEventListener('DOMContentLoaded', initApp);

// Load stream list
async function loadStreamList() {
    const container = document.getElementById('stream-list');
    container.innerHTML = '<div style="text-align:center;color:#666;padding:20px;">Loading stream list...</div>';
    try {
        const streams = await streamManager.list();
        renderStreamList(streams);
        updateStatusBar();
    } catch (error) {
        showError('Failed to load stream list: ' + error.message);
        container.innerHTML = '<div style="color:#e74c3c;padding:10px;">Failed to load</div>';
    }
}

// Render stream list
function renderStreamList(streams) {
    const container = document.getElementById('stream-list');
    container.innerHTML = '';
    
    streams.forEach(stream => {
        const item = document.createElement('div');
        item.className = 'stream-item';
        if (streamManager.currentStream && streamManager.currentStream.name === stream.name) {
            item.classList.add('active');
        }
        
        item.innerHTML = `
            <div class="stream-item-name">${escapeHtml(stream.name)}</div>
            <div class="stream-item-preview">${escapeHtml(stream.description || '')}</div>
            <div class="stream-item-tags">
                ${Object.entries(stream.tags || {}).map(([k, v]) => 
                    `<span class="tag">${escapeHtml(k)}=${escapeHtml(v)}</span>`
                ).join('')}
            </div>
        `;
        
        item.addEventListener('click', () => selectStream(stream.name));
        container.appendChild(item);
    });
}

// Select a stream
async function selectStream(name) {
    try {
        const [detail, metadata] = await Promise.all([
            streamManager.get(name),
            streamManager.getMetadata(name)
        ]);
        
        streamManager.currentStream = { name, ...detail, ...metadata };
        
        renderStreamDetail(name, streamManager.currentStream);
        
        // Update list highlight
        document.querySelectorAll('.stream-item').forEach(item => {
            item.classList.remove('active');
            if (item.querySelector('.stream-item-name').textContent === name) {
                item.classList.add('active');
            }
        });
    } catch (error) {
        showError('Failed to fetch stream details: ' + error.message);
    }
}

// Render stream detail
function renderStreamDetail(name, detail) {
    document.getElementById('stream-detail').style.display = 'block';
    document.getElementById('empty-state').style.display = 'none';
    
    document.getElementById('stream-name').textContent = name;
    document.getElementById('stream-version').textContent = `v${detail.version}`;
    document.getElementById('stream-description').textContent = detail.description || '';
    
    // Render tags
    const tagsContainer = document.getElementById('stream-tags');
    tagsContainer.innerHTML = Object.entries(detail.tags || {}).map(([k, v]) => 
        `<span class="tag">${escapeHtml(k)}=${escapeHtml(v)}</span>`
    ).join('');
    
    // Load content
    loadStreamContent(name);
}

// Load stream content
async function loadStreamContent(name) {
    const editor = document.getElementById('editor');
    editor.value = 'Loading content...';
    try {
        const content = await streamManager.read(name);
        editor.value = content;
    } catch (error) {
        showError('Failed to load content: ' + error.message);
        editor.value = 'Failed to load: ' + error.message;
    }
}

// Filter stream list
function filterStreams(query) {
    const items = document.querySelectorAll('.stream-item');
    items.forEach(item => {
        const name = item.querySelector('.stream-item-name').textContent.toLowerCase();
        const preview = item.querySelector('.stream-item-preview').textContent.toLowerCase();
        const matches = name.includes(query.toLowerCase()) || preview.includes(query.toLowerCase());
        item.style.display = matches ? 'block' : 'none';
    });
}

// Show create stream dialog
function showCreateStreamDialog() {
    const name = prompt('Enter a stream name:');
    if (name) {
        const description = prompt('Enter a description (optional):') || '';
        createStream(name, description);
    }
}

// Create stream
async function createStream(name, description) {
    try {
        await streamManager.create(name, description);
        renderStreamList(streamManager.streams);
        updateStatusBar();
        showSuccess(`Stream "${name}" created successfully`);
    } catch (error) {
        showError('Failed to create stream: ' + error.message);
    }
}

// Update status bar
function updateStatusBar() {
    document.getElementById('status-streams').textContent = `Streams: ${streamManager.streams.length}`;
    document.getElementById('status-subscriptions').textContent = `Subscriptions: ${subscriptionManager.getActiveSubscriptions().length}`;
}

// Render active subscription list
function renderActiveSubscriptions() {
    const container = document.getElementById('subscriptions-list');
    if (!container) return;
    
    const subscriptions = subscriptionManager.getActiveSubscriptions();
    container.innerHTML = '';
    
    if (subscriptions.length === 0) {
        container.innerHTML = '<div style="color:#666;font-size:12px;padding:8px;">No active subscriptions</div>';
        return;
    }
    
    subscriptions.forEach(sub => {
        const item = document.createElement('div');
        item.className = 'subscription-item';
        item.style.cssText = 'padding:6px 8px;border-bottom:1px solid var(--border-color);font-size:12px;';
        
        const nameEl = document.createElement('div');
        nameEl.style.cssText = 'font-weight:500;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;';
        nameEl.textContent = sub.streamName;
        nameEl.title = sub.streamName;
        
        const idEl = document.createElement('div');
        idEl.style.cssText = 'color:#666;font-size:11px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;';
        idEl.textContent = sub.id;
        idEl.title = sub.id;
        
        item.appendChild(nameEl);
        item.appendChild(idEl);
        
        item.addEventListener('click', () => {
            selectStream(sub.streamName);
        });
        
        container.appendChild(item);
    });
}

// HTML escaping
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Switch tab
function switchTab(tabName) {
    document.querySelectorAll('.tab').forEach(tab => {
        tab.classList.toggle('active', tab.dataset.tab === tabName);
    });
    
    document.querySelectorAll('.tab-content').forEach(content => {
        content.classList.toggle('active', content.id === `tab-${tabName}`);
    });
    
    if (tabName === 'metadata' && streamManager.currentStream) {
        document.getElementById('meta-description').value = streamManager.currentStream.description || '';
        renderTagsList(streamManager.currentStream.tags || {});
    }
}

// Read stream content
async function readStreamContent() {
    if (!streamManager.currentStream) return;
    
    try {
        const content = await streamManager.read(streamManager.currentStream.name);
        document.getElementById('editor').value = content;
        updateLastOperation('Read');
    } catch (error) {
        showError('Read failed: ' + error.message);
    }
}

// Write stream content
async function writeStreamContent() {
    if (!streamManager.currentStream) return;
    
    const content = document.getElementById('editor').value;
    try {
        await streamManager.write(streamManager.currentStream.name, content);
        showSuccess('Write successful');
        updateLastOperation('Write');
    } catch (error) {
        showError('Write failed: ' + error.message);
    }
}

// Append stream content
async function appendStreamContent() {
    if (!streamManager.currentStream) return;
    
    // Show modal dialog
    const modal = document.getElementById('append-modal');
    const textarea = document.getElementById('append-textarea');
    textarea.value = '';
    modal.style.display = 'flex';
    textarea.focus();
}

// Confirm append content
async function confirmAppendContent() {
    if (!streamManager.currentStream) return;
    
    const textarea = document.getElementById('append-textarea');
    const content = textarea.value;
    
    if (content) {
        try {
            await streamManager.append(streamManager.currentStream.name, content);
            await readStreamContent();
            showSuccess('Append successful');
            updateLastOperation('Append');
        } catch (error) {
            showError('Append failed: ' + error.message);
        }
    }
    
    // Close modal dialog
    document.getElementById('append-modal').style.display = 'none';
}

// Close append modal dialog
function closeAppendModal() {
    document.getElementById('append-modal').style.display = 'none';
}

// Undo operation
async function undoStreamContent() {
    if (!streamManager.currentStream) return;
    
    try {
        await streamManager.undo(streamManager.currentStream.name);
        await readStreamContent();
        showSuccess('Undo successful');
        updateLastOperation('Undo');
    } catch (error) {
        showError('Undo failed: ' + error.message);
    }
}

// Search stream content
async function searchStreamContent() {
    if (!streamManager.currentStream) return;
    
    const pattern = document.getElementById('search-input').value;
    const regex = document.getElementById('regex-mode').checked;
    
    if (!pattern) {
        showError('Please enter a search query');
        return;
    }
    
    try {
        const results = await streamManager.searchInStream(
            streamManager.currentStream.name,
            pattern,
            100,
            100,
            regex
        );
        renderSearchResults(results.matches);
        updateLastOperation('Search');
    } catch (error) {
        showError('Search failed: ' + error.message);
    }
}

// Render search results
function renderSearchResults(matches) {
    const container = document.getElementById('search-results');
    container.innerHTML = '';
    
    const items = matches || [];
    
    if (items.length === 0) {
        container.innerHTML = '<div class="search-result-item">No matching results</div>';
        return;
    }
    
    items.forEach((match, index) => {
        const item = document.createElement('div');
        item.className = 'search-result-item';
        item.innerHTML = `
            <div class="search-result-info">
                Match #${index + 1} | Position: ${match.start_pos}-${match.end_pos} | Line: ${match.line}
            </div>
            <div class="search-result-preview">${escapeHtml(match.preview)}</div>
        `;
        
        item.addEventListener('click', () => {
            const editor = document.getElementById('editor');
            editor.setSelectionRange(match.start_pos, match.end_pos);
            editor.focus();
        });
        
        container.appendChild(item);
    });
}

// Load stream metadata
async function loadStreamMetadata(name) {
    try {
        const metadata = await streamManager.getMetadata(name);
        
        document.getElementById('meta-description').value = metadata.description || '';
        
        renderTagsList(metadata.tags || {});
    } catch (error) {
        showError('Failed to load metadata: ' + error.message);
    }
}

// Render tag list
function renderTagsList(tags) {
    const container = document.getElementById('tags-list');
    container.innerHTML = '';
    
    Object.entries(tags).forEach(([key, value]) => {
        const item = document.createElement('div');
        item.className = 'tag-item';
        item.innerHTML = `
            <span>${escapeHtml(key)}=${escapeHtml(value)}</span>
            <span class="remove-tag" data-key="${escapeHtml(key)}">&times;</span>
        `;
        
        item.querySelector('.remove-tag').addEventListener('click', (e) => {
            removeTag(e.target.dataset.key);
        });
        
        container.appendChild(item);
    });
}

// Add tag
async function addTag() {
    if (!streamManager.currentStream) return;
    
    const key = document.getElementById('tag-key').value;
    const value = document.getElementById('tag-value').value;
    
    if (!key || !value) {
        showError('Please enter a tag key and value');
        return;
    }
    
    try {
        await streamManager.setTag(streamManager.currentStream.name, key, value);
        await loadStreamMetadata(streamManager.currentStream.name);
        document.getElementById('tag-key').value = '';
        document.getElementById('tag-value').value = '';
        showSuccess('Tag added successfully');
    } catch (error) {
        showError('Failed to add tag: ' + error.message);
    }
}

// Remove tag
async function removeTag(key) {
    if (!streamManager.currentStream) return;
    
    try {
        await streamManager.removeTag(streamManager.currentStream.name, key);
        await loadStreamMetadata(streamManager.currentStream.name);
        showSuccess('Tag removed successfully');
    } catch (error) {
        showError('Failed to remove tag: ' + error.message);
    }
}

// Update description
async function updateDescription() {
    if (!streamManager.currentStream) return;
    
    const description = document.getElementById('meta-description').value;
    
    try {
        await streamManager.updateDescription(streamManager.currentStream.name, description);
        showSuccess('Description updated successfully');
    } catch (error) {
        showError('Failed to update description: ' + error.message);
    }
}

// Rename stream
async function renameStream() {
    if (!streamManager.currentStream) return;
    
    const newName = document.getElementById('new-name').value;
    
    if (!newName) {
        showError('Please enter a new name');
        return;
    }
    
    try {
        const oldName = streamManager.currentStream.name;
        await streamManager.rename(oldName, newName);
        
        streamManager.currentStream.name = newName;
        document.getElementById('stream-name').textContent = newName;
        document.getElementById('new-name').value = '';
        
        renderStreamList(streamManager.streams);
        
        showSuccess(`Rename successful: ${oldName} -> ${newName}`);
        updateLastOperation('Rename');
    } catch (error) {
        showError('Rename failed: ' + error.message);
    }
}

// Update last operation
function updateLastOperation(op) {
    const now = new Date();
    const timeStr = now.toLocaleTimeString('en-US');
    document.getElementById('status-last-op').textContent = `Last operation: ${op} ${timeStr}`;
}

// Subscribe to a stream
async function subscribeToStream() {
    if (!streamManager.currentStream) return;
    
    const fromPosition = document.getElementById('from-position').value;
    const position = fromPosition ? parseInt(fromPosition) : null;
    
    try {
        const subscriptionId = await subscriptionManager.subscribe(
            streamManager.currentStream.name,
            position
        );
        
        showSuccess(`Subscription successful: ${subscriptionId}`);
        updateLastOperation('Subscribe');
        renderActiveSubscriptions();
        updateStatusBar();
    } catch (error) {
        showError('Subscription failed: ' + error.message);
    }
}

// Unsubscribe
async function unsubscribeFromStream() {
    if (!streamManager.currentStream) return;
    
    const subscriptionId = subscriptionManager.getSubscriptionId(streamManager.currentStream.name);
    if (!subscriptionId) {
        showError('The current stream has no active subscriptions');
        return;
    }
    
    try {
        await subscriptionManager.unsubscribe(subscriptionId);
        showSuccess('Unsubscribe successful');
        updateLastOperation('Unsubscribe');
        renderActiveSubscriptions();
        updateStatusBar();
    } catch (error) {
        showError('Unsubscribe failed: ' + error.message);
    }
}

// Delete current stream
async function deleteCurrentStream() {
    if (!streamManager.currentStream) return;

    const name = streamManager.currentStream.name;

    if (!confirm(`Are you sure you want to delete the stream "${name}"? This action cannot be undone.`)) {
        return;
    }

    try {
        await streamManager.delete(name);
        document.getElementById('stream-detail').style.display = 'none';
        document.getElementById('empty-state').style.display = 'flex';
        renderStreamList(streamManager.streams);
        updateStatusBar();
        showSuccess(`Stream "${name}" deleted`);
    } catch (error) {
        showError('Delete failed: ' + error.message);
    }
}

// Show success message
function showSuccess(message) {
    console.log(message);
    const container = ensureToastContainer();
    const toast = document.createElement('div');
    toast.className = 'toast-success';
    toast.style.cssText = 'background:#27ae60;color:#fff;padding:10px 16px;border-radius:4px;font-size:14px;box-shadow:0 2px 8px rgba(0,0,0,0.2);max-width:400px;word-break:break-word;';
    toast.textContent = message;
    container.appendChild(toast);
    setTimeout(() => {
        toast.classList.add('removing');
        setTimeout(() => toast.remove(), 300);
    }, 3000);
}
