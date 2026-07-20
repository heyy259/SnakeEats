console.log('text-stream 前端UI启动');

// 配置管理
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

// 从URL参数加载配置
function loadConfig() {
    const params = new URLSearchParams(window.location.search);
    if (params.has('port')) {
        config.port = parseInt(params.get('port'));
    }

    // 从sessionStorage读取token（避免URL泄露）
    const savedToken = sessionStorage.getItem('text_stream_token');
    if (savedToken) {
        config.token = savedToken;
    } else if (params.has('token')) {
        config.token = params.get('token');
        sessionStorage.setItem('text_stream_token', config.token);
    }

    // 从本地存储加载
    const saved = localStorage.getItem('text_stream_config');
    if (saved) {
        try {
            const parsed = JSON.parse(saved);
            if (parsed.token) delete parsed.token; // 不从localStorage恢复token
            Object.assign(config, parsed);
        } catch (e) {
            console.warn('无法解析保存的配置:', e);
        }
    }
}

// 保存配置到本地存储
function saveConfig() {
    localStorage.setItem('text_stream_config', JSON.stringify(config));
}

// 初始化应用
function initApp() {
    loadConfig();
    console.log('配置:', config);
    
    // 初始化WebSocket客户端
    wsClient = new WebSocketClient(config.port, config.token, {
        autoReconnect: config.autoReconnect,
        maxReconnectAttempts: config.maxReconnectAttempts,
        reconnectBaseDelay: config.reconnectBaseDelay
    });
    
    // 初始化流管理器
    streamManager = new StreamManager(wsClient);
    
    // 初始化订阅管理器
    subscriptionManager = new SubscriptionManager(wsClient);
    
    // 注册订阅通知回调
    subscriptionManager.onNotification((notification) => {
        addNotification(notification);
        updateStatusBar();
    });
    
    // 注册连接状态回调
    wsClient.onConnectionChange((status) => {
        updateConnectionUI(status);
    });
    
    // 注册错误回调
    wsClient.onError((error) => {
        const msg = error.message || '连接错误';
        showError('WebSocket错误: ' + msg);
    });
    
    // 绑定连接按钮事件
    document.getElementById('btn-connect').addEventListener('click', () => {
        wsClient.connect();
    });
    
    document.getElementById('btn-disconnect').addEventListener('click', () => {
        wsClient.disconnect();
    });
    
    // 绑定流列表按钮事件
    document.getElementById('btn-create-stream').addEventListener('click', showCreateStreamDialog);
    
    // 绑定搜索事件
    document.getElementById('stream-search').addEventListener('input', (e) => {
        filterStreams(e.target.value);
    });
    
    // 绑定标签页切换
    document.querySelectorAll('.tab').forEach(tab => {
        tab.addEventListener('click', (e) => switchTab(e.target.dataset.tab));
    });

    // 绑定编辑器工具栏按钮
    document.getElementById('btn-read').addEventListener('click', readStreamContent);
    document.getElementById('btn-write').addEventListener('click', writeStreamContent);
    document.getElementById('btn-append').addEventListener('click', appendStreamContent);
    document.getElementById('btn-undo').addEventListener('click', undoStreamContent);

    // 绑定搜索事件
    document.getElementById('btn-search').addEventListener('click', searchStreamContent);

    // 绑定元数据操作按钮
    document.getElementById('btn-add-tag').addEventListener('click', addTag);
    document.getElementById('btn-update-desc').addEventListener('click', updateDescription);
    document.getElementById('btn-rename').addEventListener('click', renameStream);

    // 绑定订阅/取消订阅按钮
    document.getElementById('btn-subscribe').addEventListener('click', subscribeToStream);
    document.getElementById('btn-unsubscribe').addEventListener('click', unsubscribeFromStream);

    // 绑定删除流按钮
    document.getElementById('btn-delete-stream').addEventListener('click', deleteCurrentStream);

    // 绑定追加模态对话框事件
    document.getElementById('append-modal-close').addEventListener('click', closeAppendModal);
    document.getElementById('append-modal-cancel').addEventListener('click', closeAppendModal);
    document.getElementById('append-modal-confirm').addEventListener('click', confirmAppendContent);
    
    // 点击模态框外部关闭
    document.getElementById('append-modal').addEventListener('click', (e) => {
        if (e.target === document.getElementById('append-modal')) {
            closeAppendModal();
        }
    });

    // 键盘快捷键支持
    document.getElementById('append-textarea').addEventListener('keydown', (e) => {
        // Ctrl+Enter 或 Cmd+Enter 确认追加
        if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
            e.preventDefault();
            confirmAppendContent();
        }
        // Escape 关闭模态框
        if (e.key === 'Escape') {
            closeAppendModal();
        }
    });

    // 自动连接
    wsClient.connect();
}

// 更新连接状态UI
function updateConnectionUI(status) {
    const statusEl = document.getElementById('connection-status');
    const connectBtn = document.getElementById('btn-connect');
    const disconnectBtn = document.getElementById('btn-disconnect');
    
    switch (status) {
        case 'connecting':
            statusEl.textContent = '连接中...';
            connectBtn.disabled = true;
            disconnectBtn.disabled = true;
            break;
        case 'connected':
            statusEl.textContent = '已连接';
            connectBtn.disabled = true;
            disconnectBtn.disabled = false;
            // 连接成功后加载系统信息（仅在连接确实可用时）
            if (wsClient && wsClient.ws && wsClient.ws.readyState === WebSocket.OPEN) {
                loadSystemInfo();
            }
            break;
        case 'disconnected':
            statusEl.textContent = '未连接';
            connectBtn.disabled = false;
            disconnectBtn.disabled = true;
            break;
        case 'reconnecting':
            statusEl.textContent = '重连中...';
            connectBtn.disabled = true;
            disconnectBtn.disabled = true;
            break;
        case 'auth_failed':
            statusEl.textContent = '认证失败';
            connectBtn.disabled = false;
            disconnectBtn.disabled = true;
            break;
    }
    
    // 更新底部状态栏
    document.getElementById('status-connection').textContent = statusEl.textContent;
}

// 加载系统信息
async function loadSystemInfo() {
    try {
        const info = await wsClient.send('system.info');
        document.getElementById('system-id').textContent = `ID: ${info.system_id}`;
        document.getElementById('port').textContent = `端口: ${info.port}`;
        // 加载流列表
        await loadStreamList();
    } catch (error) {
        console.error('获取系统信息失败:', error);
    }
}

// 添加通知到列表
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
    
    // 限制通知数量
    while (notificationsList.children.length > 100) {
        notificationsList.removeChild(notificationsList.lastChild);
    }
}

// 确保toast容器存在
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

// 显示错误
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

// 页面加载完成后初始化
document.addEventListener('DOMContentLoaded', initApp);

// 加载流列表
async function loadStreamList() {
    const container = document.getElementById('stream-list');
    container.innerHTML = '<div style="text-align:center;color:#666;padding:20px;">加载流列表...</div>';
    try {
        const streams = await streamManager.list();
        renderStreamList(streams);
        updateStatusBar();
    } catch (error) {
        showError('加载流列表失败: ' + error.message);
        container.innerHTML = '<div style="color:#e74c3c;padding:10px;">加载失败</div>';
    }
}

// 渲染流列表
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

// 选择流
async function selectStream(name) {
    try {
        const [detail, metadata] = await Promise.all([
            streamManager.get(name),
            streamManager.getMetadata(name)
        ]);
        
        streamManager.currentStream = { name, ...detail, ...metadata };
        
        renderStreamDetail(name, streamManager.currentStream);
        
        // 更新列表高亮
        document.querySelectorAll('.stream-item').forEach(item => {
            item.classList.remove('active');
            if (item.querySelector('.stream-item-name').textContent === name) {
                item.classList.add('active');
            }
        });
    } catch (error) {
        showError('获取流详情失败: ' + error.message);
    }
}

// 渲染流详情
function renderStreamDetail(name, detail) {
    document.getElementById('stream-detail').style.display = 'block';
    document.getElementById('empty-state').style.display = 'none';
    
    document.getElementById('stream-name').textContent = name;
    document.getElementById('stream-version').textContent = `v${detail.version}`;
    document.getElementById('stream-description').textContent = detail.description || '';
    
    // 渲染标签
    const tagsContainer = document.getElementById('stream-tags');
    tagsContainer.innerHTML = Object.entries(detail.tags || {}).map(([k, v]) => 
        `<span class="tag">${escapeHtml(k)}=${escapeHtml(v)}</span>`
    ).join('');
    
    // 加载内容
    loadStreamContent(name);
}

// 加载流内容
async function loadStreamContent(name) {
    const editor = document.getElementById('editor');
    editor.value = '加载内容...';
    try {
        const content = await streamManager.read(name);
        editor.value = content;
    } catch (error) {
        showError('加载内容失败: ' + error.message);
        editor.value = '加载失败: ' + error.message;
    }
}

// 过滤流列表
function filterStreams(query) {
    const items = document.querySelectorAll('.stream-item');
    items.forEach(item => {
        const name = item.querySelector('.stream-item-name').textContent.toLowerCase();
        const preview = item.querySelector('.stream-item-preview').textContent.toLowerCase();
        const matches = name.includes(query.toLowerCase()) || preview.includes(query.toLowerCase());
        item.style.display = matches ? 'block' : 'none';
    });
}

// 显示创建流对话框
function showCreateStreamDialog() {
    const name = prompt('请输入流名称:');
    if (name) {
        const description = prompt('请输入描述 (可选):') || '';
        createStream(name, description);
    }
}

// 创建流
async function createStream(name, description) {
    try {
        await streamManager.create(name, description);
        renderStreamList(streamManager.streams);
        updateStatusBar();
        showSuccess(`流 "${name}" 创建成功`);
    } catch (error) {
        showError('创建流失败: ' + error.message);
    }
}

// 更新状态栏
function updateStatusBar() {
    document.getElementById('status-streams').textContent = `流: ${streamManager.streams.length}`;
    document.getElementById('status-subscriptions').textContent = `订阅: ${subscriptionManager.getActiveSubscriptions().length}`;
}

// 渲染活跃订阅列表
function renderActiveSubscriptions() {
    const container = document.getElementById('subscriptions-list');
    if (!container) return;
    
    const subscriptions = subscriptionManager.getActiveSubscriptions();
    container.innerHTML = '';
    
    if (subscriptions.length === 0) {
        container.innerHTML = '<div style="color:#666;font-size:12px;padding:8px;">无活跃订阅</div>';
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

// HTML转义
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// 切换标签页
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

// 读取流内容
async function readStreamContent() {
    if (!streamManager.currentStream) return;
    
    try {
        const content = await streamManager.read(streamManager.currentStream.name);
        document.getElementById('editor').value = content;
        updateLastOperation('读取');
    } catch (error) {
        showError('读取失败: ' + error.message);
    }
}

// 写入流内容
async function writeStreamContent() {
    if (!streamManager.currentStream) return;
    
    const content = document.getElementById('editor').value;
    try {
        await streamManager.write(streamManager.currentStream.name, content);
        showSuccess('写入成功');
        updateLastOperation('写入');
    } catch (error) {
        showError('写入失败: ' + error.message);
    }
}

// 追加流内容
async function appendStreamContent() {
    if (!streamManager.currentStream) return;
    
    // 显示模态对话框
    const modal = document.getElementById('append-modal');
    const textarea = document.getElementById('append-textarea');
    textarea.value = '';
    modal.style.display = 'flex';
    textarea.focus();
}

// 确认追加内容
async function confirmAppendContent() {
    if (!streamManager.currentStream) return;
    
    const textarea = document.getElementById('append-textarea');
    const content = textarea.value;
    
    if (content) {
        try {
            await streamManager.append(streamManager.currentStream.name, content);
            await readStreamContent();
            showSuccess('追加成功');
            updateLastOperation('追加');
        } catch (error) {
            showError('追加失败: ' + error.message);
        }
    }
    
    // 关闭模态对话框
    document.getElementById('append-modal').style.display = 'none';
}

// 关闭追加模态对话框
function closeAppendModal() {
    document.getElementById('append-modal').style.display = 'none';
}

// 撤销操作
async function undoStreamContent() {
    if (!streamManager.currentStream) return;
    
    try {
        await streamManager.undo(streamManager.currentStream.name);
        await readStreamContent();
        showSuccess('撤销成功');
        updateLastOperation('撤销');
    } catch (error) {
        showError('撤销失败: ' + error.message);
    }
}

// 搜索流内容
async function searchStreamContent() {
    if (!streamManager.currentStream) return;
    
    const pattern = document.getElementById('search-input').value;
    const regex = document.getElementById('regex-mode').checked;
    
    if (!pattern) {
        showError('请输入搜索内容');
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
        updateLastOperation('搜索');
    } catch (error) {
        showError('搜索失败: ' + error.message);
    }
}

// 渲染搜索结果
function renderSearchResults(matches) {
    const container = document.getElementById('search-results');
    container.innerHTML = '';
    
    const items = matches || [];
    
    if (items.length === 0) {
        container.innerHTML = '<div class="search-result-item">无匹配结果</div>';
        return;
    }
    
    items.forEach((match, index) => {
        const item = document.createElement('div');
        item.className = 'search-result-item';
        item.innerHTML = `
            <div class="search-result-info">
                匹配 #${index + 1} | 位置: ${match.start_pos}-${match.end_pos} | 行: ${match.line}
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

// 加载流元数据
async function loadStreamMetadata(name) {
    try {
        const metadata = await streamManager.getMetadata(name);
        
        document.getElementById('meta-description').value = metadata.description || '';
        
        renderTagsList(metadata.tags || {});
    } catch (error) {
        showError('加载元数据失败: ' + error.message);
    }
}

// 渲染标签列表
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

// 添加标签
async function addTag() {
    if (!streamManager.currentStream) return;
    
    const key = document.getElementById('tag-key').value;
    const value = document.getElementById('tag-value').value;
    
    if (!key || !value) {
        showError('请输入标签键和值');
        return;
    }
    
    try {
        await streamManager.setTag(streamManager.currentStream.name, key, value);
        await loadStreamMetadata(streamManager.currentStream.name);
        document.getElementById('tag-key').value = '';
        document.getElementById('tag-value').value = '';
        showSuccess('标签添加成功');
    } catch (error) {
        showError('添加标签失败: ' + error.message);
    }
}

// 删除标签
async function removeTag(key) {
    if (!streamManager.currentStream) return;
    
    try {
        await streamManager.removeTag(streamManager.currentStream.name, key);
        await loadStreamMetadata(streamManager.currentStream.name);
        showSuccess('标签删除成功');
    } catch (error) {
        showError('删除标签失败: ' + error.message);
    }
}

// 更新描述
async function updateDescription() {
    if (!streamManager.currentStream) return;
    
    const description = document.getElementById('meta-description').value;
    
    try {
        await streamManager.updateDescription(streamManager.currentStream.name, description);
        showSuccess('描述更新成功');
    } catch (error) {
        showError('更新描述失败: ' + error.message);
    }
}

// 重命名流
async function renameStream() {
    if (!streamManager.currentStream) return;
    
    const newName = document.getElementById('new-name').value;
    
    if (!newName) {
        showError('请输入新名称');
        return;
    }
    
    try {
        const oldName = streamManager.currentStream.name;
        await streamManager.rename(oldName, newName);
        
        streamManager.currentStream.name = newName;
        document.getElementById('stream-name').textContent = newName;
        document.getElementById('new-name').value = '';
        
        renderStreamList(streamManager.streams);
        
        showSuccess(`重命名成功: ${oldName} -> ${newName}`);
        updateLastOperation('重命名');
    } catch (error) {
        showError('重命名失败: ' + error.message);
    }
}

// 更新最后操作
function updateLastOperation(op) {
    const now = new Date();
    const timeStr = now.toLocaleTimeString('zh-CN');
    document.getElementById('status-last-op').textContent = `最后操作: ${op} ${timeStr}`;
}

// 订阅流
async function subscribeToStream() {
    if (!streamManager.currentStream) return;
    
    const fromPosition = document.getElementById('from-position').value;
    const position = fromPosition ? parseInt(fromPosition) : null;
    
    try {
        const subscriptionId = await subscriptionManager.subscribe(
            streamManager.currentStream.name,
            position
        );
        
        showSuccess(`订阅成功: ${subscriptionId}`);
        updateLastOperation('订阅');
        renderActiveSubscriptions();
        updateStatusBar();
    } catch (error) {
        showError('订阅失败: ' + error.message);
    }
}

// 取消订阅
async function unsubscribeFromStream() {
    if (!streamManager.currentStream) return;
    
    const subscriptionId = subscriptionManager.getSubscriptionId(streamManager.currentStream.name);
    if (!subscriptionId) {
        showError('当前流没有活跃的订阅');
        return;
    }
    
    try {
        await subscriptionManager.unsubscribe(subscriptionId);
        showSuccess('取消订阅成功');
        updateLastOperation('取消订阅');
        renderActiveSubscriptions();
        updateStatusBar();
    } catch (error) {
        showError('取消订阅失败: ' + error.message);
    }
}

// 删除当前流
async function deleteCurrentStream() {
    if (!streamManager.currentStream) return;

    const name = streamManager.currentStream.name;

    if (!confirm(`确定要删除流 "${name}" 吗？此操作不可撤销。`)) {
        return;
    }

    try {
        await streamManager.delete(name);
        document.getElementById('stream-detail').style.display = 'none';
        document.getElementById('empty-state').style.display = 'flex';
        renderStreamList(streamManager.streams);
        updateStatusBar();
        showSuccess(`流 "${name}" 已删除`);
    } catch (error) {
        showError('删除失败: ' + error.message);
    }
}

// 显示成功提示
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
