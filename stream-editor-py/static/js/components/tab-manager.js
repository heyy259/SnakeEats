class TabManager {
    constructor() {
        this.tabList = document.getElementById('tab-list');
        this.streamList = document.getElementById('stream-list');
        this.createDialog = document.getElementById('create-stream-dialog');
        this._pollingPaused = false;
        this._autoSaveInterval = null;
        this._autoSaveEnabled = true;

        this.subscriptionManager = new StreamSubscriptionManager();

        this.setupAutoSave();
        this.setupEventListeners();
    }

    setupAutoSave() {
        this._autoSaveInterval = setInterval(() => {
            this._doAutoSave();
        }, 5000);
    }

    async _doAutoSave() {
        if (!this._autoSaveEnabled) return;

        // Global scan state gate: block auto-save for ALL tabs during scanning
        if (state.scanState && state.scanState.status &&
            state.scanState.status.toLowerCase() === 'scanning') {
            return;
        }

        const tab = state.getActiveTab();
        if (!tab || tab.isLocked) return;
        if (!tab.isModified) return;
        try {
            await fileManager.saveStream();
        } catch (e) {
            console.error('Auto-save failed:', e);
        }
    }

    setupEventListeners() {
        document.getElementById('btn-create-stream').addEventListener('click', () => this.showCreateDialog());
        document.getElementById('close-create-stream').addEventListener('click', () => this.hideCreateDialog());
        document.getElementById('cancel-create-stream').addEventListener('click', () => this.hideCreateDialog());
        document.getElementById('confirm-create-stream').addEventListener('click', () => this.createStream());

        state.on('activeSystemChanged', (systemId) => {
            this._stopPolling();
            this.loadStreams();
            if (systemId) {
                const wsPort = window.WEBSOCKET_PORT || 6000;
                this.subscriptionManager.connect(wsPort, systemId);
                if (!this.subscriptionManager.isConnected()) {
                    this._startPolling(systemId);
                }
            }
        });
        state.on('streamsChanged', () => this.renderStreamList());
        state.on('tabsChanged', () => this.renderTabs());
        state.on('activeTabChanged', () => this.renderTabs());
        state.on('tabModified', () => this.renderTabs());
        state.on('scanPositionForTab', (data) => this._updateTabScanPosition(data));
        // 扫描停止时清除所有tab上的扫描指示器
        state.on('scanStateChanged', (scanState) => {
            if (!scanState || (scanState.status || '').toLowerCase() === 'idle') {
                this._clearAllScanIndicators();
            }
        });
    }

    async loadStreams() {
        const systemId = state.activeSystemId;
        if (!systemId) return;

        try {
            const result = await api.getStreams(systemId);
            if (result.success) {
                const oldStreams = state.streamSnapshots[systemId] || [];
                const newStreams = result.data;
                const changes = this._detectChanges(oldStreams, newStreams);
                state.streamSnapshots[systemId] = newStreams;
                state.setStreams(systemId, newStreams);
                if (changes.hasChanges) {
                    this._handleChanges(changes);
                }
            }
        } catch (e) {
            console.warn('[TabManager] loadStreams failed:', e.message);
        }
    }

    _startPolling(systemId) {
        this._stopPolling(systemId);
        if (this.subscriptionManager && this.subscriptionManager.isConnected()) {
            return;
        }
        state.streamPollTimers[systemId] = setInterval(() => {
            if (!this._pollingPaused) this.loadStreams();
        }, 5000);
    }

    _stopPolling(systemId) {
        if (systemId) {
            if (state.streamPollTimers[systemId]) {
                clearInterval(state.streamPollTimers[systemId]);
                delete state.streamPollTimers[systemId];
            }
        } else {
            Object.keys(state.streamPollTimers).forEach(id => {
                clearInterval(state.streamPollTimers[id]);
                delete state.streamPollTimers[id];
            });
        }
    }

    pausePolling() {
        this._pollingPaused = true;
    }

    resumePolling() {
        this._pollingPaused = false;
    }

    _detectChanges(oldStreams, newStreams) {
        const oldMap = {};
        oldStreams.forEach(s => { oldMap[s.name] = s; });
        const newMap = {};
        newStreams.forEach(s => { newMap[s.name] = s; });

        const created = newStreams.filter(s => !oldMap[s.name]);
        const deleted = oldStreams.filter(s => !newMap[s.name]);
        const modified = newStreams.filter(s => {
            const old = oldMap[s.name];
            if (!old) return false;
            return (
                old.version !== s.version ||
                old.content_length !== s.content_length ||
                old.description !== s.description ||
                JSON.stringify(old.tags || {}) !== JSON.stringify(s.tags || {})
            );
        });

        return {
            hasChanges: created.length > 0 || deleted.length > 0 || modified.length > 0,
            created, deleted, modified
        };
    }

    _handleChanges(changes) {
        changes.deleted.forEach(stream => {
            const tabId = `${state.activeSystemId}:${stream.name}`;
            const tab = state.tabs.find(t => t.id === tabId);
            if (tab) {
                state.removeTab(tabId);
                this.updateEditor();
            }
        });
    }

    renderStreamList() {
        const systemId = state.activeSystemId;
        const streams = state.getStreams(systemId);

        if (!systemId) {
            this.streamList.innerHTML = '<div class="empty-state"><p>Select a system to view streams</p></div>';
            return;
        }

        if (streams.length === 0) {
            this.streamList.innerHTML = '<div class="empty-state"><p>No streams available</p></div>';
            return;
        }

        this.streamList.innerHTML = `<div class="stream-items">${streams.map(stream => `
            <div class="stream-item ${stream.name === state.activeStreamName ? 'active' : ''}" data-stream-name="${stream.name}">
                <div class="stream-item-header">
                    <span class="stream-name">${stream.name}</span>
                    <span class="stream-version">v${stream.version || '1'}</span>
                    <div class="stream-actions">
                        <button class="btn btn-sm btn-icon" data-action="edit-metadata" title="Edit metadata">📝</button>
                        <button class="btn btn-sm btn-icon" data-action="rename" title="Rename">✏️</button>
                        <button class="btn btn-sm btn-icon btn-danger" data-action="delete" title="Delete">🗑</button>
                    </div>
                </div>
                <div class="stream-description">${stream.description || 'No description'}</div>
                <div class="stream-meta">
                    <span>${this.formatLength(stream.content_length)}</span>
                    <div class="stream-tags">
                        ${Object.entries(stream.tags || {}).map(([key, val]) => `<span class="stream-tag">${key}: ${val}</span>`).join('')}
                    </div>
                </div>
            </div>
        `).join('')}</div>`;

        this.streamList.querySelectorAll('.stream-item').forEach(item => {
            item.addEventListener('click', (e) => {
                if (!e.target.closest('.stream-actions')) {
                    this.openStream(item.dataset.streamName);
                }
            });
        });

        this.streamList.querySelectorAll('[data-action]').forEach(btn => {
            btn.addEventListener('click', (e) => {
                e.stopPropagation();
                const item = btn.closest('.stream-item');
                const streamName = item.dataset.streamName;
                const action = btn.dataset.action;

                if (action === 'delete') this.deleteStream(streamName);
                else if (action === 'rename') this.renameStream(streamName);
                else if (action === 'edit-metadata') this.showMetadataEditor(streamName);
            });
        });
    }

    renderTabs() {
        const tabs = state.tabs;
        const activeTabId = state.activeTabId;

        if (tabs.length === 0) {
            this.tabList.innerHTML = '';
            return;
        }

        this.tabList.innerHTML = tabs.map(tab => `
            <div class="tab-item ${tab.id === activeTabId ? 'active' : ''} ${tab.isLocked ? 'locked' : ''}" data-tab-id="${tab.id}">
                <span class="tab-title">${tab.streamName}</span>
                ${tab.isLocked ? '<span class="tab-lock-icon">🔒</span>' : ''}
                ${tab.isModified ? '<span class="tab-modified-dot"></span>' : ''}
                <button class="tab-close-btn" data-tab-id="${tab.id}" title="Close tab">×</button>
            </div>
        `).join('');

        this.tabList.querySelectorAll('.tab-item').forEach(item => {
            item.addEventListener('click', (e) => {
                if (!e.target.closest('.tab-close-btn')) {
                    state.setActiveTab(item.dataset.tabId);
                    this.updateEditor();
                }
            });
        });

        this.tabList.querySelectorAll('.tab-close-btn').forEach(btn => {
            btn.addEventListener('click', (e) => {
                e.stopPropagation();
                this.closeTab(btn.dataset.tabId);
            });
        });

        const activeTab = state.getActiveTab();
        document.getElementById('active-stream-name').textContent = activeTab ? activeTab.streamName : '';
        document.getElementById('modified-indicator').style.display = activeTab?.isModified ? 'inline' : 'none';
        document.getElementById('btn-save').disabled = !activeTab;
        document.getElementById('btn-save-as').disabled = !activeTab;

        // 重渲染后恢复扫描位置显示
        if (state.scanState && state.scanState.status !== 'idle') {
            const scannedTabId = `${state.scanState.systemId}:${state.scanState.streamName}`;
            const text = state.getEditorState(scannedTabId)?.content || '';
            const pos = state.scanState.currentPosition || 0;
            const { row, col } = charOffsetToRowCol(pos, text);
            this._updateTabScanPosition({ row, col, tabId: scannedTabId });

            // 如果扫描目标tab恰好是激活tab，恢复编辑器中的标记
            if (scannedTabId === state.activeTabId && typeof markdownEditor !== 'undefined') {
                setTimeout(() => {
                    if (state.scanState && state.scanState.status !== 'idle') {
                        markdownEditor.showScanPosition(state.scanState.currentPosition || 0);
                    }
                }, 0);
            }
        }
    }

    _updateTabScanPosition({ row, col, tabId }) {
        if (!tabId) return;
        const tabEl = this.tabList.querySelector(`[data-tab-id="${CSS.escape(tabId)}"]`);
        if (!tabEl) return;

        let posEl = tabEl.querySelector('.tab-scan-pos');
        if (!posEl) {
            posEl = document.createElement('span');
            posEl.className = 'tab-scan-pos';
            const titleEl = tabEl.querySelector('.tab-title');
            if (titleEl) {
                titleEl.after(posEl);
            } else {
                tabEl.insertBefore(posEl, tabEl.firstChild);
            }
        }
        posEl.textContent = `Ln ${row}, Col ${col}`;

        // 扫描指示小箭头 — 让非激活tab也能看出扫描在进行
        let indicatorEl = tabEl.querySelector('.tab-scan-indicator');
        if (!indicatorEl) {
            indicatorEl = document.createElement('span');
            indicatorEl.className = 'tab-scan-indicator';
            indicatorEl.textContent = '▶';
            tabEl.appendChild(indicatorEl);
        }
    }

    _clearAllScanIndicators() {
        this.tabList.querySelectorAll('.tab-scan-pos').forEach(el => el.remove());
        this.tabList.querySelectorAll('.tab-scan-indicator').forEach(el => el.remove());
    }

    async openStream(streamName) {
        const systemId = state.activeSystemId;
        if (!systemId) return;

        const tabId = `${systemId}:${streamName}`;
        const existing = state.tabs.find(t => t.id === tabId);
        if (existing) {
            state.setActiveTab(tabId);
            this.updateEditor();
            return;
        }

        try {
            this.subscriptionManager.subscribe(systemId, streamName, (change) => {
                this.handleStreamChange(streamName, change);
            });

            const result = await api.getStreamContent(systemId, streamName);
            if (result.success) {
                const tab = {
                    id: tabId,
                    systemId,
                    streamName,
                    isModified: false,
                    isLocked: false
                };
                state.addTab(tab);
                state.updateEditorState(tabId, { content: result.data.content });
                this.updateEditor();
            } else {
                state.setError(`无法打开流 "${streamName}": ${result.error || '未知错误'}`);
            }
        } catch (err) {
            state.setError(`无法连接系统，流 "${streamName}" 可能不可用`);
        }
    }

    handleStreamChange(streamName, change) {
        // change 来自 subscription_manager.notify_clients 的通知:
        //   新格式: {type:'stream.on_change', system_id, stream_name, change: {type, position, text, ...}}
        //   旧格式: change 字段内还有一层 text-stream params 包装 {name, subscription_id, change: {...}, new_size}
        // 兼容两种: 如果 change.change 是 {type, ...} 就直接用，否则尝试再解一层
        const inner = change.change || {};
        const actualChange = (inner.type !== undefined) ? inner : (inner.change || {});
        const changeType = actualChange.type;

        switch (changeType) {
            case 'set':          // stream.write() → full-text update
            case 'write':
            case 'append':
            case 'prepend':
            case 'insert':
            case 'delete_range':
            case 'delete':       // text-stream uses "delete" for delete_range (NOT stream deletion)
            case 'replace_range':
                this.updateEditorContent(streamName);
                break;
            case 'tags':
            case 'set_tag':
            case 'remove_tag':
                {
                    const tabId = `${state.activeSystemId}:${streamName}`;
                    if (state.activeTabId === tabId) {
                        this.updateStreamMetadata(streamName);
                    }
                }
                break;
            case 'rename':
                this.updateStreamName(streamName, actualChange.new_name);
                break;
            // stream.delete (删除整个流) 不触发 stream.on_change 通知，
            // 所以 'delete' 类型永远指向 delete_range 内容操作，不是流删除。
            default:
                console.log('未知变化类型:', changeType, '原始数据:', change);
        }
    }

    async updateEditorContent(streamName) {
        const tabId = `${state.activeSystemId}:${streamName}`;
        
        try {
            const result = await api.getStreamContent(state.activeSystemId, streamName);
            
            if (result.success) {
                state.updateEditorState(tabId, { content: result.data.content });
                
                const tab = state.tabs.find(t => t.id === tabId);
                if (tab && tab.isModified) {
                    state.markTabModified(tabId, false);
                }
                
                if (state.activeTabId === tabId && typeof markdownEditor !== 'undefined') {
                    markdownEditor.setContent(result.data.content);
                }
            }
        } catch (e) {
            console.error('更新编辑器内容失败:', e);
        }
    }

    async updateStreamMetadata(streamName) {
        console.log('流元数据已更新:', streamName);
        await this.loadStreams();
    }

    updateStreamName(oldName, newName) {
        const tabId = `${state.activeSystemId}:${oldName}`;
        const tab = state.tabs.find(t => t.id === tabId);
        
        if (tab) {
            const newTabId = `${state.activeSystemId}:${newName}`;
            tab.id = newTabId;
            tab.streamName = newName;

            const callback = this.subscriptionManager.callbacks.get(oldName);
            if (callback) {
                this.subscriptionManager.callbacks.delete(oldName);
                this.subscriptionManager.callbacks.set(newName, callback);
            }
            
            // 更新状态
            state.emit('tabsChanged', state.tabs);
        }
    }

    closeTab(tabId) {
        const tab = state.tabs.find(t => t.id === tabId);
        if (tab?.isLocked) {
            state.setError('Cannot close a locked tab (scanning in progress)');
            return;
        }

        if (tab?.isModified) {
            if (!confirm('This stream has unsaved changes. Close anyway?')) return;
        }

        // 取消订阅
        if (tab) {
            this.subscriptionManager.unsubscribe(tab.systemId, tab.streamName);
        }

        state.removeTab(tabId);
        this.updateEditor();
    }

    updateEditor() {
        const activeTab = state.getActiveTab();
        const placeholder = document.getElementById('editor-placeholder');
        const editor = document.getElementById('editor');

        if (activeTab) {
            placeholder.style.display = 'none';
            editor.style.display = 'block';
            const editorState = state.getEditorState(activeTab.id);
            if (typeof markdownEditor !== 'undefined') {
                markdownEditor.setContent(editorState.content);
            }
        } else {
            placeholder.style.display = 'flex';
            editor.style.display = 'none';
        }
    }

    showCreateDialog() {
        if (!state.activeSystemId) {
            state.setError('Please select a system first');
            return;
        }
        this.createDialog.style.display = 'flex';
        const nameInput = document.getElementById('stream-name');
        const descInput = document.getElementById('stream-description');
        nameInput.focus();

        // Enter key support for both inputs
        const onEnter = (e) => {
            if (e.key === 'Enter') {
                e.preventDefault();
                this.createStream();
            }
        };
        nameInput.addEventListener('keydown', onEnter);
        descInput.addEventListener('keydown', onEnter);

        // Cleanup listeners on next show
        this._enterListeners = { nameInput, descInput, handler: onEnter };
    }

    hideCreateDialog() {
        this.createDialog.style.display = 'none';
    }

    async createStream() {
        const name = document.getElementById('stream-name').value.trim();
        const description = document.getElementById('stream-description').value.trim();

        if (!name) {
            state.setError('Stream name is required');
            return;
        }

        try {
            const result = await api.createStream(state.activeSystemId, { name, description });
            if (result.success) {
                await this.loadStreams();
                await this.openStream(name);
                document.getElementById('stream-name').value = '';
                document.getElementById('stream-description').value = '';
            } else {
                state.setError(result.error || 'Failed to create stream');
            }
        } catch (err) {
            state.setError('创建流失败: 系统连接超时，请检查系统是否正常运行');
        } finally {
            this.hideCreateDialog();
        }
    }

    async deleteStream(streamName) {
        if (!confirm(`确定删除流 "${streamName}" 吗？此操作不可恢复。`)) return;

        state.setLoading(true);
        try {
            const result = await api.deleteStream(state.activeSystemId, streamName);
            if (result.success) {
                const tabId = `${state.activeSystemId}:${streamName}`;
                state.removeTab(tabId);
                await this.loadStreams();
                this.updateEditor();
            } else {
                state.setError(result.error || 'Failed to delete stream');
            }
        } catch (e) {
            state.setError('Delete failed: ' + e.message);
        } finally {
            state.setLoading(false);
        }
    }

    async renameStream(oldName) {
        const newName = prompt(`重命名流 "${oldName}":`, oldName);
        if (!newName || newName === oldName) return;

        state.setLoading(true);
        try {
            const result = await api.renameStream(state.activeSystemId, oldName, newName);
            if (result.success) {
                const oldTabId = `${state.activeSystemId}:${oldName}`;
                const newTabId = `${state.activeSystemId}:${newName}`;
                const tab = state.tabs.find(t => t.id === oldTabId);
                if (tab) {
                    tab.id = newTabId;
                    tab.streamName = newName;
                    state.emit('tabsChanged', state.tabs);
                }
                if (state.activeTabId === oldTabId) {
                    state.activeTabId = newTabId;
                }
                if (state.editorStates[oldTabId]) {
                    state.editorStates[newTabId] = state.editorStates[oldTabId];
                    delete state.editorStates[oldTabId];
                }
                if (state.activeStreamName === oldName) {
                    state.activeStreamName = newName;
                }
                await this.loadStreams();
                this.renderTabs();
                this.updateEditor();
            } else {
                state.setError(result.error || 'Failed to rename stream');
            }
        } catch (e) {
            state.setError('Rename failed: ' + e.message);
        } finally {
            state.setLoading(false);
        }
    }

    showMetadataEditor(streamName) {
        const streams = state.getStreams(state.activeSystemId);
        const stream = streams.find(s => s.name === streamName);
        if (!stream) return;

        // 创建元数据编辑对话框
        const dialog = document.createElement('div');
        dialog.className = 'dialog-overlay';
        dialog.id = 'metadata-editor-dialog';
        dialog.innerHTML = `
            <div class="dialog">
                <div class="dialog-header">
                    <h2>编辑流元数据 - ${streamName}</h2>
                    <button class="dialog-close" id="close-metadata-editor">×</button>
                </div>
                <div class="dialog-content">
                    <div class="form-group">
                        <label for="metadata-description">描述</label>
                        <textarea id="metadata-description" rows="3" placeholder="流描述">${stream.description || ''}</textarea>
                    </div>
                    <div class="form-group">
                        <label>标签</label>
                        <div id="metadata-tags-list" class="metadata-tags-list">
                            ${Object.entries(stream.tags).map(([key, val]) => `
                                <div class="metadata-tag-item" data-key="${key}">
                                    <span class="tag-key">${key}</span>: <span class="tag-value">${val}</span>
                                    <button class="btn btn-sm btn-danger remove-tag-btn" data-key="${key}">×</button>
                                </div>
                            `).join('')}
                        </div>
                        <div class="add-tag-form">
                            <input type="text" id="new-tag-key" placeholder="Key" class="tag-input">
                            <input type="text" id="new-tag-value" placeholder="Value" class="tag-input">
                            <button class="btn btn-sm btn-primary" id="add-tag-btn">添加</button>
                        </div>
                    </div>
                </div>
                <div class="dialog-footer">
                    <button class="btn btn-secondary" id="cancel-metadata-editor">取消</button>
                    <button class="btn btn-primary" id="save-metadata-editor">保存</button>
                </div>
            </div>
        `;

        document.body.appendChild(dialog);

        // 绑定事件
        document.getElementById('close-metadata-editor').addEventListener('click', () => dialog.remove());
        document.getElementById('cancel-metadata-editor').addEventListener('click', () => dialog.remove());

        // 添加标签
        document.getElementById('add-tag-btn').addEventListener('click', async () => {
            const key = document.getElementById('new-tag-key').value.trim();
            const value = document.getElementById('new-tag-value').value.trim();
            if (!key) return;

            const result = await api.setStreamTag(state.activeSystemId, streamName, key, value);
            if (result.success) {
                // 添加到列表
                const tagsList = document.getElementById('metadata-tags-list');
                const tagItem = document.createElement('div');
                tagItem.className = 'metadata-tag-item';
                tagItem.dataset.key = key;
                tagItem.innerHTML = `
                    <span class="tag-key">${key}</span>: <span class="tag-value">${value}</span>
                    <button class="btn btn-sm btn-danger remove-tag-btn" data-key="${key}">×</button>
                `;
                tagsList.appendChild(tagItem);

                // 绑定删除事件
                tagItem.querySelector('.remove-tag-btn').addEventListener('click', async () => {
                    await this.removeTag(streamName, key, tagItem);
                });

                // 清空输入
                document.getElementById('new-tag-key').value = '';
                document.getElementById('new-tag-value').value = '';
            } else {
                state.setError(result.error || 'Failed to add tag');
            }
        });

        // 删除标签
        dialog.querySelectorAll('.remove-tag-btn').forEach(btn => {
            btn.addEventListener('click', async () => {
                const key = btn.dataset.key;
                const tagItem = btn.closest('.metadata-tag-item');
                await this.removeTag(streamName, key, tagItem);
            });
        });

        // 保存描述
        document.getElementById('save-metadata-editor').addEventListener('click', async () => {
            const description = document.getElementById('metadata-description').value;
            const result = await api.updateStreamDescription(state.activeSystemId, streamName, description);
            if (result.success) {
                await this.loadStreams();
                dialog.remove();
            } else {
                state.setError(result.error || 'Failed to update description');
            }
        });
    }

    async removeTag(streamName, key, tagItem) {
        const result = await api.removeStreamTag(state.activeSystemId, streamName, key);
        if (result.success) {
            tagItem.remove();
            await this.loadStreams();
        } else {
            state.setError(result.error || 'Failed to remove tag');
        }
    }

    formatLength(length) {
        if (length < 1024) return `${length} B`;
        if (length < 1024 * 1024) return `${(length / 1024).toFixed(1)} KB`;
        return `${(length / (1024 * 1024)).toFixed(1)} MB`;
    }
}

const tabManager = new TabManager();
