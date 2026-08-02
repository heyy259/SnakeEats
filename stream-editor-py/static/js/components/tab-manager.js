class TabManager {
    constructor() {
        this.tabList = document.getElementById('tab-list');
        this.streamList = document.getElementById('stream-list');
        this.createDialog = document.getElementById('create-stream-dialog');
        this._pollingPaused = false;
        this._autoSaveTimer = null;
        this._autoSaveDelay = 5000;
        this._autoSaveEnabled = true;

        this.subscriptionManager = new StreamSubscriptionManager();

        this.setupAutoSave();
        this.setupEventListeners();
    }

    setupAutoSave() {
        // Auto-save 5 seconds after the user stops editing (debounce mode)
        state.on('tabModified', ({ tabId, modified }) => {
            if (!modified) return;
            this._resetAutoSaveTimer(tabId);
        });

        // Also reset when switching tabs (the user may start editing right after)
        state.on('activeTabChanged', (tabId) => {
            if (tabId) this._resetAutoSaveTimer(tabId);
        });
    }

    _resetAutoSaveTimer(tabId) {
        if (this._autoSaveTimer) {
            clearTimeout(this._autoSaveTimer);
        }
        this._autoSaveTimer = setTimeout(() => {
            this._doAutoSave();
        }, this._autoSaveDelay);
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
        // Clear scan indicators on all tabs when scanning stops
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

        // Restore scan position display after re-render
        if (state.scanState && state.scanState.status !== 'idle') {
            const scannedTabId = `${state.scanState.systemId}:${state.scanState.streamName}`;
            const text = state.getEditorState(scannedTabId)?.content || '';
            const pos = state.scanState.currentPosition || 0;
            const { row, col } = charOffsetToRowCol(pos, text);
            this._updateTabScanPosition({ row, col, tabId: scannedTabId });

            // If the scanned tab happens to be the active tab, restore the marker in the editor
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

        // Small scan indicator arrow — lets inactive tabs also show that scanning is in progress
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
                state.setError(`Failed to open stream "${streamName}": ${result.error || 'Unknown error'}`);
            }
        } catch (err) {
            state.setError(`Cannot connect to the system; stream "${streamName}" may be unavailable`);
        }
    }

    handleStreamChange(streamName, change) {
        // change comes from subscription_manager.notify_clients notifications:
        //   New format: {type:'stream.on_change', system_id, stream_name, change: {type, position, text, ...}}
        //   Old format: change wraps another text-stream params layer {name, subscription_id, change: {...}, new_size}
        // Compatible with both: if change.change is {type, ...}, use it directly; otherwise try unwrapping one more layer
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
            // stream.delete (deleting the entire stream) does not trigger a stream.on_change notification,
            // so the 'delete' type always refers to a delete_range content operation, not stream deletion.
            default:
                console.log('Unknown change type:', changeType, 'raw data:', change);
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
                    // Content unchanged → skip, do not touch DOM / cursor (self-notification scenario)
                    if (markdownEditor.getContent() === result.data.content) {
                        return;
                    }
                    markdownEditor.setContent(result.data.content);
                }
            }
        } catch (e) {
            console.error('Failed to update editor content:', e);
        }
    }

    async updateStreamMetadata(streamName) {
        console.log('Stream metadata updated:', streamName);
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
            
            // Update state
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

        // Unsubscribe
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
            state.setError('Failed to create stream: system connection timed out, please check that the system is running');
        } finally {
            this.hideCreateDialog();
        }
    }

    async deleteStream(streamName) {
        if (!confirm(`Are you sure you want to delete stream "${streamName}"? This action cannot be undone.`)) return;

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
        const newName = prompt(`Rename stream "${oldName}":`, oldName);
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

        // Create the metadata editor dialog
        const dialog = document.createElement('div');
        dialog.className = 'dialog-overlay';
        dialog.id = 'metadata-editor-dialog';
        dialog.innerHTML = `
            <div class="dialog">
                <div class="dialog-header">
                    <h2>Edit Stream Metadata - ${streamName}</h2>
                    <button class="dialog-close" id="close-metadata-editor">×</button>
                </div>
                <div class="dialog-content">
                    <div class="form-group">
                        <label for="metadata-description">Description</label>
                        <textarea id="metadata-description" rows="3" placeholder="Stream description">${stream.description || ''}</textarea>
                    </div>
                    <div class="form-group">
                        <label>Tags</label>
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
                            <button class="btn btn-sm btn-primary" id="add-tag-btn">Add</button>
                        </div>
                    </div>
                </div>
                <div class="dialog-footer">
                    <button class="btn btn-secondary" id="cancel-metadata-editor">Cancel</button>
                    <button class="btn btn-primary" id="save-metadata-editor">Save</button>
                </div>
            </div>
        `;

        document.body.appendChild(dialog);

        // Bind events
        document.getElementById('close-metadata-editor').addEventListener('click', () => dialog.remove());
        document.getElementById('cancel-metadata-editor').addEventListener('click', () => dialog.remove());

        // Add tag
        document.getElementById('add-tag-btn').addEventListener('click', async () => {
            const key = document.getElementById('new-tag-key').value.trim();
            const value = document.getElementById('new-tag-value').value.trim();
            if (!key) return;

            const result = await api.setStreamTag(state.activeSystemId, streamName, key, value);
            if (result.success) {
                // Add to the list
                const tagsList = document.getElementById('metadata-tags-list');
                const tagItem = document.createElement('div');
                tagItem.className = 'metadata-tag-item';
                tagItem.dataset.key = key;
                tagItem.innerHTML = `
                    <span class="tag-key">${key}</span>: <span class="tag-value">${value}</span>
                    <button class="btn btn-sm btn-danger remove-tag-btn" data-key="${key}">×</button>
                `;
                tagsList.appendChild(tagItem);

                // Bind delete event
                tagItem.querySelector('.remove-tag-btn').addEventListener('click', async () => {
                    await this.removeTag(streamName, key, tagItem);
                });

                // Clear inputs
                document.getElementById('new-tag-key').value = '';
                document.getElementById('new-tag-value').value = '';
            } else {
                state.setError(result.error || 'Failed to add tag');
            }
        });

        // Delete tag
        dialog.querySelectorAll('.remove-tag-btn').forEach(btn => {
            btn.addEventListener('click', async () => {
                const key = btn.dataset.key;
                const tagItem = btn.closest('.metadata-tag-item');
                await this.removeTag(streamName, key, tagItem);
            });
        });

        // Save description
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
