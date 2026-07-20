class ScanControl {
    constructor() {
        this.statusDot = document.getElementById('scan-status-dot');
        this.statusText = document.getElementById('scan-status-text');
        this.btnStart = document.getElementById('btn-scan-start');
        this.btnPause = document.getElementById('btn-scan-pause');
        this.btnResume = document.getElementById('btn-scan-resume');
        this.btnStop = document.getElementById('btn-scan-stop');
        this.positionInfo = document.getElementById('scan-position-info');
        this.posRowCol = document.getElementById('scan-pos-rowcol');
        this.posBefore = document.getElementById('scan-pos-before');
        this.posAfter = document.getElementById('scan-pos-after');
        this.previewRowCol = document.getElementById('preview-rowcol');
        this.previewBefore = document.getElementById('preview-before');
        this.previewAfter = document.getElementById('preview-after');
        this.positionInput = document.getElementById('scan-position');
        this.btnSetPosition = document.getElementById('btn-set-position');
        this.setupEventListeners();
    }

    setupEventListeners() {
        this.btnStart.addEventListener('click', () => this.startScan());
        this.btnPause.addEventListener('click', () => this.pauseScan());
        this.btnResume.addEventListener('click', () => this.resumeScan());
        this.btnStop.addEventListener('click', () => this.stopScan());
        this.btnSetPosition.addEventListener('click', () => this.setPosition());
        this.positionInput.addEventListener('input', () => this._updatePositionPreview());

        state.on('scanStateChanged', (scanState) => this.updateUI(scanState));
        state.on('activeTabChanged', () => {
            this.updateButtons();
            this._onActiveTabChanged();
        });

        // 监听 WebSocket 推送的扫描事件（替代轮询）
        state.on('scanWSStateChange', (data) => this._onScanStateChanged(data));
        state.on('scanPositionUpdated', (data) => this._onScanPositionUpdated(data));
        state.on('scanCompleted', (change) => this._handleScanCompleted(change));
        state.on('scanError', (change) => this._handleScanError(change));

        // 恢复后端扫描状态 — 页面加载时 / WebSocket 重连后
        state.on('activeSystemChanged', () => this.checkBackendState());
        state.on('wsReconnected', () => this.checkBackendState());
    }

    updateUI(scanState) {
        if (!scanState) {
            this.statusDot.className = 'status-dot idle';
            this.statusText.textContent = 'IDLE';
            this.positionInfo.style.display = 'none';
            this._updateScanStreamDisplay(null);
            if (typeof markdownEditor !== 'undefined') {
                markdownEditor.removeScanPosition();
            }
            this.updateButtons();
            return;
        }

        const status = (scanState.status || 'idle').toLowerCase();
        this.statusDot.className = `status-dot ${status}`;
        this.statusText.textContent = scanState.status || 'IDLE';

        if (scanState.totalMarkersFound != null) {
            this.statusText.textContent +=
                ` (${scanState.normalSucceeded || 0} ok, ${scanState.normalFailed || 0} fail)`;
        }

        if (status === 'scanning' || status === 'paused') {
            this.positionInfo.style.display = 'block';
            const pos = scanState.currentPosition || 0;

            // 使用扫描目标tab内容做行/列换算（不是激活tab）
            const text = this._getScannedTabContent();
            const { row, col } = charOffsetToRowCol(pos, text);
            this.posRowCol.textContent = `Ln ${row}, Col ${col}`;

            const { before, after } = getSurroundingChars(pos, text, 5);
            this.posBefore.textContent = before;
            this.posAfter.textContent = after;

            // 仅当扫描目标tab是当前激活tab时才在编辑器显示标记
            const scannedTabId = this._getScannedTabId();
            const activeTab = state.getActiveTab();
            if (typeof markdownEditor !== 'undefined') {
                if (activeTab && activeTab.id === scannedTabId) {
                    markdownEditor.showScanPosition(pos);
                } else {
                    markdownEditor.removeScanPosition();
                }
            }

            // 推送到tab标签页显示（不再节流——WS推送已是300ms节流）
            if (scannedTabId) {
                state.emit('scanPositionForTab', { row, col, tabId: scannedTabId });
            }

            // 显示扫描目标流名
            this._updateScanStreamDisplay(scanState.streamName);
        } else {
            this.positionInfo.style.display = 'none';
        }

        this.updateButtons();
    }

    _startPositionPolling() {
        this._stopPositionPolling();
        state.scanPositionTimer = setInterval(async () => {
            try {
                const result = await api.getScanStatus();
                if (result.success && state.scanState) {
                    const data = result.data;
                    const prevStatus = state.scanState.status;
                    state.scanState.currentPosition = data.currentPosition || 0;
                    state.scanState.status = (data.status || state.scanState.status).toLowerCase();
                    state.scanState.totalMarkersFound = data.totalMarkersFound;
                    state.scanState.normalSucceeded = data.normalSucceeded;
                    state.scanState.normalFailed = data.normalFailed;
                    this.updateUI(state.scanState);

                    if (prevStatus.toLowerCase() === 'scanning' && data.status.toLowerCase() === 'idle') {
                        this._handleScanCompleted();
                    }
                }
            } catch (e) {
                console.warn('[ScanControl] poll error:', e.message);
            }
        }, 1000);
    }

    _stopPositionPolling() {
        if (state.scanPositionTimer) {
            clearInterval(state.scanPositionTimer);
            state.scanPositionTimer = null;
        }
    }

    _handleScanCompleted(change) {
        this._stopPositionPolling();

        // 更新最终统计（如果 scan_complete 事件携带了数据）
        if (change && state.scanState) {
            state.scanState.normalSucceeded = change.normal_succeeded;
            state.scanState.normalFailed = change.normal_failed;
            state.scanState.totalMarkersFound = change.total_markers;
        }

        // Save scanned tab info BEFORE clearing scanState
        const scannedSystemId = state.scanState?.systemId;
        const scannedStreamName = state.scanState?.streamName;
        const scannedTabId = scannedSystemId && scannedStreamName
            ? `${scannedSystemId}:${scannedStreamName}`
            : null;

        state.setScanState(null);

        // Unlock the SCANNED tab, not the currently active tab
        if (scannedTabId) {
            state.lockTab(scannedTabId, false);
            state.markTabModified(scannedTabId, false);
            if (typeof tabManager !== 'undefined') {
                tabManager.updateEditorContent(scannedStreamName).catch(e => {
                    console.warn('[ScanControl] Failed to refresh content after scan:', e);
                });
            }
        }
        if (typeof markdownEditor !== 'undefined') {
            markdownEditor.removeScanPosition();
        }
    }

    updateButtons() {
        const activeTab = state.getActiveTab();
        const scanState = state.scanState;
        const rawStatus = scanState?.status || 'idle';
        // Normalize case: backend may return "IDLE"/"idle", "SCANNING"/"scanning", etc.
        const status = rawStatus.toLowerCase();

        this.btnStart.disabled = !activeTab || status !== 'idle';
        this.btnPause.disabled = status !== 'scanning';
        this.btnResume.disabled = status !== 'paused';
        this.btnStop.disabled = status !== 'scanning' && status !== 'paused';
        this.positionInput.disabled = status === 'idle';
        this.btnSetPosition.disabled = status === 'idle';
    }

    _onScanStateChanged(data) {
        if (!state.scanState) return;
        const newStatus = data.status?.toLowerCase() || 'idle';
        state.scanState.status = newStatus;
        // 直接调用 updateUI，不要 emit 'scanStateChanged'（会导致无限递归）
        this.updateUI(state.scanState);
        this.updateButtons();
    }

    _onScanPositionUpdated(data) {
        if (!state.scanState) return;
        state.scanState.currentPosition = data.position || 0;
        // totalSize 不再使用 — totalLength 已按用户要求清理
        this.updateUI(state.scanState);
    }

    _handleScanError(change) {
        console.error('[ScanControl] Scan error:', change.error_message);
        this.showToast('Scan error: ' + (change.error_message || 'Unknown'), 'error');
    }

    async checkBackendState() {
        try {
            const result = await api.getScanStatus();
            if (!result.success) return;

            const data = result.data;
            const status = (data.status || 'idle').toLowerCase();

            if (status === 'idle') return;

            if (state.scanState && state.scanState.status === status
                && state.scanState.systemId === data.systemId
                && state.scanState.streamName === data.streamName) {
                return;
            }

            console.log('[ScanControl] Restoring scan state from backend:',
                status, data.systemId, data.streamName);

            state.setScanState({
                systemId: data.systemId,
                streamName: data.streamName,
                status: status,
                currentPosition: data.currentPosition || 0,
                startedAt: new Date().toISOString(),
                totalMarkersFound: data.totalMarkersFound,
                normalSucceeded: data.normalSucceeded,
                normalFailed: data.normalFailed,
            });

            if (data.systemId && data.streamName) {
                const tabId = `${data.systemId}:${data.streamName}`;
                if (status === 'scanning') {
                    state.lockTab(tabId, true);
                    state.markTabModified(tabId, false);
                }
            }

            if (status === 'scanning') {
                if (typeof tabManager !== 'undefined' && tabManager.subscriptionManager?.isConnected()) {
                    console.log('[ScanControl] WebSocket connected — using push for restored scan');
                } else {
                    this._startPositionPolling();
                }
            }
        } catch (e) {
            console.warn('[ScanControl] Failed to check backend scan state:', e.message);
        }
    }

    async startScan() {
        const activeTab = state.getActiveTab();
        if (!activeTab) {
            this.showToast('No active tab open', 'warning');
            return;
        }

        console.log('[ScanControl] Starting scan for:', activeTab.systemId, activeTab.streamName);
        const result = await api.startScan(activeTab.systemId, activeTab.streamName);
        console.log('[ScanControl] startScan result:', result);
        
        if (result.success) {
            state.setScanState({
                systemId: activeTab.systemId,
                streamName: activeTab.streamName,
                status: (result.data.status || 'scanning').toLowerCase(),
                currentPosition: 0,
                startedAt: new Date().toISOString()
            });
            state.lockTab(activeTab.id, true);
            state.markTabModified(activeTab.id, false);
            // 如果 WebSocket 连接可用，依赖推送；否则回退到轮询
            if (typeof tabManager !== 'undefined' && tabManager.subscriptionManager?.isConnected()) {
                console.log('[ScanControl] Using WebSocket push for scan updates');
            } else {
                console.warn('[ScanControl] WebSocket not connected, falling back to polling');
                this._startPositionPolling();
            }
            this.showToast('Scan started on: ' + activeTab.streamName, 'success');
        } else {
            const errMsg = result.error || 'Unknown error';
            console.error('[ScanControl] Failed to start scan:', errMsg);
            this.showToast('Scan failed: ' + errMsg, 'error');
        }
    }

    async pauseScan() {
        if (!state.getActiveTab()) return;

        const result = await api.pauseScan();
        if (result.success && state.scanState) {
            state.scanState.status = (result.data.status || 'paused').toLowerCase();
            state.emit('scanStateChanged', state.scanState);
            // Unlock the SCANNED tab, not the currently active tab
            if (state.scanState.systemId && state.scanState.streamName) {
                const scannedTabId = `${state.scanState.systemId}:${state.scanState.streamName}`;
                state.lockTab(scannedTabId, false);
            }
        }
    }

    async resumeScan() {
        if (!state.getActiveTab()) return;

        const result = await api.resumeScan();
        if (result.success && state.scanState) {
            state.scanState.status = (result.data.status || 'scanning').toLowerCase();
            state.emit('scanStateChanged', state.scanState);
            // Lock the SCANNED tab, not the currently active tab
            if (state.scanState.systemId && state.scanState.streamName) {
                const scannedTabId = `${state.scanState.systemId}:${state.scanState.streamName}`;
                state.lockTab(scannedTabId, true);
                state.markTabModified(scannedTabId, false);
            }
        }
    }

    async stopScan() {
        if (!state.getActiveTab()) return;

        const result = await api.stopScan();
        if (result.success) {
            this._handleScanCompleted();
        }
    }

    async setPosition() {
        const position = parseInt(this.positionInput.value);
        if (isNaN(position) || position < 0 || !state.scanState) return;

        const result = await api.setScanPosition(position);
        if (result.success) {
            state.scanState.currentPosition = position;
            this.updateUI(state.scanState);
            this.showToast('扫描位置已设置', 'success');
        } else {
            this.showToast('设置失败: ' + (result.error || 'Unknown'), 'error');
        }
    }

    _updatePositionPreview() {
        const position = parseInt(this.positionInput.value);
        if (isNaN(position) || position < 0) {
            this.previewRowCol.textContent = 'Ln 1, Col 1';
            this.previewBefore.textContent = '';
            this.previewAfter.textContent = '';
            return;
        }

        const text = this._getScannedTabContent();

        const { row, col } = charOffsetToRowCol(position, text);
        const { before, after } = getSurroundingChars(position, text, 5);

        this.previewRowCol.textContent = `Ln ${row}, Col ${col}`;
        this.previewBefore.textContent = before;
        this.previewAfter.textContent = after;
    }

    showToast(message, type = 'info') {
        window.showToast(message, type, 5000);
    }

    // ── 扫描目标tab定位工具 ──

    /**
     * 获取扫描目标tab的ID
     * @returns {string|null}
     */
    _getScannedTabId() {
        if (!state.scanState || !state.scanState.systemId || !state.scanState.streamName) return null;
        return `${state.scanState.systemId}:${state.scanState.streamName}`;
    }

    /**
     * 获取扫描目标tab的编辑器文本内容
     * @returns {string}
     */
    _getScannedTabContent() {
        const tabId = this._getScannedTabId();
        if (!tabId) return '';
        const editorState = state.getEditorState(tabId);
        return editorState?.content || '';
    }

    /**
     * 激活tab切换事件处理 — 恢复或清除扫描位置标记
     */
    _onActiveTabChanged() {
        if (!state.scanState) return;
        const status = (state.scanState.status || '').toLowerCase();
        if (status !== 'scanning' && status !== 'paused') return;

        const scannedTabId = this._getScannedTabId();
        const activeTab = state.getActiveTab();

        if (activeTab && activeTab.id === scannedTabId) {
            // 切换回扫描目标tab → 恢复标记和位置显示
            // 注意：markdownEditor.setContent() 在 activeTabChanged 事件之后才调用，
            // 所以需要用 setTimeout(0) 延后到 setContent 完成之后执行
            setTimeout(() => {
                if (typeof markdownEditor !== 'undefined' && state.scanState) {
                    markdownEditor.showScanPosition(state.scanState.currentPosition || 0);
                }
            }, 0);
            this._refreshPositionDisplay();
        } else {
            // 切换离开扫描目标tab → 清除标记
            if (typeof markdownEditor !== 'undefined') {
                markdownEditor.removeScanPosition();
            }
        }
    }

    /** 强制刷新位置显示（行/列、前后字符、stream名） */
    _refreshPositionDisplay() {
        if (!state.scanState) return;
        const status = (state.scanState.status || '').toLowerCase();
        if (status !== 'scanning' && status !== 'paused') return;

        const pos = state.scanState.currentPosition || 0;
        const text = this._getScannedTabContent();
        const { row, col } = charOffsetToRowCol(pos, text);
        this.posRowCol.textContent = `Ln ${row}, Col ${col}`;

        const { before, after } = getSurroundingChars(pos, text, 5);
        this.posBefore.textContent = before;
        this.posAfter.textContent = after;

        this._updateScanStreamDisplay(state.scanState.streamName);
    }

    /** 显示/隐藏扫描目标流名 */
    _updateScanStreamDisplay(streamName) {
        const el = document.getElementById('scan-target-stream');
        if (!el) return;
        if (streamName) {
            el.textContent = streamName;
            el.style.display = 'inline';
        } else {
            el.style.display = 'none';
        }
    }
}

const scanControl = new ScanControl();