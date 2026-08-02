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

        // Listen for scan events pushed via WebSocket (replaces polling)
        state.on('scanWSStateChange', (data) => this._onScanStateChanged(data));
        state.on('scanPositionUpdated', (data) => this._onScanPositionUpdated(data));
        state.on('scanCompleted', (change) => this._handleScanCompleted(change));
        state.on('scanError', (change) => this._handleScanError(change));

        // Restore backend scan state — on page load / after WebSocket reconnect
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

            // Use the scanned tab's content for row/col calculation (not the active tab)
            const text = this._getScannedTabContent();
            const { row, col } = charOffsetToRowCol(pos, text);
            this.posRowCol.textContent = `Ln ${row}, Col ${col}`;

            const { before, after } = getSurroundingChars(pos, text, 5);
            this.posBefore.textContent = before;
            this.posAfter.textContent = after;

            // Only show the marker in the editor when the scanned tab is the active tab
            const scannedTabId = this._getScannedTabId();
            const activeTab = state.getActiveTab();
            if (typeof markdownEditor !== 'undefined') {
                if (activeTab && activeTab.id === scannedTabId) {
                    markdownEditor.showScanPosition(pos);
                } else {
                    markdownEditor.removeScanPosition();
                }
            }

            // Push to the tab label display (no longer throttled — WS push is already throttled to 300ms)
            if (scannedTabId) {
                state.emit('scanPositionForTab', { row, col, tabId: scannedTabId });
            }

            // Display the scanned target stream name
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

        // Update final stats (if the scan_complete event carried data)
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
        // Call updateUI directly, don't emit 'scanStateChanged' (would cause infinite recursion)
        this.updateUI(state.scanState);
        this.updateButtons();
    }

    _onScanPositionUpdated(data) {
        if (!state.scanState) return;
        state.scanState.currentPosition = data.position || 0;
        // totalSize no longer used — totalLength removed per user request
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
            // Rely on push if the WebSocket connection is available; otherwise fall back to polling
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
            this.showToast('Scan position set', 'success');
        } else {
            this.showToast('Failed to set position: ' + (result.error || 'Unknown'), 'error');
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

    // ── Scanned tab targeting helpers ──

    /**
     * Get the ID of the scanned target tab
     * @returns {string|null}
     */
    _getScannedTabId() {
        if (!state.scanState || !state.scanState.systemId || !state.scanState.streamName) return null;
        return `${state.scanState.systemId}:${state.scanState.streamName}`;
    }

    /**
     * Get the editor text content of the scanned target tab
     * @returns {string}
     */
    _getScannedTabContent() {
        const tabId = this._getScannedTabId();
        if (!tabId) return '';
        const editorState = state.getEditorState(tabId);
        return editorState?.content || '';
    }

    /**
     * Active tab change handler — restore or clear the scan position marker
     */
    _onActiveTabChanged() {
        if (!state.scanState) return;
        const status = (state.scanState.status || '').toLowerCase();
        if (status !== 'scanning' && status !== 'paused') return;

        const scannedTabId = this._getScannedTabId();
        const activeTab = state.getActiveTab();

        if (activeTab && activeTab.id === scannedTabId) {
            // Switched back to the scanned tab → restore the marker and position display
            // Note: markdownEditor.setContent() is called after the activeTabChanged event,
            // so use setTimeout(0) to run after setContent has completed
            setTimeout(() => {
                if (typeof markdownEditor !== 'undefined' && state.scanState) {
                    markdownEditor.showScanPosition(state.scanState.currentPosition || 0);
                }
            }, 0);
            this._refreshPositionDisplay();
        } else {
            // Switched away from the scanned tab → clear the marker
            if (typeof markdownEditor !== 'undefined') {
                markdownEditor.removeScanPosition();
            }
        }
    }

    /** Force refresh the position display (row/col, surrounding chars, stream name) */
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

    /** Show/hide the scanned target stream name */
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