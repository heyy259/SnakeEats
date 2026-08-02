/**
 * Tool Interaction Bar — streamlined interactive input bar
 *
 * Features:
 * - Only shows interactive background processes (keep_stdin===true), combining labels tool_name + process_name
 * - Click to select a target
 * - textarea input, Enter to send, Shift/Ctrl+Enter for newline
 * - Supports @toolName|{params} tool calls
 * - Output log (tool execution results, process output, etc.)
 * - WS event-driven refresh, falling back to 1s polling when disconnected
 */
class ToolInteractionBar {
    constructor() {
        this.targetList = document.getElementById('target-list');
        this.input = document.getElementById('interaction-input');
        this.sendBtn = document.getElementById('send-interaction-btn');
        this.selectedLabel = document.getElementById('selected-target-label');
        this.outputLog = document.getElementById('output-log');

        this.selectedProcess = null;
        this.processCache = [];
        this.refreshTimer = null;
        this.onSend = null;

        this._setupListeners();
        this._startRefresh();
    }

    _setupListeners() {
        this.input.addEventListener('keydown', (e) => {
            // Enter sends (without Shift/Ctrl)
            if (e.key === 'Enter' && !e.shiftKey && !e.ctrlKey) {
                e.preventDefault();
                this._send();
            }
            // Shift+Enter or Ctrl+Enter → default behavior (insert newline)
        });
        this.sendBtn.addEventListener('click', () => this._send());

        // Listen for process events pushed via WebSocket (replaces polling)
        state.on('processStarted', (change) => this._onProcessStarted(change));
        state.on('processExited', (change) => this._onProcessExited(change));

        // Clear the output log when a new scan starts
        state.on('scanWSStateChange', (evt) => {
            if (evt.status === 'scanning' && evt.oldStatus !== 'scanning') {
                this.clearOutput();
            }
        });
    }

    // ── Send ──

    async _send() {
        const message = this.input.value.trim();
        if (!message) return;
        this.input.value = '';

        if (this.onSend) {
            this.onSend(message);
            return;
        }

        if (!this.selectedProcess) return;
        try {
            await api.sendProcessInput(this.selectedProcess.process_name, message, true, 'utf-8');
        } catch (e) {
            this._refresh();
        }
    }

    // ── Output log ──

    addOutput(message, type) {
        if (!this.outputLog) return;
        this.outputLog.style.display = 'block';
        const line = document.createElement('div');
        line.className = 'output-line output-' + (type || 'system');
        line.textContent = message;
        this.outputLog.appendChild(line);
        this.outputLog.scrollTop = this.outputLog.scrollHeight;
    }

    clearOutput() {
        if (this.outputLog) {
            this.outputLog.innerHTML = '';
            this.outputLog.style.display = 'none';
        }
    }

    // ── Process list refresh ──

    async _refresh() {
        try {
            const result = await api.listProcesses(null, 'alive');
            if (result.success) {
                // Keep only processes that accept interactive input (keep_stdin === true)
                this.processCache = (result.data || []).filter(p => p.keep_stdin === true);
            }
        } catch (e) { /* silent */ }
        this._renderTargets();
    }

    _onProcessStarted(change) {
        // Background process started → refresh the list
        this._refresh();
    }

    _onProcessExited(change) {
        this._refresh();
        if (this.selectedProcess?.process_name === change.process_name) {
            const label = change.exit_code != null
                ? `Process ${change.tool_name} (${change.process_name}) exited with code ${change.exit_code}`
                : `Process ${change.tool_name} (${change.process_name}) was terminated`;
            this.addOutput(label, 'system');
        }
    }

    _startRefresh() {
        this._refresh();  // initial load

        // Refresh immediately after WS reconnect
        state.on('wsReconnected', () => this._refresh());

        // Fallback: 1s polling when WS is disconnected and scanning is in progress
        this.refreshTimer = setInterval(() => {
            const wsConnected = typeof tabManager !== 'undefined'
                && tabManager.subscriptionManager?.isConnected();
            if (!wsConnected && this._isScanning()) {
                this._refresh();
            }
        }, 1000);
    }

    _isScanning() {
        return state.scanState
            && state.scanState.status
            && state.scanState.status.toLowerCase() === 'scanning';
    }

    // ── Render process entries ──

    _renderTargets() {
        if (this.processCache.length === 0) {
            this.targetList.innerHTML = '<span class="target-placeholder">No interactive background processes</span>';
            return;
        }

        this.targetList.innerHTML = this.processCache.map(p => {
            const isSel = this.selectedProcess?.process_name === p.process_name;
            const label = `${p.tool_name} + ${p.process_name}`;
            return `<div class="target-item ${isSel ? 'selected' : ''}"
                         data-process="${this._esc(p.process_name)}">
                <span class="target-label">${this._esc(label)}</span>
                <button class="btn-stop-process" data-process="${this._esc(p.process_name)}" title="Stop process">✕</button>
            </div>`;
        }).join('');

        this.targetList.querySelectorAll('.target-item').forEach(el => {
            el.addEventListener('click', (e) => {
                // Don't intercept clicks on the stop button
                if (e.target.closest('.btn-stop-process')) return;
                this._select(el.dataset.process);
            });
        });

        // Stop button events
        this.targetList.querySelectorAll('.btn-stop-process').forEach(btn => {
            btn.addEventListener('click', async (e) => {
                e.stopPropagation();
                const pn = btn.dataset.process;
                const proc = this.processCache.find(p => p.process_name === pn);
                const label = proc ? `${proc.tool_name} (${pn})` : pn;
                if (!confirm(`Are you sure you want to stop process "${label}"?`)) return;
                try {
                    const result = await api.stopProcess(pn, 2);
                    if (result.success) {
                        this.addOutput(`Process ${label} stopped`, 'system');
                        this._refresh();
                    } else {
                        this.addOutput(`Failed to stop process: ${result.error || 'unknown'}`, 'error');
                    }
                } catch (e) {
                    this.addOutput(`Error stopping process: ${e.message}`, 'error');
                }
            });
        });

        if (this.selectedProcess
            && !this.processCache.find(p => p.process_name === this.selectedProcess.process_name)) {
            this._deselect();
        }
    }

    _select(processName) {
        const proc = this.processCache.find(p => p.process_name === processName);
        if (!proc) return;

        if (this.selectedProcess?.process_name === processName) {
            this._deselect();
            return;
        }

        this.selectedProcess = proc;
        this.input.disabled = false;
        this.sendBtn.disabled = false;
        this.input.placeholder = `Send to ${proc.tool_name} + ${proc.process_name}, Enter to send, Shift/Ctrl+Enter for newline`;
        this.selectedLabel.textContent = `\u25B6 ${proc.tool_name} \u2014 ${proc.process_name}`;
        this.input.focus();
        this._renderTargets();
    }

    _deselect() {
        this.selectedProcess = null;
        this.input.disabled = true;
        this.sendBtn.disabled = true;
        this.input.value = '';
        this.input.placeholder = 'Select a target to start typing, Enter to send, Shift/Ctrl+Enter for newline';
        this.selectedLabel.textContent = 'Select a process to start typing...';
        this._renderTargets();
    }

    _esc(s) {
        const d = document.createElement('div');
        d.textContent = s;
        return d.innerHTML;
    }
}

const toolInteraction = new ToolInteractionBar();
