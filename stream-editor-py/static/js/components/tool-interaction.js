/**
 * Tool Interaction Bar — 精简交互输入栏
 *
 * 功能：
 * - 只显示可交互后台进程（keep_stdin===true），组合标签 tool_name + process_name
 * - 点击选中目标
 * - textarea 输入，Enter 发送，Shift/Ctrl+Enter 换行
 * - 支持 @toolName|{params} 工具调用
 * - 输出日志（工具执行结果、进程输出等）
 * - WS 事件驱动刷新，断连时降级 1s 轮询
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
            // Enter 发送（不含 Shift/Ctrl）
            if (e.key === 'Enter' && !e.shiftKey && !e.ctrlKey) {
                e.preventDefault();
                this._send();
            }
            // Shift+Enter 或 Ctrl+Enter → 默认行为（插入换行）
        });
        this.sendBtn.addEventListener('click', () => this._send());

        // 监听 WebSocket 推送的进程事件（替代轮询）
        state.on('processStarted', (change) => this._onProcessStarted(change));
        state.on('processExited', (change) => this._onProcessExited(change));

        // 新扫描开始时清理输出日志
        state.on('scanWSStateChange', (evt) => {
            if (evt.status === 'scanning' && evt.oldStatus !== 'scanning') {
                this.clearOutput();
            }
        });
    }

    // ── 发送 ──

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

    // ── 输出日志 ──

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

    // ── 进程列表刷新 ──

    async _refresh() {
        try {
            const result = await api.listProcesses(null, 'alive');
            if (result.success) {
                // 只保留可交互输入的进程（keep_stdin === true）
                this.processCache = (result.data || []).filter(p => p.keep_stdin === true);
            }
        } catch (e) { /* silent */ }
        this._renderTargets();
    }

    _onProcessStarted(change) {
        // 后台进程启动 → 刷新列表
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
        this._refresh();  // 初始加载

        // WS 重连后立即刷新
        state.on('wsReconnected', () => this._refresh());

        // 降级兜底：WS断连且正在扫描时 1s 轮询
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

    // ── 渲染进程条目 ──

    _renderTargets() {
        if (this.processCache.length === 0) {
            this.targetList.innerHTML = '<span class="target-placeholder">无可交互的后台进程</span>';
            return;
        }

        this.targetList.innerHTML = this.processCache.map(p => {
            const isSel = this.selectedProcess?.process_name === p.process_name;
            const label = `${p.tool_name} + ${p.process_name}`;
            return `<div class="target-item ${isSel ? 'selected' : ''}"
                         data-process="${this._esc(p.process_name)}">
                <span class="target-label">${this._esc(label)}</span>
                <button class="btn-stop-process" data-process="${this._esc(p.process_name)}" title="停止进程">✕</button>
            </div>`;
        }).join('');

        this.targetList.querySelectorAll('.target-item').forEach(el => {
            el.addEventListener('click', (e) => {
                // 不拦截 stop 按钮的点击
                if (e.target.closest('.btn-stop-process')) return;
                this._select(el.dataset.process);
            });
        });

        // 停止按钮事件
        this.targetList.querySelectorAll('.btn-stop-process').forEach(btn => {
            btn.addEventListener('click', async (e) => {
                e.stopPropagation();
                const pn = btn.dataset.process;
                const proc = this.processCache.find(p => p.process_name === pn);
                const label = proc ? `${proc.tool_name} (${pn})` : pn;
                if (!confirm(`确定要停止进程 "${label}" 吗？`)) return;
                try {
                    const result = await api.stopProcess(pn, 2);
                    if (result.success) {
                        this.addOutput(`已停止进程 ${label}`, 'system');
                        this._refresh();
                    } else {
                        this.addOutput(`停止进程失败: ${result.error || 'unknown'}`, 'error');
                    }
                } catch (e) {
                    this.addOutput(`停止进程出错: ${e.message}`, 'error');
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
        this.input.placeholder = `发送到 ${proc.tool_name} + ${proc.process_name}，Enter 发送，Shift/Ctrl+Enter 换行`;
        this.selectedLabel.textContent = `\u25B6 ${proc.tool_name} \u2014 ${proc.process_name}`;
        this.input.focus();
        this._renderTargets();
    }

    _deselect() {
        this.selectedProcess = null;
        this.input.disabled = true;
        this.sendBtn.disabled = true;
        this.input.value = '';
        this.input.placeholder = '选择目标后输入，Enter 发送，Shift/Ctrl+Enter 换行';
        this.selectedLabel.textContent = '选择进程后输入...';
        this._renderTargets();
    }

    _esc(s) {
        const d = document.createElement('div');
        d.textContent = s;
        return d.innerHTML;
    }
}

const toolInteraction = new ToolInteractionBar();
